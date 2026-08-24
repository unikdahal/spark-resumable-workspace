# Design note: AQE stage-digest granularity, and the Celeborn corrupted/rerun-stage question

Companion to `LLD-resumable-spark-driver.md`. Two problems raised against the current PoC:

1. `QueryResumeCoordinator` gates on one whole-plan `PlanDigest`. If AQE replans even one node
   mid-query, the digest never matches again, even though other stages didn't change.
2. `confirmAlive`'s TCP liveness probe can't tell a *reachable* shuffle location from a *correct*
   one — could an anchor point at data Celeborn's own `stageRerun`/revive mechanism has since
   superseded or the worker has since corrupted?

Both have a real fix and a fix that isn't buildable solo. This note says which is which, and
what got empirically checked rather than assumed.

## 1. Whole-plan digest vs. AQE mid-query replanning

### Root cause

`SafePlanKey.key(plan)` hashes the whole physical tree in one pass. AQE re-plans stage by stage
as real stats arrive; a later stage choosing a different join strategy changes the whole-plan hash
even though every earlier stage is byte-identical to the captured run. One digest, one bit of
information, no way to say "these three stages are fine, this one changed."

### Best approach: key anchors by stage, not by plan

AQE's `QueryStageExec` (`ShuffleQueryStageExec` / `BroadcastQueryStageExec`) **freezes once
materialized** — `resultOption` is set once and never revisited, regardless of what AQE decides
for later stages. That's the granularity the digest needs to move to.

**Capture** (extends §3.1's `PlanDigest`, doesn't replace it): at each stage's materialization
point, compute a `StageDigest` that chains to its inputs —

```
StageDigest(stage) = SHA256(
  canonicalized-node-hash(stage's own frozen plan subtree)
  ++ StageDigest(input stage 1) ++ StageDigest(input stage 2) ++ ...
)
```

— a Merkle chain, not a flat hash. Write the shuffle/broadcast anchor under this key instead of
under the whole-plan digest.

**Adopt**: walk the *current* AQE plan bottom-up. For each stage, compute its `StageDigest` the
same way and probe the store. A hit means: this stage, and everything it transitively depends on,
is structurally identical to some previously-materialized run — adopt it, and feed its real stats
(reusing this session's per-partition `MapStatus` byte-size fix, not a placeholder) into AQE's own
cost model so *downstream* replanning decisions are correctly informed by adopted data. A miss
means recompute that stage — and by construction every stage downstream of a miss also misses,
since its own `StageDigest` chains through the missed one. That's invariant V-2 ("rejecting an
anchor transitively rejects its descendants") falling out of the chaining for free, not a separate
rule to enforce.

This is exactly why a later stage taking a different path (different observed skew, different
join strategy) no longer poisons an earlier stage's anchor — the earlier stage's `StageDigest`
never included anything about the later one.

### Status: built and proven, not just designed

Both halves landed:

- **Spark fork** (`resume-adoption-spark-core`, commit `8a666cc`): `ResumeHooks.StageResumeHook`
  (`tryAdoptShuffleStage` / `onShuffleStageMaterialized`) plus a patch to
  `AdaptiveSparkPlanExec`'s own stage-materialization loop — an adopted `ShuffleQueryStageExec`
  skips `stage.materialize()` outright and synthesizes the same `StageSuccess` event a real
  materialize would eventually produce, so nothing downstream (replanning, cost evaluation, the
  `allChildStagesMaterialized` convergence check) can tell the difference.
- **resume-poc-sql** (commit `a90e644`): `StageDigest.scala` — the Merkle-chained hasher described
  above, reusing `SafePlanKey`'s node/scan hashing but treating a nested `QueryStageExec` as an
  opaque leaf (its own already-known digest) instead of walking through it.

The central claim — an earlier stage's digest survives a later stage taking a different path — is
checked empirically, not asserted: `StageDigestDemo` + `run-stage-digest-test.sh` run the identical
fact/dim aggregate-then-join query as two separate JVM processes, differing only in
`autoBroadcastJoinThreshold` (forces `SortMergeJoin` in one, `BroadcastHashJoin` in the other).
3/3 checks pass — both land on the identical result, the whole-plan digest **differs** (the
failure mode this fixes), and the fact-side aggregate's own stage digest is **identical** across
both runs despite that.

One thing found building this, worth keeping: the first version of this test ran both sessions
sequentially in one JVM and got a false negative — `NamedExpression`'s `exprId` counter is a
single JVM-wide `AtomicLong`, so two sessions in one process drift exprIds against each other in a
way two freshly-launched JVMs analyzing the identical query deterministically do not (each starts
the counter fresh and assigns it in the same rule-application order). Every other capture/adopt
demo in this project already avoided this by construction — separate `java` invocations — this was
the first one to hit it. Now documented in `StageDigestDemo`'s doc comment.

### What's not wired yet

No live end-to-end *adoption* demo — one where a second JVM actually skips computing a shuffle
stage and the job runs measurably fewer tasks for it. That needs an AQE+SQL+Celeborn integration
harness neither existing project currently has: `resume-poc`'s pipeline is raw RDD (Celeborn-backed,
no AQE — AQE only applies to `AdaptiveSparkPlanExec`, which wraps SQL/DataFrame execution, not the
RDD API), and `resume-poc-sql`'s pipeline is SQL/AQE but plain Spark shuffle (no Celeborn, so
nothing survives a real cross-process restart for shuffle bytes specifically). The hook plumbing
and the proven digest scheme are the two pieces that harness would assemble; building it is the
next increment, not a redesign.

## 2. Celeborn: could an adopted anchor point at corrupted or superseded data?

### What was checked, and why it's a narrower question than first framed

Went looking for a cross-restart integrity ledger in Celeborn: `PartitionLocation.epoch`,
`LifecycleManager.latestPartitionLocation`, the epoch-comparison at `handleRevive`. Dead end, for
a structural reason, not a missing feature: `latestPartitionLocation` is an in-memory map **owned
by one `LifecycleManager` instance**. Driver A's `LifecycleManager` dies with driver A. There is no
second, independent authority Celeborn maintains that driver B could ask "has this partition been
revived since driver A last knew about it" — the `LifecycleManager` *is* the authority, by design,
and driver B's freshly-constructed one only knows what the anchor told it. An epoch comparison
needs two sides; only one exists after a crash.

Building the other side — a worker RPC exposing "current epoch for partition X" — is a Celeborn
*protocol* change (worker + client + protobuf), not a client-side patch. That would be a second
unreviewed extension to a codebase this project is already extending once, and its correctness
would depend on protocol work authored by the same project trying to close the gap with it. Not
building that solo.

Two things narrow the real residual risk once the "no live authority" fact is taken seriously:

**`stageRerun`/revive needs a live reducer to trigger it.** It fires on a fetch failure from an
actively-running task. While driver A is dead, nothing is fetching, so nothing revives, in that
window. The case that actually matters is narrower than "Celeborn might rerun this after I
capture it": it's a revive that was **already in flight or just-completed at the instant driver A
crashed**, such that driver A's last-known state (what got captured into the anchor) was already
stale by milliseconds. A real but small window, not an open-ended one.

**Corruption that flips bytes without truncating still gets caught — checked, not assumed.**
Celeborn's default compression codec (LZ4) wraps each block with an XXHash32 checksum;
`Lz4Decompressor` throws `"Checksum not equal!"` on mismatch
(`celeborn/client/src/main/java/org/apache/celeborn/client/compress/Lz4Decompressor.java:86`).
That exception surfaces as a fetch failure to Spark, which is the same recovery path verified
below. So the silent-wrong-answer surface is narrower still: it needs compression disabled *and*
a corruption that Spark's own fetch-length checks also miss — not "any corruption on any adopted
shuffle."

### What actually closes most of this — verified, not asserted

Spark's `DAGScheduler` fetch-failure handling (`unregisterMapOutput` → stage resubmit) does not
care *why* a fetch failed, or *how* the map output got registered — `seedAdopted` and a real
`ShuffleMapTask` completion look identical to it. Built a test for exactly the case that matters:
adopt for real against a genuinely live worker (`confirmAlive` truly passes), kill that worker in
the window *after* adoption and *before* any reduce task reads a byte, then let the job run.

`resume-poc/run-kill-before-fetch-test.sh` — 2/2 checks pass:

```
RESUME-POC RUN-KILL outcome=Adopted ADOPT-CONFIRMED-ALIVE
RESUME-POC RUN-KILL worker-killed signal received -- fetching now (expect a fetch failure, map-stage resubmit, then a correct result)
RESUME-POC RUN-KILL tasksRun=9 result=OK-RECOMPUTED-AFTER-FETCH-FAILURE
PASS: adoption confirmed alive before the worker died
PASS: job completed with the CORRECT result after the worker died mid-flight
```

(First attempt correctly failed with `RESERVE_SLOTS_FAILED` — a one-worker cluster has nowhere to
push a recomputed map output once its only worker is dead, which is Celeborn refusing unsafe work,
not a recovery-path bug. A second worker, kept alive throughout, was added to the topology to
isolate the actual question.)

This converts most of the corruption/rerun class from "wrong answer" to "lost optimization, safe
recompute" — invariant A-1 holding by the same mechanism that already protects a plain restart
with no resume-poc involved at all. It is Spark's own recovery, unmodified, doing the work.

### What's still genuinely open

- Pure durability: a worker acknowledges a commit, then crash-restarts and loses bytes it never
  actually fsynced. Whether Celeborn's commit protocol can produce that gap is a question about
  Celeborn's own internals this project hasn't audited and shouldn't claim to have closed.
- The narrow in-flight-revive-at-crash-instant window above — real, small, not yet measured.

### Status: built and proven, not just designed (rung 7.5)

The "don't build a second Celeborn patch" recommendation below this section is what this note
originally concluded. It turned out to be avoidable: Celeborn already has an RPC that answers
"what does this file currently look like" without needing a new epoch-check protocol message —
`OPEN_STREAM` (and its batched sibling `BATCH_OPEN_STREAM`), the same RPC a real reduce-task
fetch opens first. `CelebornAdoption.confirmFresh` (rung 7.5, called from
`ResumeCoordinator.tryAdopt` between `confirmAlive`'s rung 7 and `adopt`) uses it to compare every
captured `PartitionLocation`'s byte length against its live length, right now, on the worker that
holds it — reused, not invented: no new Celeborn wire message, no server-side patch, no protocol
change.

**One real bug found and fixed building this, worth keeping.** The obvious implementation —
request `OPEN_STREAM` with `startIndex=0, endIndex=Int.MaxValue` and read the last entry of the
reply's `chunkOffsets` — is wrong for the code path a normal remote fetch actually takes.
`FetchHandler.handleReduceOpenStreamInternal`'s default branch calls
`makeStreamHandler(streamId, meta.getNumChunks)` with **no `offsets` argument at all** — the wire
reply genuinely has an empty `chunkOffsets` list regardless of the file's real length, even though
`numChunks` is populated. Only the `readLocalShuffle=true` branch passes the real
`ReduceFileMeta.getChunkOffsets()`. Setting that flag is itself a disclosed deviation from its
documented meaning ("I will read this file off local disk myself," normally only set by a
co-located client) — see the "what's still not closed" outreach question below. Once set, the
returned list is finalized to end with the file's true total byte length by
`PartitionMetaHandler.afterClose()`, guaranteed true for every captured anchor because capture
only runs after the shuffle's map stage has committed. `CelebornAdoption.currentFileLength`'s doc
comment has the full trace. Found by empirical failure, not code review: the first version of
`run-demo.sh`'s RUN 2 (adopt against an untouched, live worker) failed with a false
`CELEBORN_STALE` rejection — `numChunks=1, chunkOffsets=[]` for a genuinely valid, unmodified
file — which is what led to reading `FetchHandler`'s source in the first place.

**Verified, not assumed: a genuine Celeborn-side revive destroys the superseded location's file,
so rung 7.5 does catch supersession, not just truncation-in-place.** The concern this section
originally raised — "an old epoch's file might still be sitting there, untouched, reporting its
original captured length, even though a newer epoch is now the real answer" — turns out not to
hold, checked by reading the revive path itself: `LifecycleManager.releasePartitionLocation` (the
path a completed revive/reassign takes for the locations it supersedes) calls
`destroySlotsWithRetry`, which RPCs the worker; `Controller.handleDestroy` on the worker side
calls `storageManager.cleanFile`, which deletes both the on-disk file **and** the worker's
in-memory `FileInfo` entry for that exact filename (filenames are `partitionId-epoch-mode`, so an
old epoch and a new one are different filenames, different map entries). A captured anchor
pointing at a destroyed epoch's filename gets a genuine "file not found" from `getRawFileInfo` on
the next `OPEN_STREAM`, surfacing as `None` from `currentFileLength` — treated as NOT fresh, same
as a dead worker. This is a source-reading finding, not an orchestrated live-revive integration
test — this project did not build a harness that forces a real Celeborn revive mid-flight and then
adopts against the result; that remains open (see below).

**What was built and empirically checked** (`resume-poc/run-demo.sh`, `resume-poc/Demo.scala`):
- Positive path: RUN 2 adopts against a genuinely live, untouched worker — rung 7.5 passes on
  real, unmodified data (this is the case the `chunkOffsets` bug above broke, now fixed).
- Negative path (`adopt-expect-stale` / RUN-STALE): the anchor's captured length for one location
  is deliberately falsified before `tryAdopt` sees it, worker otherwise genuinely alive (rung 7
  passes) — `confirmFresh` correctly rejects with `CELEBORN_STALE`, and the test asserts the
  *specific* rejection reason, not just that some rejection happened, so a bug that made rung 7
  fire first would still be caught. This substitutes a falsified captured value for an
  orchestrated live revive/corruption — a disclosed substitution, not a hidden one: it exercises
  the identical comparison a genuine revive or truncation would hit, without this project taking
  on reproducing Celeborn's internal revive machinery deterministically inside a demo harness.
- Rung 7 (dead worker, RUN 3) still passes unmodified alongside rung 7.5.
- Response-shape hardening: empty `chunkOffsets` and a `chunkOffsets.size != numChunks + 1`
  mismatch are both treated as `None` (not fresh), never as "0 bytes" or "trust it anyway" — an
  inconclusive probe must never read as confirmed-fresh (invariant A-1).
- Scale: `confirmFresh` batches every location across every partition into one
  `BATCH_OPEN_STREAM` RPC per distinct worker (`CelebornAdoption.currentFileLengths`), not one
  `OPEN_STREAM` per location — adopt-time freshness-check cost now scales with worker count, not
  shuffle width.

### What's still not closed

- **The narrow in-flight-revive-at-crash-instant window, still real, still not measured.** If a
  revive's `destroySlotsWithRetry` RPC hasn't reached the worker yet at the instant rung 7.5
  probes it — revive decided but not yet executed — the old file is still intact and still
  reports its original length, so it passes as fresh even though a newer epoch is now
  authoritative. This is the same window the "what actually closes most of this" section above
  already named for the fetch-failure-recovery argument; rung 7.5 does not close it, and nothing
  in this project measures how wide it actually is in practice.
- **Sorted-file interaction, not exercised by any test here.** `confirmFresh` always requests the
  whole file (`endIndex=Int.MaxValue`). `FetchHandler`'s own comment says that only gets rerouted
  through `partitionsSorter.getSortedFileInfo` if the *unsorted* file was already deleted by an
  earlier *range* read (a skew-partition partial read) — sorting is layout-only, not
  byte-count-changing, so the total length rung 7.5 compares against should still match even then.
  "Should" is doing real work in that sentence: this project's demo never issues a range read, so
  this path has never actually been run, only reasoned about from source.
- **`readLocalShuffle=true` from a non-colocated client is a real deviation, not a patch, but
  still worth asking Celeborn about.** This replaces the old outreach question this section used
  to end on (an epoch-authority RPC) — that question turned out answerable from source, see above.
  The live one:
  > Is setting `readLocalShuffle=true` on `OPEN_STREAM`/`BATCH_OPEN_STREAM` from a client that is
  > not actually co-located with the worker a supported way to read a partition's current
  > committed length and chunk layout, or does it risk depending on behavior that's only
  > incidentally correct today?
- Pure durability (a worker acks a commit, then loses unfsynced bytes on its own crash-restart) —
  unaudited, as before.

Everything else in this note — the fetch-failure test, the compression checksum finding, the
`chunkOffsets`/batching/response-shape fixes above — stands on its own regardless of the answer.
