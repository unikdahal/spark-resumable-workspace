# Resumable Spark driver: current approach, what's covered, what isn't

Status snapshot as of 2026-08-16. This document sits above `LLD-resumable-spark-driver.md` (the
full target design) and `design-aqe-and-corrupted-rerun.md` (two specific fixes, in depth) to
answer one question plainly: **of the full design, what has actually been built and proven, what
exists only on paper, and what is a known, disclosed gap.** Every claim below is backed by a
commit, a test, or an explicit "not tested, reasoned from source" label — nothing here is aspirational.

## The problem, in one paragraph

A Spark driver holds two kinds of state: shuffle **bytes**, which for a Celeborn-backed shuffle
live independently on remote workers and survive a driver crash, and the **metadata that names
and trusts those bytes** (`MapOutputTracker` registration, Celeborn's own `LifecycleManager`
catalog), which is driver-local and dies with the driver. A restarted driver re-plans the query
from scratch and, by default, recomputes everything, even shuffle stages whose output is still
sitting on disk. The goal is to let a restarted driver re-adopt that surviving data instead of
recomputing it — *provided it can prove the data is still correct*, because adopting wrong data
silently is worse than not adopting at all. That's invariant **A-1**: *losing state is safe; wrong
state is catastrophic.* Every design decision below is in service of that one asymmetry — when in
doubt, the code recomputes, never guesses.

## The shape of the actual work: four repositories, two proof-of-concept tracks

The LLD describes one system. What actually exists is two separate, never-merged proof-of-concept
tracks, plus two upstream forks each carrying one narrow, real patch:

| Repo (branch) | Role | Proves |
|---|---|---|
| `resume-poc` (`main`, own GitHub repo) | RDD-level pipeline, real Celeborn cluster | The adoption *mechanism* — scheduler seeding, Celeborn catalog seeding, the acceptance-ladder rungs that don't need a `SparkPlan` |
| `resume-poc-sql` (`main`, own GitHub repo) | SQL/DataFrame pipeline, AQE, no Celeborn | The SQL-layer anchoring mechanisms (DPP, broadcast, write, result, query constants) and digest-based gating — no cross-process shuffle-byte survival |
| `resume-poc-e2e` (`main`, own GitHub repo) | SQL/DataFrame pipeline, AQE, real Celeborn cluster | The combination of the two above — a real AQE shuffle stage genuinely skipped, against real Celeborn-backed bytes, across a real two-JVM restart |
| `spark` fork (`resume-adoption-spark-core` branch, `myfork`) | Patches to real Apache Spark 3.5 source | Hook points (`ResumeHooks`) that let an external coordinator intercept DPP/broadcast/write/result/query-constant execution and, new this session, per-AQE-stage shuffle materialization |
| `celeborn` fork (`resume-adoption-patch10` branch → `main` on `myfork`) | One patch to real Apache Celeborn | `LifecycleManager.adoptShuffle` / `confirmAlive` — the adoption API this whole design needs and that does not exist upstream |

**The first two PoC tracks were never combined — until `resume-poc-e2e`.** `resume-poc` proves
bytes survive and can be re-registered with Spark's scheduler; `resume-poc-sql` proves SQL-layer
state (subquery results, broadcast payloads, write commit messages, query-scoped constants, and
per-stage digests) can be captured and correctly gated, but only against plain Spark shuffle.
`resume-poc-e2e` is a third, small project that reuses both projects' actual code
(`CelebornAdoption` ported unchanged from `resume-poc`, `StageDigest`/`SafePlanKey` ported
unchanged from `resume-poc-sql`) behind `ResumeHooks.StageResumeHook`, and proves a real AQE SQL
query, backed by a real Celeborn cluster, gets a shuffle stage genuinely skipped by a second JVM —
see "What's covered beyond the ladder" below for what it found and fixed doing that.

## What's covered, by acceptance-ladder rung (LLD §3.6's eight rungs)

The LLD's acceptance ladder is the gate an anchor must pass before being adopted. Coverage today,
rung by rung:

| Rung | What it checks | Status |
|---|---|---|
| 1. `schemaVersion` match | Anchor's schema version equals the running coordinator's | **Built + tested** (`resume-poc`, RUN 2b: mismatched version → reject → recompute → correct result) |
| 2–6. Determinism class, ancestor/closure rejection, tripwires, `PlanDigest` match, per-`InputAnchor` re-verification | SQL-layer correctness gates | **Assembled and tested, non-AQE only** (`resume-poc-sql`'s `QueryResumeCoordinator`, commit `08b0082`): one digest atomically gates DPP + broadcast + result + query-constant anchors together. Explicitly does not cover AQE plans — see gaps below. |
| 7. Celeborn liveness | Is the worker holding this data even reachable? | **Built + tested**, and **known weaker than the LLD wants** (`resume-poc`'s `confirmAlive`): a real TCP-reachability probe against the real (killed) worker port. Proven to catch "worker process is dead." **Not proven** to catch the case the LLD actually emphasizes — an app whose files were reclaimed after Celeborn's own heartbeat-timeout expiry while the worker process stays up. A live TCP probe to a live port passes regardless of reclaim state. Disclosed in `resume-poc/README.md` point 3, unchanged this session. |
| 7.5. Data freshness (this session's addition, not in the original LLD numbering) | Does the captured file still match what's live on the worker, byte for byte, right now? | **Built + tested** (`resume-poc`'s `confirmFresh`, commit `08498f7`). Closes the gap `confirmAlive` structurally cannot: reachable-but-stale/superseded/corrupted data. See its own section below — this is the most-scrutinized piece of work in this session. |
| 8. Local shuffle reader off / no AQE skew specs on the adopted plan | Precondition (co-location assumption) / anti-corruption check for skew-split reads | **Built + tested, but as a post-hoc fact-check, not a pre-adoption gate — checked, not assumed, that a gate is even possible here.** `SkewGuard.assertLocalShuffleReaderOff` is wired into both `capture` and `tryAdopt` and proven to refuse (`resume-poc`, `RUN reader-guard`). `SkewGuard.hasSkewPartitionSpecs` (ported into `resume-poc-e2e`, which genuinely has a `SparkPlan` in scope) turned out NOT wireable as a pre-adoption rejection: the skew specs live on the CONSUMER of a stage, not the stage itself, and `OptimizeSkewedJoin` decides whether to split using stats that don't exist yet at adoption-decision time. Used instead as a post-hoc check on the run's final plan (`resume-poc-e2e`'s `skew-adopt` mode) — and a real bug came out of building it: the placeholder per-mapper stats an adoption feeds AQE silently defeated skew-splitting entirely (fixed in `MapOutputTrackerAccess.seedAdopted`), after which a genuine skew split against an adopted stage was forced to happen and empirically confirmed correct, on top of the structural full-mapper-range-coverage guarantee already verified from source. |

Net: rungs 1, 7, 7.5, and 8 are proven in the Celeborn-backed tracks (`resume-poc` for the
reader-off half, `resume-poc-e2e` for the skew half). Rungs 2–6 are proven assembled and gating
correctly, but only for non-AQE plans, and never against a Celeborn-backed shuffle. No single test
exercises all eight rungs together against one real query.

## What's covered beyond the ladder

- **The core scheduler fact (LLD §6.1).** `MapOutputTrackerMaster.registerShuffle` /
  `registerMapOutput`, called from a JVM that never ran the map stage, make
  `findMissingPartitions` empty and the `DAGScheduler` skip the map stage — verified by an actual
  task-count assertion (`resume-poc` RUN 2: 3 tasks instead of 7), not a log grep.
- **Celeborn adoption is real, not stubbed (LLD §6.2, patch 10).** `LifecycleManager.adoptShuffle`
  is a genuine patch to Celeborn (`fb9ccfa93`, 4 files, +177/-4, additive only), and the second
  JVM's reduce tasks read real bytes from the real worker the first JVM wrote — the LLD's §13.3
  "data path" test, not just "metadata path."
- **Per-partition `MapStatus` byte sizes are real, not a flat placeholder (LLD §2.3).**
  `CelebornAdoption.partitionByteSizes` reads real `StorageInfo.fileSize` off already-captured
  `PartitionLocation`s, so AQE's cost model sees real numbers for adopted shuffles. Verified via
  `RESUME_POC_DEBUG_SIZES=1`. What this does *not* prove: that a later AQE stage's replanning
  decision is bit-identical whether an earlier stage was adopted or recomputed — no harness
  exists that could answer that (see the combined-harness gap below).
- **AQE stage-digest granularity fix (design note §1), built and empirically proven.** The
  original design gated on one whole-plan digest, which breaks the moment AQE replans any single
  node mid-query. Fixed with a Merkle-chained per-stage digest: `StageDigest.scala`
  (`resume-poc-sql`, commit `a90e644`) plus a real patch to Spark's own
  `AdaptiveSparkPlanExec` stage-materialization loop (`spark` fork, commit `8a666cc8864`) that lets
  an adopted `ShuffleQueryStageExec` skip `stage.materialize()` outright and synthesize the same
  `StageSuccess` event a real materialize would produce. Proven with two separate JVM processes
  running the identical query with different `autoBroadcastJoinThreshold` settings (forcing
  `SortMergeJoin` in one, `BroadcastHashJoin` in the other): whole-plan digests differ (the failure
  mode being fixed), results are identical, and the fact-side aggregate's own stage digest is
  identical across both runs despite the later join strategy diverging. 3/3 checks pass
  (`run-stage-digest-test.sh`).
- **Fetch-failure recovery closes most of the "adopted-then-the-worker-dies" risk (design note
  §2), verified, not assumed.** Adopt for real against a genuinely live worker, kill that worker in
  the narrow window after adoption and before any reduce task reads a byte, then let the job run.
  Spark's own unmodified `DAGScheduler` fetch-failure handling (`unregisterMapOutput` → stage
  resubmit) doesn't care why a fetch failed or how the map output got registered — `seedAdopted`
  and a real `ShuffleMapTask` completion look identical to it. 2/2 checks pass
  (`run-kill-before-fetch-test.sh`).
- **Real Redis-backed anchor store and a real concurrent-writer fencing test (LLD §7.3/§7.4).**
  `RedisAnchorStore` plus `ZombieFenceDemo` runs two genuinely concurrent JVM processes against a
  real Redis instance and proves the losing writer's write is refused by the Lua CAS fence, not
  simulated sequentially.
- **A real bug found and fixed in this session's freshness-check work, kept as a documented
  lesson.** The obvious implementation of "read a Celeborn file's live length" — request
  `OPEN_STREAM`, read the last `chunkOffsets` entry — is wrong for the RPC path a normal fetch
  takes; `FetchHandler`'s default branch never populates `chunkOffsets` at all, so the naive
  version silently computed `live=0` and false-rejected genuinely fresh data. Fixed by setting
  `readLocalShuffle=true` (a disclosed, deliberate reuse of an existing field outside its normal
  meaning, not a protocol change) and by treating an empty or shape-inconsistent response as an
  inconclusive probe (`None`, not fresh) rather than guessing zero. Then verified from source, not
  assumed, that a genuine Celeborn revive actually deletes the superseded location's file
  (`LifecycleManager.releasePartitionLocation` → `destroySlotsWithRetry` → worker
  `Controller.handleDestroy` → `storageManager.cleanFile`), so rung 7.5 catches supersession by a
  newer epoch, not merely truncation of the same file in place.
- **`resume-poc-e2e`: the combined AQE+SQL+Celeborn adoption demo, closing the gap named above.**
  A real AQE SQL query (`fact.groupBy("dim_key").agg(sum("amount"))` joined to `dim`), backed by a
  real local Celeborn cluster, run as two separate JVMs. `capture` forces a shuffle join
  (`SortMergeJoin`); `adopt` forces a broadcast join (`BroadcastHashJoin`) against the *same*
  fact/dim data — the exact cross-strategy pressure the stage-digest fix (design note §1) exists
  for, now driving a real skip instead of just a matching hash in a unit test. 5/5 checks pass
  (`resume-poc-e2e/run-e2e-demo.sh`): correct result; all 4 reduce partitions genuinely populated
  in the captured anchors (not just plausibly-empty hash buckets); a real task-count drop measured
  against a COLD baseline of the *same* broadcast-join query (not against `capture`, which would
  confound the comparison with the join-strategy difference alone); the adopted stage's digest
  provably traces back to a `StageCaptured` event, not coincidence; and the join strategy is
  confirmed to genuinely differ between the two runs. Two of those checks were wrong in the first
  draft — a task-count comparison confounded by join strategy, and a partition-count assumption
  that was plausible but unverified — both caught by an advisor review before being shipped as
  passing proof of something they didn't actually establish; see `resume-poc-e2e/README.md`.
  Two real, previously-undiscovered bugs came out of building this:
  - **Celeborn's own map-side stage-end commit is asynchronous relative to a map stage's own
    `MapOutputStatistics` future resolving.** `resume-poc`'s post-`.collect()` capture never hits
    this — the reduce stage's wall-clock time hides the race. A hook capturing at
    `AdaptiveSparkPlanExec`'s own per-stage materialization callback has no such luxury and hit it
    directly: `exportFileGroups` returning empty for a map stage Spark itself considered
    successful. Fixed with Celeborn's own existing, public `CommitHandler.waitStageEnd`, not a new
    patch.
  - **`MapOutputStatistics` is not the only stats channel AQE reads, and adopting a stage does
    not populate the other one.** `ShuffleExchangeExec.runtimeStatistics` — what
    `AQEPropagateEmptyRelation` actually consults for row-count-driven decisions — comes from
    plain `SQLMetric` accumulators only a real map task ever increments. The first end-to-end
    pass adopted correctly, ran fewer tasks, and still produced a silently WRONG empty result,
    because AQE read zero rows off those never-incremented accumulators and rewrote the whole
    downstream plan to `LocalTableScan <empty>`. Fixed by capturing the real `dataSize`/
    `shuffleRecordsWritten` values at materialization time and setting them on the adopting
    stage's own exec node before returning from `tryAdoptShuffleStage`. Serious enough that the
    `StageResumeHook` trait's own doc comment in the `spark` fork was wrong and has been corrected
    — it previously implied `MapOutputStatistics` alone was sufficient, which is what led to
    writing this bug in the first place.
- **`resume-poc-sql`: §13.1's real question, against Iceberg — two rounds of real bugs, both
  found and fixed, not just the first one.** `SafePlanKey`/`StageDigest` had NO digest case for
  `BatchScanExec` (DSv2) at all — checked from source before writing a test, then confirmed
  empirically: a plain read with no dynamic pruning hashed to the literal constant string
  `"NODE:BatchScanExec:"` regardless of table, content, or snapshot, so the whole-plan digest was
  IDENTICAL before and after mutating an Iceberg table to a new snapshot, same query text — a
  false-POSITIVE match, the failure mode that would let a resumed query silently adopt an anchor
  captured against stale data. First fix (`SparkTable.table().currentSnapshot()`, a live,
  public, no-reflection API) closed that. A second, deliberately isolated check (comparing the
  LEAF digest directly, specifically to dodge the exprId-drift artifact that can mask a real bug
  behind a same-JVM whole-plan comparison) then found the first fix itself was blind to `VERSION
  AS OF` / `.option("snapshot-id", ...)` time-travel reads — `currentSnapshot()` always reports
  whatever is current RIGHT NOW, ignoring what the query actually pinned. Fixed by preferring the
  scan's own (reflectively-read, package-private) pinned snapshot id when present, falling back to
  the live current snapshot only for a genuinely unpinned read. Named-branch reads (`.option(
  "branch", ...)`, a real canary/audit workflow's mechanism) turned out to already be covered by
  this same fix for free, verified rather than assumed (a branch genuinely diverged from `main`
  gets a correctly different digest). The whole Iceberg-specific block is now wrapped in
  `try`/`NonFatal`, degrading to non-discriminating fallback fingerprinting on any unexpected
  failure rather than crashing digest computation for the whole query. 5 JVM runs (stability,
  drift, time-travel ×2, branch), 8 counted checks + 2 hard-gated scenarios, reproduced twice. All
  7 of this project's pre-existing test scripts re-verified passing after BOTH rounds of changes
  (no regression), and a latent `.m2`-pollution landmine in 6 of them (missing the `-o` offline
  flag) was found and fixed along the way. Still disclosed as open, not silently assumed solved:
  non-Iceberg DSv2 connectors (table-name-only discrimination, not content-aware), schema
  evolution interacting with an unchanged data snapshot, and non-Hadoop Iceberg catalogs (REST/
  Hive/Glue) — reasoned to be unaffected (`SparkTable` is catalog-implementation-agnostic) but not
  empirically exercised against any of them.
- **`resume-poc-e2e`: Iceberg + a real Polaris REST catalog + real Celeborn, combined — the gap
  every earlier Iceberg proof left open (`resume-poc-sql` proved the digest fix in isolation, a
  Hadoop catalog, no Celeborn at all).** `run-e2e-iceberg-test.sh` stands up a real Apache Polaris
  server (`docker.io/apache/polaris`) and a real S3-compatible store (MinIO) from scratch, creates
  a catalog through Polaris's actual REST management API (OAuth2 client-credentials, not a
  shortcut), then runs the same capture/adopt/cold pattern as `run-e2e-demo.sh` against a table
  read through a real `BatchScanExec`. 10/10 checks pass, reproduced twice from a cold standup
  (containers torn down and recreated between runs, not reused): correct result; a real stage
  adopted (the actual integration point — a `BatchScanExec`-fed stage inside an adopted stage's
  ancestry, not just a matching hash in a unit test); the adopted digest traces back to a real
  `StageCaptured` event; adopt ran strictly fewer tasks than a cold baseline of the identical
  query; all 4 reduce partitions genuinely populated; the scan was forced to plan as multiple
  mappers (`read.split.target-size` lowered) so `seedAdopted`'s per-mapper-stats-spreading fix —
  found on the plain-table skew path earlier this session — is now exercised on the Iceberg path
  too, not just assumed to generalize; and the actual go/no-go scenario `SafePlanKey`'s own doc
  comment names — `iceberg-mutate` appends a new snapshot after capture, then
  `iceberg-adopt-after-mutate` proves the stale digest is correctly MISSED (not silently adopted)
  and the recompute still produces the CORRECT post-mutation result. That last pair is the one
  this whole combined harness exists to prove and the first draft didn't have — an advisor review
  flagged that the original 6 checks were all positive-match (capture → adopt → equal digests →
  OK), which would all still pass even if the digest function were a constant; the cold-adopt
  check alone only proves an empty store adopts nothing, never that a populated-but-wrong store is
  correctly rejected.
  - **One infra root cause, found from Polaris's own source, not by guessing.** Polaris's
    SERVER-SIDE S3 client (used for its own CREATE TABLE/NAMESPACE existence checks) is built
    from the catalog's `storageConfigInfo` JSON at catalog-creation time — `pathStyleAccess`,
    `endpoint`, `endpointInternal` — NOT from generic `s3.*` catalog *properties*, which only
    reach the Iceberg client's own `FileIO`. Setting `s3.path-style-access` as a catalog property
    (the first several attempts) never reached Polaris's internal client at all, which then
    defaulted to virtual-hosted-style S3 addressing and tried to resolve a bucket-prefixed
    hostname Polaris's own container network can't see (`UnknownHostException`). Found by reading
    Polaris's own `PolarisRestCatalogMinIOIT` integration test source, not by trial and error
    against the REST API alone. `endpoint` is what Iceberg REST *clients* (Spark, on the host)
    see; `endpointInternal` is what Polaris itself (in its own container) uses — letting the two
    sides resolve the same object store through two different hostnames. `stsUnavailable: true`
    on the storage config sidesteps STS/AssumeRole entirely (plain MinIO doesn't implement it);
    both sides use the same static credentials instead, which is also why no IAM role/trust-policy
    machinery was needed here despite earlier LocalStack-based attempts assuming it would be.
  - **One real bug found and fixed while building the go/no-go test itself, orthogonal to the
    digest logic under test.** `iceberg-adopt-after-mutate`'s first draft reused capture's exact
    Celeborn `application.uniqueId` — correct for the true-adoption modes (they need to inherit
    the SAME registered shuffle), wrong here, where adoption is expected NOT to happen. Reusing
    the identity collided with the already-materialized shuffle Celeborn had registered under
    that app+shuffleId from the original capture run, and the recompute path silently returned
    zero rows (`got=Map()`) instead of throwing — a real result-correctness hazard, not a test
    artifact, caught only because `checkIcebergResultAfterMutate` compares against an
    independently-computed expected value rather than trusting `result=OK` from the log line
    alone. Fixed the same way `iceberg-adopt-cold` already did it (a distinct appUniqueId suffix)
    — the anchor store's own `queryId` (a separate concept from the Celeborn application identity)
    stays unchanged, so the anchor lookup still finds what capture wrote.
  - **Disclosed, not silently assumed solved:** `local[4]`, one Celeborn worker, one MinIO/Polaris
    instance, ~4500 rows, one catalog, one table shape. This closes "Iceberg+Polaris+Celeborn have
    never been run together, and Polaris was never tested at all" — it does not stand in for a
    production-scale, multi-tenant, or multi-catalog claim.

## What's explicitly NOT covered

Ranked roughly by how much it matters to the design's overall claim, most important first.

1. **`PlanDigest` fingerprint stability against a real DSv2 connector-backed scan (§13.1's actual
   go/no-go question) is now answered for Iceberg specifically — not proven for DSv2 generally.**
   `digest_stability.py` tests a different, older Python prototype, not `SafePlanKey`/`StageDigest`
   as actually built — running it would not have answered this. What does:
   `resume-poc-sql/run-iceberg-digest-test.sh`, found and fixed two real bugs in two rounds (see
   "What's covered beyond the ladder" above) — `BatchScanExec` had NO digest case at all
   originally (a false-POSITIVE non-discrimination bug worse than the instability §13.1 worried
   about), and the first fix for that turned out itself blind to time-travel/branch pins. Both
   closed: normal reads via the table's live current snapshot, pinned reads (time travel or a
   named branch) via the scan's own resolved snapshot id, preferred when present. The generic
   fallback for any OTHER DSv2 connector (table name + `scan.description()` + runtime filters) is
   real, tested-as-far-as-it-goes discrimination, better than the previous nothing, but not proven
   content-aware the way the Iceberg path now is — a same-named non-Iceberg table whose content
   changed without a connector-specific identity this code knows how to read would still collide.
   §13.1's question is closed for the one connector this project actually has on its classpath,
   including its time-travel and branch read modes, not for DSv2 as a category.
2. **Rungs 2–6 have never run against an AQE plan, in a coordinator that also gates on them.**
   `QueryResumeCoordinator` is deliberately scoped to non-AQE queries — `AdaptiveSparkPlanExec.
   executedPlan` returns whatever `currentPhysicalPlan` currently holds with no side effects if
   read pre-execution, so a pre-`.collect()` digest and a post-run digest can legitimately differ
   for the identical query, and comparing them would compare two different things. `StageDigest`
   (now proven driving a real adoption in `resume-poc-e2e`) is a different, AQE-capable
   mechanism, but `E2EStageHook` assembles its own inline acceptance ladder (schema version, rung
   7, rung 7.5) rather than reusing rungs 2–6's determinism-class/ancestor-rejection/tripwire
   machinery — that machinery still only exists in the non-AQE `QueryResumeCoordinator`.
3. **Rung 8's skew-spec half is a post-hoc fact-check, not a pre-adoption gate, anywhere in this
   project — and checked, not assumed, that no sound gate is possible with the information
   available at adoption time.** `AQEShuffleReadExec` (which carries skew specs) wraps a stage
   from above as its consumer; `OptimizeSkewedJoin` decides whether to split using that stage's
   own just-materialized stats, which don't exist yet when `tryAdoptShuffleStage` must decide.
   `resume-poc-e2e`'s `skew-adopt` mode confirms, empirically, that a real skew split against an
   adopted stage produces a correct result — but nothing in this project ever REJECTS an adoption
   because it might later be skew-split; only a real bug (fabricated per-mapper stats silently
   defeating skew-splitting entirely, fixed in `seedAdopted`) came out of even reaching that
   confirmation.
4. **Rung 7's real target case is unmet.** `confirmAlive` proves worker-process-dead is caught. It
   does not prove app-reclaimed-after-heartbeat-timeout-while-worker-stays-up is caught, which is
   the case the LLD emphasizes. A correct rung 7 needs a per-shuffle existence check or an
   app-registration check against the Celeborn Master, not a raw port-reachability probe. This
   project does the cheaper, weaker thing and says so in `resume-poc/README.md`.
5. **Rung 7.5's own disclosed residual gaps** (see `design-aqe-and-corrupted-rerun.md` §2 for full
   detail):
   - The narrow in-flight-revive-at-crash-instant window: if a revive's destroy RPC hasn't reached
     the worker yet at the instant the freshness probe runs, the superseded file is still intact
     and still reports its original length, so it passes as fresh. Real, disclosed, unmeasured.
   - The sorted-file/range-read interaction — reasoned from source, THEN empirically confirmed:
     `resume-poc-e2e`'s `skew-adopt` scenario captures a shuffle stage whose own later
     consumption (a real AQE skew-split read, in the same run) re-sorts the underlying Celeborn
     file, then has a second JVM's rung 7.5 probe (always a whole-file request) run against that
     now-sorted file. It does not false-reject — 9/9 checks pass, reproduced twice.
   - `readLocalShuffle=true` from a client that is not actually co-located with the worker is a
     real, working deviation from that field's documented meaning, not a protocol patch — flagged
     as the concrete question worth asking Celeborn upstream (replaces this note's original,
     now-resolved, epoch-authority question).
   - Pure durability — a worker acknowledging a commit, then losing unfsynced bytes on its own
     crash-restart — is unaudited. Whether Celeborn's own commit protocol can produce that gap is a
     question about Celeborn's internals this project hasn't investigated.
6. **`resume-poc-e2e`'s own disclosed scope limits** (see `resume-poc-e2e/README.md` for the full
   list):
   - Only `ShuffleExchangeExec` is handled (`stage.shuffle match { case s: ShuffleExchangeExec =>
     ...}`); a `ReusedExchangeExec`-wrapped stage falls through to "recompute," untested.
   - `dataSize`/`rowCount` (the fix for the `SQLMetric`/`runtimeStatistics` bug — see above) are
     whole-stage totals, not per-partition. `CoalesceShufflePartitions`, a different AQE rule that
     also reads per-partition sizes (for merging small partitions rather than splitting large
     ones), has no test here specifically targeting it — `OptimizeSkewedJoin` itself, and the
     per-mapper split within it, are no longer on this list; both are now proven end-to-end (see
     rung 8 above).
   - `CommitHandler.waitStageEnd` (the fix for the async-stage-end-commit race — see above) is
     bounded by `celeborn.client.push.stageEnd.timeout`, and this project captures anyway on
     timeout, producing a deliberately-empty anchor that will simply fail to match or fail rung
     7.5 later (safe under A-1) — but nothing alerts specifically on a capture that always times
     out, which would silently never contribute a usable anchor.
   - Two query shapes and a kill-mid-flight scenario are now tested (`run-e2e-demo.sh`,
     `run-e2e-kill-test.sh`) — still one hook implementation, one small local cluster, not a
     stress test, not a real multi-stage query with many simultaneously-adoptable stages at once.
7. **Two Celeborn patch-10 fixes are correct by inspection, not adversarially tested.**
   `ReducePartitionCommitHandler.adoptCommittedShuffle` seeds `shuffleMapperAttempts` from the
   crashed driver's captured array — but the demo's single local-mode run has no task retries or
   speculation, so the captured array is all zeros either way; the run cannot distinguish the fix
   from the bug it replaced. Likewise `LifecycleManager.adoptShuffle`'s watermark bump
   (`advanceShuffleIdGeneratorAtLeast`) prevents a second, unadopted shuffle in the same driver
   from colliding with an adopted `celebornShuffleId` — the demo has exactly one shuffle per
   driver, so no collision is possible either way. Both code paths run; neither has been exercised
   by a test that would fail without them.
8. **Restore-side fencing (LLD §8's P1 `FENCE` step) is not exercised.** `RedisAnchorStore` proves
   the *capture*-side write fence (§7.4) against a real concurrent writer. `tryAdopt` never calls
   `acquireGeneration` — it only reads anchors and adopts — so there is nothing on the restore path
   for a generation bump to protect in this slice (no incremental writer runs during restore).
9. **Everything the LLD scopes as out-of-bounds for this whole effort and that no PoC track
   attempts:** Kubernetes deployment mechanics, in-flight-task loss (the LLD itself calls this
   unsolvable and says to bound the loss, not close it), and non-AQE-plan handling beyond what
   `QueryResumeCoordinator` already covers.
10. **Batched freshness-probe fan-out (`BATCH_OPEN_STREAM`) is implemented but only exercised at
    small scale** (a handful of partitions, one or two workers, across `run-demo.sh` and
    `resume-poc-e2e/run-e2e-demo.sh`) — never load-tested against a shuffle with the partition
    counts or worker-fleet sizes a real production query would have.
11. **Celeborn application-identity reuse without adoption is a silent-empty-result hazard, found
    by accident while building the Iceberg go/no-go test, and nothing in this project detects or
    refuses it.** `iceberg-adopt-after-mutate`'s first draft reused capture's exact Celeborn
    `application.uniqueId` for a run that (correctly) did NOT adopt — the digest missed, so this
    was meant to be a plain independent recompute. Because Celeborn had already materialized a
    shuffle under that same app+shuffleId from the earlier capture run, the recompute's write
    collided with it and the read silently returned zero rows instead of throwing or erroring.
    Worked around in the test by using a distinct appUniqueId (mirroring `iceberg-adopt-cold`'s
    existing pattern) — but that is a workaround for the TEST, not a fix in the mechanism: a real
    resumed driver that restarts under the same application identity and then does NOT adopt (a
    digest miss, a config change, an expired anchor) hits this exact shape, and nothing here
    would catch it. No coordinator in this project checks for or rejects identity reuse across a
    non-adopting restart. This is a real, disclosed gap in the mechanism, not just a test-harness
    footnote.

## Repo / commit map

| Repo | Latest relevant commit | Remote |
|---|---|---|
| `resume-poc` | `08498f7` rung 7.5 freshness check | `github.com/unikdahal/resume-poc` |
| `resume-poc-sql` | `8571253` stage-granular digest + Iceberg BatchScanExec fix (2 rounds: mutation + time-travel/branch) | `github.com/unikdahal/resume-poc-sql` |
| `resume-poc-e2e` | `05f9214` combined demo + skew (rung 8) + kill-mid-flight + Iceberg/Polaris/Celeborn combined | `github.com/unikdahal/resume-poc-e2e` |
| `spark` fork | `a7b2868f108` (`StageResumeHook` doc fix, on top of `8a666cc8864`) | `github.com/unikdahal/spark` (`myfork`) |
| `celeborn` fork | `fb9ccfa93` `adoptShuffle`/`confirmAlive` | `github.com/unikdahal/celeborn` (`myfork`) |

## How to reproduce every claim above

```
resume-poc/run-demo.sh                    # rungs 1, 7, 7.5, half of 8; scheduler-skip fact; MapStatus sizes
resume-poc/run-kill-before-fetch-test.sh  # fetch-failure recovery after adoption
resume-poc/run-redis-fence-test.sh        # LLD §7.4 concurrent-writer fencing, real Redis
resume-poc-sql/run-stage-digest-test.sh   # AQE stage-digest survives a later stage's replan
resume-poc-sql/run-iceberg-digest-test.sh # §13.1 against real Iceberg: stability, drift, time-travel, branch
resume-poc-e2e/run-e2e-demo.sh            # the combined AQE+SQL+Celeborn adoption demo, 9/9 checks
resume-poc-e2e/run-e2e-kill-test.sh       # fetch-failure recovery for an adopted AQE stage, 2/2 checks
resume-poc-e2e/run-e2e-iceberg-test.sh    # Iceberg + a real Polaris REST catalog + Celeborn, combined, 10/10 checks
```
(each script documents its own prerequisites — JDK 17, a locally-built `./celeborn`, and for the
Redis test, a reachable Redis instance — at the top of the file.)

## Bottom line

The mechanism half of this design — "can a second JVM correctly and safely re-attach to a first
JVM's Celeborn-backed shuffle output, and correctly refuse when it shouldn't" — is built and
tested about as thoroughly as a solo effort can manage, and it has been proven *combined*, not
just piecewise: `resume-poc-e2e` runs real AQE SQL queries against a real Celeborn cluster and
gets shuffle stages genuinely skipped by a second JVM, surviving the exact cross-strategy pressure
the stage-digest fix exists for, a real AQE skew split against fabricated-but-now-accurate
per-mapper stats, a worker dying mid-flight after adoption with AQE's own replanning already
committed to the adopted stats, and — the gap that stood open longest — Iceberg read through a
real Apache Polaris REST catalog server feeding an adopted Celeborn-backed stage, including the
actual go/no-go scenario (table mutated after capture, stale digest correctly missed, not
silently adopted). Building that combined harness is also what surfaced this
project's most consequential findings — the async stage-end commit race, the
`SQLMetric`/`runtimeStatistics` gap that `MapOutputStatistics` alone doesn't cover, and
`seedAdopted`'s fabricated-stats shape silently defeating AQE's own skew optimization — the kind
of bug that only shows up when the pieces actually run together, which is exactly why closing this
gap mattered more than any other item on this list. Every one of those findings was chased down
and fixed, not just written up as a known limitation. The same discipline closed §13.1's own
stated go/no-go question for real, not by rerunning an old prototype but by pointing the actual
`SafePlanKey`/`StageDigest` code at a real Iceberg table and finding a serious, previously-hidden
false-positive digest-collision bug in the process — fixed, not just documented. The SQL/planning
half that's still open — rungs 2–6 in a coordinator that also handles AQE, the structural fact
that rung 8's skew check can only ever be a post-hoc fact-check rather than a pre-adoption gate
given what's knowable at adoption time, and digest stability for DSv2 connectors OTHER than
Iceberg specifically — is narrower now than at any earlier point in this project, but still real,
still unclosed. Nothing in this document should be read as "production ready"; it should be read
as an accurate map of exactly where the proven ground ends.
