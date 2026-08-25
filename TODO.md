# TODO & PROGRESS — resumable Spark execution

This file is both the dispatch list and the progress record. Status symbols:
✅ done (evidence exists) · 🔄 in flight (code written, verification queued/running) · ⬜ pending.
Detailed evidence for every claim: [docs/upstream/VERIFICATION-STATUS.md](docs/upstream/VERIFICATION-STATUS.md).
Planned commit/PR splits: [docs/upstream/PATCH-SPLIT-PLAN.md](docs/upstream/PATCH-SPLIT-PLAN.md) (session-deltas section).

Read `README.md` and `RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md` for context; this file is the
worklist, not the design. Design docs per mechanism live in `docs/upstream/`
(`ROW-LEVEL-RECOVERY-DESIGN.md`, `AUTH-PRECONDITION-DESIGN.md`, `CELEBORN-BLOB-BACKEND-DESIGN.md`).

**Audit pass (2026-08-25, this revision):** every status below was re-verified against the actual
git history and current file contents in the three forks — not against the previous revision of
this file. Where the previous revision disagreed with what is actually in the tree, the tree wins
and the correction is noted. Two real discrepancies were found and are called out in
[§ Audit findings](#audit-findings) rather than fixed here, per instruction to update this document
only.

---

## Rules every agent must follow

1. **THE #1 PRIORITY: production-ready implementation** — work/scale/ship beats docs, cleanup,
   planning. Secondary work happens only while builds hold the machine.
2. **NEVER sleep or idle-wait.** While anything runs: review code for bugs, improve docs, plan,
   clean up stale artifacts (never delete evidence a doc cites), think deeper.
3. **One build at a time machine-wide**: `exec 9>verification/logs/.verification.lock && flock 9`.
   Queue follow-ups behind the lock instead of waiting.
4. **Build flags**: JDK 17 at `~/.sdkman/candidates/java/17.0.11-tem`, `taskset -c 0,1`,
   Maven `-T 1 -XX:ActiveProcessorCount=2`, Gradle `--max-workers=2`.
5. **Never edit source inside a repo while that repo is being built**; other repos are fair game.
6. **Run each project's own gates**: Celeborn spotless + ConfigurationSuite; Iceberg
   errorprone-in-compile + spotless; Spark scalastyle.
7. **Evidence over claims**: PASS only if a run on this machine printed it. Reproduce before fixing;
   decide whether code, test, or fixture is wrong first.
8. **Commit only clean, finished work.** Before committing, `git status --porcelain` your repo and
   account for every modified file — an edit unrelated to the task you're closing out is a sign
   something was left mid-flight. See the Controller.scala item in Audit findings for what this
   looks like when it isn't done.
9. Commits: one concern, what changed + why alternatives were rejected,
   `Co-Authored-By`/`Claude-Session` trailers. Only when the operator asks.

---

## Where the project stands (2026-08-25, re-verified)

| Lane | State |
|---|---|
| Spark (`spark-resumable-upstream`) | **Clean, 0 uncommitted files.** `e3faa500b29` (row-level/transaction surface) and `eb657b051ef` (DAGScheduler fixture fix) landed and pushed. Confirmed in tree: `HashPartitioner(2)` / two-reducer fixture is present in `DAGSchedulerSuite.scala`, so the scheduler finding is closed, not open — see Audit findings for the doc that still says otherwise |
| Celeborn (`celeborn`) | A1, A3, A4 landed and pushed (`b80ba5f72`, `11c0a17fd`, `b4e7cad58` — the last is the E1-unblocking `shuffleBlockResolver` compile fix). **1 uncommitted file**, unrelated to any group task — see Audit findings |
| Iceberg (`oss-fixes/iceberg`) | **Clean, 0 uncommitted files.** B1, B2, B3, B4, B5, B6 all landed and pushed (`5ee1e72`, `372fb10`, `c681dd1`, `f1e81e8`, `d5fcfc2`). Confirmed in tree: `gradle/libs.versions.toml` pins `spark41 = "5.0.0-SNAPSHOT"` |
| Docs | D-group complete: protocol spec folds in blob backend; runbook has blob ops; threat model T-14/T-15; split plan CIP-4/CIP-5 + session deltas; two new design docs |
| Verification pipeline | E1 probe **PASS**; E2 two-driver harness on attempt 5 — it has already flushed out four real defects (probe path/JDK pin, dual-target `shuffleBlockResolver`, scala-2.13.17 worker compile, upstream HA `node.ids` startup crash — all fixed). C9 benchmark generation + C6 golden-fixture regeneration queued on the Spark lane |

### Immediate next actions (in order)

1. **Resolve the two audit findings below** — five minutes, no build needed for either.
2. Fan out the remaining work: A2, A5–A10, B7, C5–C9, E2/E3 (see group sections). Nothing above is
   still gated on an "owner commit decision" — everything verified in the last session is already
   landed and pushed to all three forks.

---

## Audit findings

Two items the previous revision of this file did not surface. Neither blocks other work; both are
quick for whoever picks up the relevant repo.

**F-doc-1 — `VERIFICATION-STATUS.md` §2a contradicts itself on the DAGScheduler finding, and the
tree says the fix is real.** The "Resolution" paragraph in §2a claims the fixture was corrected and
reruns green at 233/233. Directly below it, the "2a-findings" follow-up table still shows
`DAGSchedulerSuite repro | FAIL (same test, deterministic) | 232/233` and the text says "the
adoption finding remains open." Both were written at the same doc-edit timestamp (05:55:48), after
the actual fix commit `eb657b051ef` (03:31:29) — so the stale FAIL row was carried forward instead
of updated. Confirmed directly against source: `DAGSchedulerSuite.scala` currently has the corrected
two-mapper/two-reducer fixture (`new HashPartitioner(2)`, `Array(10L, 20L)`), matching the commit
message's fix, not the failing state the follow-up table describes. **Action for whoever touches
this doc next:** delete or correct the stale FAIL row in the "2a-findings" table and drop "the
adoption finding remains open" — the finding is closed. Five-minute doc edit, no rebuild required to
make the correction (though re-running the suite once more to reconfirm 233/233 costs nothing).

**F-doc-2 — one uncommitted file in `celeborn` unrelated to any tracked task.**
`worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Controller.scala` has a local,
uncommitted diff to `validChunkOffsets` (unboxing `jList[java.lang.Long]` to primitive `long` before
the checks). It is not part of the recovery work this file tracks — nothing in A1–A10 touches chunk
offset validation — and it looks unfinished: `primitives.forall(_ != null)` is dead code once the
values are unboxed to primitive `long` (a primitive can't be null), and the change from
`offsets.isEmpty || offsets.get(0) == 0L` to `primitives.nonEmpty && primitives.head == 0L` flips the
behavior for empty offset lists from accepted to rejected, which is exactly the kind of change that
needs a test, not just a rewrite. **Action:** whoever owns this file next should either finish it
(fix the dead check, confirm the empty-list behavior change is intended, add a test) or
`git checkout` it back to committed state. Do not build over it without deciding first — rule 8 above
exists because of exactly this kind of leftover.

---

## Group A — Celeborn

- ✅ **A1 landed**: worker-to-worker replicate RPC, replicated repair command, and the leader-only
  repair scheduler are committed (`11c0a17fd`) and pushed. 15/15 suites were green before landing;
  `b4e7cad58` (dropping a removed `shuffleBlockResolver` override) was a follow-up compile fix
  required for the Spark 5 API, also pushed.
- ✅ **A3 auth precondition**: committed `b80ba5f72`. Recovery without `celeborn.auth.enabled=true`
  fails at session construction naming both `celeborn.auth.enabled` and
  `spark.celeborn.auth.enabled`. Tests: `CelebornShuffleStageRecoveryExtensionSuiteJ` 2/2.
  Follow-ups flagged, not blocking: `tests/spark-it/CelebornDriverRecoverySuite` and
  `dev-harnesses/celeborn-cluster-harness/.../TwoDriverShuffleJob.scala` enable recovery without
  auth in their fixtures and must add the flag before they're trusted as end-to-end proof.
- ✅ **A4 per-application quota**: committed `11c0a17fd`, pushed. `maxInlineBytesPerApp` /
  `maxInlineRecordsPerApp` conf keys, per-app ledgers via reserve/release, three-way distinguishable
  rejections (per-recovery / per-application / cluster-wide), HA snapshot validation + restore fold.
  `RecoveryInlineCapacitySuite` 4/4; `RecoveryBlobPointerSuite` grew to 11/11 in the same commit
  (includes the shrink-regression test below); `ConfigurationSuite` 8/8 with regenerated
  `docs/configuration/master.md`.
- ✅ **Repair capacity-leak fix**: real bug, found during A4 verification and fixed in the same
  commit. Shrinking repairs (replacing long dead replica IDs with short live ones) leaked byte
  budget because release only tracked records, not the byte delta — per-recovery and global
  counters crept until publications were refused for capacity no longer in use. Fixed by splitting
  bytes/records release in `releaseRecoveryTaskCommitCapacity`; regression test in
  `RecoveryBlobPointerSuite`.
- ⬜ **A2 worker-to-worker blob repair integration test.** Confirmed still absent: no
  `MiniClusterFeature`-based suite drives `ReplicateRecoveryBlob` across two real worker processes.
  `RecoveryBlobReplicationSuite` (client module) exercises the replication *logic* against a fake
  transport, which is a different test than this task asks for. After A1 (already landed).
- ⬜ **A5 explicit completed-execution release**: master RPC + meta manager + client
  `LifecycleManager`; releasing twice harmless, capacity returns to zero. Confirmed still absent —
  no release/complete RPC exists in the tree. Blocked on nothing now that A1 has landed.
- ⬜ **A6 remote-storage fallback tier**: design §8; second tier never satisfies quorum alone.
- ⬜ **A7 HA chaos: leader change during publication** — new HA suite asserting exactly-one-pointer
  convergence + idempotent replay.
- ⬜ **A8 old-executor publication after takeover** — fenced rejection reported as fenced.
- ⬜ **A9 rolling-upgrade protocol compatibility** — five replicated maps across versions; prove or
  correct `UPGRADE-AND-ROLLBACK.md` §2's silent-loss claim.
- ⬜ **A10 (new — promoted from review flags) repair-executor hardening.** Two items recorded in
  `VERIFICATION-STATUS.md` finding #6 during A1 review, never turned into tracked work:
  (a) `RpcRecoveryBlobRepairExecutor`'s per-worker `RpcEndpointRef` cache never invalidates, so a
  source worker that restarts on the same host:port keeps getting a possibly-dead ref — the
  worker-side `recoveryBlobPeer` cache has the same shape, so fixing the pattern once should apply
  to both; (b) `askSync` runs sequentially per repair cycle at the default RPC timeout, so one hung
  source bounds a whole cycle's wall-clock time — the "bounded per cycle" property in the design
  holds for task *counts*, not for wall time. Neither is urgent; both are real.

## Group B — Iceberg

- ✅ **B1 ledger horizon ≥ recovery window**: committed `5ee1e72`, pushed.
  `SnapshotProducer.ledgerHorizonWarning` + `commit.idempotency.recovery-window-ms` property +
  below/equal/above/undeclared boundary tests. An errorprone `DefaultLocale` defect
  (`String.format` without `Locale.ROOT`) was caught by the project's own gate and fixed in the same
  commit.
- ✅ **B2 drift fails closed** + **B3 losing-writer cleanup**: landed inside `c681dd1`. Seven
  per-dimension drift tests plus the discard/deletion-failure/foreign-message trio, all green
  against the fork (`recoveryTest` 18/18).
- ✅ **B4 catalog compatibility matrix**: committed `372fb10`, pushed.
  `CatalogTests.testIdempotencyLedgerPropertyRoundTrip` (covers every suite extending
  `CatalogTests`: JDBC, Hive, REST, Nessie, InMemory, BigQuery) plus a `TestHadoopCatalog` twin,
  since Hadoop keeps properties in metadata JSON rather than a store. Publishes at exactly the
  4096-byte field boundary and requires one snapshot, not two, after a fresh-catalog-handle reload.
- ✅ **B5 API verdict recorded**: `idempotencyKey` needs an upstream vote + revapi exception;
  rationale in `ICEBERG-PROPOSAL.md` Compatibility section.
- ✅ **B6 version decision executed and proven, in the tree right now.**
  `gradle/libs.versions.toml:93` reads `spark41 = "5.0.0-SNAPSHOT"` — confirmed directly, not from a
  commit message. `c681dd1` ported the drifted view/state API (`SparkView` now extends the
  builder-class `View`; `ViewInfo`/`ViewChange`/`SupportsReplaceView` removed;
  `BatchWriteRecoveryState` dropped per-partition messages and row counts since the durable store
  owns them; `StagedSparkTable` picks one `reportDriverMetrics` default explicitly to resolve a
  diamond). The recovery suite moved to its own Gradle sourceset/task so it can target the fork
  while the rest of the module stays on the released target — the rest of `spark/v4.1`'s helpers
  (jetty servlets, spatial types) still assume stock Spark and were correctly left alone rather than
  ported wholesale. `f1e81e8` did the actual pin flip; `d5fcfc2` is a spotless-only follow-up.
  Evidence: `recoveryTest` 18/18 against `5.0.0-SNAPSHOT` on JDK 17. **Nothing pending here.**
- ⬜ **B7 recovery pins wired into `SparkTable`/`SparkScanBuilder`** — blocked on C5.

## Group C — Spark

- ✅ **C1 triage**: full table in `VERIFICATION-STATUS.md` §2a. 11 suites run against the
  post-row-level tree; findings enumerated there.
- ✅ **C2 write-recovery suite trustworthy**: `BatchWriteRecoverySuite` 8/8 green; the stale
  August-23 failure counts are retired and no longer describe the current file.
- ✅ **C3/F9 settled**: recovery keys discriminate past `maxToStringFields` truncation once the
  fixture aggregates one expression per column; withdrawn as a finding, audit trail in
  `CLAUDE-WORKLOG.md`.
- ✅ **C4 row-level work landed and pushed**, in two commits: `e3faa500b29` (the surface itself —
  generation fence, v4 manifest, per-task semantic-action summary; connector payload stays opaque
  by keeping compatibility metadata beside rather than inside the manifest) and `eb657b051ef` (the
  DAGScheduler fixture fix found while verifying it). Both suites green: row-level suites
  7+10+4+9, `RecoveryTaskCommitSuite` 25/25, `SparkSessionExtensionSuite` 50/50,
  `DAGSchedulerSuite` 233/233 confirmed in tree — see Audit findings for the stale doc row.
  Two real gate defects were found and fixed in the same commit: a test-compile collision (a
  private helper named `execute` colliding with `Suite.execute`) and an 11-parameter scalastyle
  violation, resolved by grouping three schema options into `RowLevelSchemas` rather than raising
  the argcount limit.
- ⬜ **C5 expose execution recovery identity during source anchoring** — blocks B7. Design sketch in
  `RETENTION-AND-SIZING.md` §2 / `ICEBERG-PROPOSAL.md` open items.
- ⬜ **C6 envelope version negotiation** — decide a readable-version set before the API freeze.
  Confirmed gap: `sql/core/src/test/resources/recovery/` holds golden fixtures for envelope
  versions v1 and v2 only; `RecoveryTaskCommitEnvelope.writeManifest` gained `ManifestVersion4` for
  row-level manifests in C4 and has no golden fixture pinning its layout against drift. Regenerate
  with `SPARK_GENERATE_GOLDEN_FILES=1` once C6's version-negotiation decision is made.
- ⬜ **C7 freeze API artifact** (`dev/create-recovery-api-artifact.sh`) — confirmed untouched since
  the `ace8a2de502` checkpoint. After C4 (done) and the C6 decision. Do not freeze while any
  suite is red — C1's own table is the reason three other gates in this project ended up broken.
- ⬜ **C8 MiMa review** — after C7.
- ⬜ **C9 benchmark results** — `SPARK_GENERATE_BENCHMARK_FILES=1`, then replace the arithmetic
  table in `RETENTION-AND-SIZING.md` §3 with measured numbers.
- ✅ **C10 AQE coverage, more complete than previously recorded.** `ShuffleStageRecoveryAQESuite`
  exists and is 2/2 green (`results-spark-c10.tsv`): `"recovered statistics and map output are
  shared by reused shuffle stages"` and `"AQE coalesced and skew reads retain a recovered shuffle
  stage"`. That is stage reuse, coalescing, *and* skew all exercised — the previous revision of this
  file said "still owed: stage-reuse/coalescing/skew," which undersold what the two tests actually
  cover. Marking done; if a reviewer wants reuse/coalescing/skew as three separate test cases rather
  than two combined ones, that is a follow-up, not an open gap.

## Group D — Documentation

- ✅ D1 `VERIFICATION-STATUS.md` refreshed through 2026-08-25 (§1a, §2a, follow-ups, iceberg lane,
  findings) — **but see F-doc-1 above**, one row in it is stale relative to the tree.
- ✅ D2 `PROTOCOL-SPEC.md`: blob transport/pointer/quorum ordering + `connector.recovery` package
  move + reading list.
- ✅ D3 `RUNBOOK.md`: blob operations section (metric names, repair-keeping-up signal,
  no-live-replica runbook).
- ✅ D4 `PATCH-SPLIT-PLAN.md`: CIP-4 blob backend, CIP-5 renumber, PR-1 package-move note, session
  deltas.
- ✅ D5 `THREAT-MODEL.md`: T-14 mitigation note (per-application quotas, `facdbe2`) and T-15
  blob-upload poisoning + trust-boundary row + residual-risk update.
- ➕ Added since the last audit: `ROW-LEVEL-RECOVERY-DESIGN.md`, `AUTH-PRECONDITION-DESIGN.md`, docs
  index links, `TEST-STRATEGY.md` suite table update, `CONFIG-AND-ENABLEMENT.md` auth example fix.
- ⬜ **D6 (new) fix F-doc-1** — the concrete doc edit described in Audit findings. Small enough to
  fold into whichever task next touches `VERIFICATION-STATUS.md`, but tracked here so it isn't lost.

## Group E — End-to-end harnesses

- ✅ **E1 version probe**: **PASS** after four real fixes it flushed out (probe paths, JDK pin,
  dual-target resolver shim, upstream HA `node.ids` startup crash fixed in `CelebornConf`).
  Celeborn's Spark client compiles against `5.0.0-SNAPSHOT`
  (`-Pspark-4.2`), after fixing the probe's own path bug and, in `b4e7cad58`, dropping the
  `shuffleBlockResolver` override the Spark 5 `ShuffleManager` interface no longer declares.
- ⬜ **E2 two-driver shuffle test**: zero-task assertion; needs E1 (done) + a working v4.1 client.
  The harness job (`dev-harnesses/celeborn-cluster-harness/.../TwoDriverShuffleJob.scala`) must add
  `spark.celeborn.auth.enabled=true` before this counts as evidence — it does not yet, per the A3
  follow-up note above.
- ⬜ **E3 two-driver write test**: blocked on C7 (API freeze) only now — B6 is done, which was the
  other blocker.

---

## What can run at the same time

| Lane | Tasks | Conflicts with |
|---|---|---|
| Celeborn | A2, A5–A10 all independent of each other except A2 touches worker+spark-it test trees | overlapping files only |
| Iceberg | B7 only remaining item, blocked on C5 | none outside Iceberg |
| Spark | C5 → C7 → C8 sequential; C6 decision, C9 fully independent | none outside Spark |
| Docs | D6 (five-minute fix); anything else not yet written | none |
| Harness | E2 now (E1 done); E3 after C7 | needs build lock |

## Highest value if you can only pick three

1. **A5 explicit completed-execution release** — without it, capacity only returns when leases
   lapse, which caps real-cluster scalability of repeated resumable jobs. Nothing blocks starting
   this now.
2. **C5 → B7** — expose the execution recovery identity during source anchoring, then wire Iceberg's
   already-implemented pin lifecycle to it. This is the last piece of the read-side story and it
   unblocks a whole group task, not just one.
3. **E2 two-driver shuffle test** — E1 is proven; this is the actual end-to-end evidence the project
   has been assembling every other piece to support. Remember the auth flag first.
