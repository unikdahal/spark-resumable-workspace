# TODO & PROGRESS — resumable Spark execution

This file is both the dispatch list and the progress record. Status symbols:
✅ done (evidence exists) · 🔄 in flight (code written, verification queued/running) · ⬜ pending.
Detailed evidence for every claim: [docs/upstream/VERIFICATION-STATUS.md](docs/upstream/VERIFICATION-STATUS.md).
Planned commit/PR splits: [docs/upstream/PATCH-SPLIT-PLAN.md](docs/upstream/PATCH-SPLIT-PLAN.md) (session-deltas section).

Read `README.md` and `RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md` for context; this file is the
worklist, not the design. Design docs per mechanism live in `docs/upstream/`
(`ROW-LEVEL-RECOVERY-DESIGN.md`, `AUTH-PRECONDITION-DESIGN.md`, `CELEBORN-BLOB-BACKEND-DESIGN.md`).

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
8. Commits: one concern, what changed + why alternatives were rejected,
   `Co-Authored-By`/`Claude-Session` trailers. Only when the operator asks.

---

## Where the project stands (2026-08-25)

| Lane | State |
|---|---|
| Spark (`spark-resumable-upstream`) | All 11 recovery suites green incl. 4 row-level suites; DAGSchedulerSuite 233/233 after fixture fix; scalastyle green. 12 files uncommitted awaiting owner's commit decision |
| Celeborn (`celeborn`) | A3 auth precondition **committed** (`b80ba5f72`). Blob backend implemented; repair capacity-leak fixed (+regression test); repair-executor flags recorded. 11 files of blob work still uncommitted (A1 landing decision) |
| Iceberg (`oss-fixes/iceberg`) | Ledger horizon guard + catalog matrix + drift/cleanup tests written. v4.1 module ported to the fork's evolved View/State API (unverified). B6 pin flipped to `5.0.0-SNAPSHOT` |
| Docs | D-group complete: protocol spec folds in blob backend; runbook has blob ops; threat model T-15; split plan CIP-4/CIP-5 + session deltas; two new design docs |
| Verification pipeline | Self-chaining via flock: `run-celeborn-repair-fix` → `run-iceberg-final` → `run-celeborn-a4`. Results land in `verification/logs/results-*.tsv` |

### Immediate next actions (in order)

1. **Verify the Iceberg v4.1 view/state port compiles and tests pass**
   (`run-iceberg-final.sh` leg 4 — already queued; exercises B6+B2+B3).
2. **Celeborn A4 + repair-fix suites** (`run-celeborn-a4.sh`, queued) — includes configdocs
   regeneration gate for the new conf keys.
3. **Owner decisions needed**: commit+push A1's 11-file Celeborn blob tree; commit Spark C4 tree;
   then A2/A5/A6/A7/A8/A9 and E-group unlock.
4. **E1 probe rerun** — should pass now that the fork publishes `sql/hive`; unblocks cluster E2E.

---

## Group A — Celeborn

- ✅ **A1 evidence**: 15/15 suites green on this machine (§1a of VERIFICATION-STATUS), including the
  statemachine rerun (5/5). **Remaining gate: owner says "commit and push"** — the 11 modified/new
  files (replicate RPC, replicated repair, repair scheduler) plus the repair accounting fix.
- ✅ **A3 auth precondition**: committed `b80ba5f72`. Recovery without `celeborn.auth.enabled=true`
  fails at session construction naming both keys. Tests: `CelebornShuffleStageRecoveryExtensionSuiteJ`.
  Follow-ups flagged, not blocking: spark-it `CelebornDriverRecoverySuite` and harness
  `TwoDriverShuffleJob.scala` enable recovery without auth and must add the flag when they run.
- 🔄 **A4 per-application quota**: implemented this session — `maxInlineBytesPerApp` /
  `maxInlineRecordsPerApp` conf keys, per-app ledgers maintained by reserve/release, three-way
  distinguishable rejections (per-recovery / per-application / cluster-wide messages), HA snapshot
  validation + restore fold, reset path. New suite `RecoveryInlineCapacitySuite` (4 tests).
  **Pending: celeborn-a4 chain (spotless, suites, configdocs regen)**.
- 🔄 **Repair capacity-leak fix** (found in review): shrinking repairs leaked byte budget; fixed via
  bytes/records release split. Regression test added to `RecoveryBlobPointerSuite`.
  **Pending: celeborn-repair-fix chain (running)**.
- ⬜ **A2 worker-to-worker blob repair integration test**: new suite under `celeborn/tests/spark-it`
  or worker tests; drive `ReplicateRecoveryBlob`, assert identical bytes + failure-not-empty-success
  when the source lost the payload. After A1 lands.
- ⬜ **A5 explicit completed-execution release**: master RPC + meta manager + client
  LifecycleManager; releasing twice harmless, capacity returns to zero. Blocked on A1 landing.
- ⬜ **A6 remote-storage fallback tier**: design §8; second tier never satisfies quorum alone.
- ⬜ **A7 HA chaos: leader change during publication** — new HA suite asserting exactly-one-pointer
  convergence + idempotent replay.
- ⬜ **A8 old-executor publication after takeover** — fenced rejection reported as fenced.
- ⬜ **A9 rolling-upgrade protocol compatibility** — five replicated maps across versions;
  prove or correct UPGRADE-AND-ROLLBACK §2's silent-loss claim.
- 🔎 **Review flags for A1 owner** (recorded in VERIFICATION-STATUS findings #6):
  repair-executor endpoint-ref cache never invalidates; sequential askSync bounds cycle wall-clock.

## Group B — Iceberg

- 🔄 **B1 ledger horizon ≥ recovery window**: `SnapshotProducer.ledgerHorizonWarning` +
  `commit.idempotency.recovery-window-ms` property + boundary tests written; errorprone
  DefaultLocale caught and fixed. **Pending: iceberg-final core legs.**
- 🔄 **B2 drift fails closed** + **B3 losing-writer cleanup**: seven per-dimension drift tests +
  discard/deletion-failure/foreign-message tests written into `TestSparkWriteRecovery`.
  **Pending: v41 leg compile of the view/state port.**
- 🔄 **B4 catalog compatibility matrix**: `CatalogTests.testIdempotencyLedgerPropertyRoundTrip`
  (covers JDBC/Hive/REST/Nessie/InMemory) + HadoopCatalog twin. **Pending: iceberg-final.**
- ✅ **B5 API verdict recorded**: `idempotencyKey` needs an upstream vote + revapi exception;
  rationale in ICEBERG-PROPOSAL.md Compatibility section.
- 🔄 **B6 version decision executed**: pin flipped to fork `5.0.0-SNAPSHOT`; fork fully published to
  mavenLocal (incl. sql/hive tonight). Fork's View/State API drift ported (SparkView extends
  builder-class View; ViewInfo/ViewChange/SupportsReplaceView removed; createView/replaceView
  re-signatured; SparkWriteRecovery state reduced to the global-commit oracle).
  **Pending: compile+test proof.** Revert path documented for post-PR-1.
- ⬜ **B7 recovery pins wired into SparkTable/SparkScanBuilder** — blocked on C5.

## Group C — Spark

- ✅ **C1 triage**: full table in VERIFICATION-STATUS §2a. 11 suites, findings listed there.
- ✅ **C2 write-recovery suite trustworthy**: BatchWriteRecoverySuite 8/8 green; stale counts retired.
- ✅ **C3/F9 settled**: keys discriminate past truncation; withdrawn in CLAUDE-WORKLOG + audit trail.
- 🔄 **C4 row-level work ready to land**: all four suites green after two test-compile fixes and the
  `RowLevelSchemas` argcount fix. **Gate: owner commit decision** (split plan written).
- ⬜ **C5 expose execution recovery identity during source anchoring** — blocks B7; design sketch in
  RETENTION-AND-SIZING §2 / ICEBERG-PROPOSAL open items.
- ⬜ **C6 envelope version negotiation** — decide readable-version set before API freeze. Now also
  covers finding: **no golden fixture pins ManifestVersion4** (regenerate with
  `SPARK_GENERATE_GOLDEN_FILES=1`).
- ⬜ **C7 freeze API artifact** (`dev/create-recovery-api-artifact.sh`) — after C4 commits + C6
  decision. Do not freeze while tests are red.
- ⬜ **C8 MiMa review** — after C7.
- ⬜ **C9 benchmark results** — `SPARK_GENERATE_BENCHMARK_FILES=1`, replace arithmetic table in
  RETENTION-AND-SIZING §3.
- 🔄 **C10 AQE coverage** — scheduler-side de-risked (adoption defect hypothesis resolved as fixture
  bug; suite 233/233). Still owed: stage-reuse/coalescing/skew behaviours vs an adopted stage.

## Group D — Documentation

- ✅ D1 VERIFICATION-STATUS refreshed through today (§1a, §2a, follow-ups, iceberg lane, findings).
- ✅ D2 PROTOCOL-SPEC: blob transport/pointer/quorum ordering + package move + reading list.
- ✅ D3 RUNBOOK: blob operations section (real metric names; repair keeping up; no-live-replica runbook).
- ✅ D4 PATCH-SPLIT-PLAN: CIP-4 blob backend, CIP-5 renumber, PR-1 package-move note, session deltas.
- ✅ D5 THREAT-MODEL: T-15 blob-upload poisoning + trust-boundary row + residual-risk update.
- ➕ Added since: ROW-LEVEL-RECOVERY-DESIGN.md, AUTH-PRECONDITION-DESIGN.md, docs index links,
  TEST-STRATEGY suite table update, CONFIG-AND-ENABLEMENT auth example fix.

## Group E — End-to-end harnesses

- 🔄 **E1 version probe**: expected PASS now (fork publishes everything incl. sql/hive); rerun cheap.
- ⬜ **E2 two-driver shuffle test**: zero-task assertion; needs E1 + working v4.1 client.
  Harness job must add `spark.celeborn.auth.enabled=true` (A3 precondition).
- ⬜ **E3 two-driver write test**: blocked on B6 verification + C7.

## What can run at the same time

| Lane | Tasks | Conflicts with |
|---|---|---|
| Celeborn | A2/A5/A6/A7/A8/A9 after A1 lands; A4 verification queued | overlapping files only |
| Iceberg | B1/B2/B3/B4 verification queued; B7 after C5 | none outside Iceberg |
| Spark | C5 → C7 → C8; C6 decision; C9; C10 | none outside Spark |
| Docs | anything not yet written | none |
| Harness | E1 now; E2 after v41 green | needs build lock |

## Highest value if you can only pick three

1. **Land the trees** — owner decisions on A1 + C4 commits (everything else fans out from these).
2. **Iceberg v41 green** (B6/B2/B3) — unblocks E2/E3, the actual end-to-end proof.
3. **A5 completed-execution release** — without it capacity only returns when leases lapse, which
   caps real-cluster scalability of repeated resumable jobs.
