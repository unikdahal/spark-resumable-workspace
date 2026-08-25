# Verification status — evidence, not assessment

**Rule for this document:** a row says PASS only if a test run on this machine printed a pass. Source
reading is not verification and is labelled *implemented* instead. Codex's
`recovery_status_report.pdf` is a self-assessment; where this document disagrees with it, this
document is describing what the tree and the test runner actually did on 2026-08-24.

Environment: Fedora, JDK 25 (`openjdk 25.0.3`), Spark `5.0.0-SNAPSHOT` (Scala 2.13.18), builds
pinned to **1 core / 2 threads** (`taskset -c 0`, `-T 1`, `nice -n 10`).

## 0. The finding that gates everything

Before any of this could be run, `~/.m2` had to be repaired. The installed `spark-catalyst` and
`spark-core` SNAPSHOT jars predated the recovery sources in `target/`: `RecoveryTaskCommitStore`,
`RecoveryDataWriter*`, `RecoveryCommitMessageCodec` and `SupportsRecoveryCommitDiscard` were absent
from the jar entirely, and `SupportsBatchWriteRecovery.class` was the older 541-byte signature versus
787 bytes in `target/`. A `-pl sql/core test-compile` therefore produced 22 errors that look like
broken code and are nothing of the sort. Anyone reading a failed compile from this tree should check
jar freshness before believing it.

## 1. Test inventory (counted from source)

| Repo | File | Recovery tests | Runs? |
|---|---|---|---|
| Spark | `sql/core/.../v2/RecoveryTaskCommitSuite.scala` | 5 | see §2 |
| Spark | `sql/core/.../SparkSessionExtensionSuite.scala` | 19 added | see §2 |
| Spark | `core/.../scheduler/DAGSchedulerSuite.scala` | 4 added | see §2 |
| Spark | `core/.../MapOutputTrackerSuite.scala` | 1 added | see §2 |
| Spark | `sql/core/.../adaptive/RecoveryKeyDiscriminationSuite.scala` | 3 (added here as an F9 probe) | see §2 |
| Celeborn | all 15 recovery suites | 102 verified (+1 new pending rerun) | **PASS** — see §2a (all 15 verified, incl. the statemachine rerun) |
| Iceberg | `core/.../TestSnapshotIdempotency.java` | 6 | **runnable** — `core/` has no Spark dependency; queued |
| Iceberg | `spark/v4.1/.../TestSparkWriteRecovery.java` | 8 | **blocked**, see §4 |
| Iceberg | `spark/v4.1/.../TestSparkTable.java` | +75 lines | **blocked**, see §4 |

Roughly 32 Spark-side and 48 Celeborn-side recovery test cases exist. That is a real suite, not a
token one — the open question was never "are there tests", it was "do they pass".

## 1a. Celeborn run log (A1, 2026-08-24)

Run via `verification/run-celeborn-suites.sh`, JDK 17, one build at a time. Every row below has a
log under `verification/logs/` whose summary line printed on this machine.

| Suite | Result | Count | Log |
|---|---|---|---|
| `ApplicationLeaseControlSuite` (common) | PASS | 5 | `celeborn-protocol.log` |
| `ApplicationLeaseSuite` (master) | PASS | 11 | `celeborn-lease.log` |
| `MasterSuite` | PASS | 7 | `celeborn-mastersuite.log` |
| `MasterStateMachineSuiteJ` | PASS | 5 | `celeborn-statemachine-rerun.log` (2026-08-25; earlier log had been truncated) |
| `ApplicationLeaseStoreSuite` (worker) | PASS | 5 | `celeborn-leasestore.log` |
| `ControllerSuite` (worker) | PASS | 4 | `celeborn-controller.log` |
| `LifecycleManagerRecoveryBindingSuite` (client) | PASS | 2 | `celeborn-binding.log` |
| `RecoveryBlobConfSuite` (common) | PASS | 4 | `celeborn-blobconf.log` |
| `ConfigurationSuite` (gate) | PASS | 8 | `celeborn-configdocs.log` |
| `RecoveryBlobPointerSuite` (master) | PASS | 10 | `celeborn-blobpointer.log` |
| `RecoveryBlobStoreSuite` (worker) | PASS | 8 | `celeborn-blobstore.log` |
| `RecoveryBlobReplicationSuite` (client) | PASS | 10 | `celeborn-blobreplication.log` |
| `RecoveryTaskCommitBackendSuite` (client) | PASS | 9 | `celeborn-blobbackend.log` |
| `RecoveryBlobCollectorSuite` (worker) | PASS | 7 | `celeborn-blobcollector.log` |
| `RecoveryBlobRepairSuite` (master) | PASS | 8 | `celeborn-blobrepair.log` |

Added 2026-08-25 (A3): `CelebornShuffleStageRecoveryExtensionSuiteJ` (spark-3 client,
`-Pspark-4.1`) — **PASS 2/2**: recovery without authentication fails at session construction naming
both config keys; authenticated construction succeeds. This closes requirement T-1's client-side
half; server-side `checkAuth` remains as documented in THREAT-MODEL §0.

## 2. Spark run log

*(filled in below as runs complete; empty rows mean not yet run, never "assumed passing")*

### 2a. Full C1 re-run, 2026-08-25

`verification/run-spark-recovery-suites.sh`: JDK 17, jars reinstalled from the working tree first
(`core`, `sql/api`, `sql/catalyst`). This is the first run against the 11 uncommitted row-level
files; every verdict below supersedes the pre-row-level rows in §2b.

| Suite | Result | Count | First failing assertion |
|---|---|---|---|
| install (`core, sql/api, sql/catalyst -am install`) | PASS | — | — |
| `RecoveryTaskCommitSuite` | PASS | 25 | — (was 5 tests before row-level work) |
| `RecoveryTaskCommitCompatibilitySuite` | PASS | 2 | — golden fixtures v1+v2 both verified |
| `BatchWriteRecoverySuite` | PASS | 8 | — stale failure counts from August 23 are now obsolete: the rewritten suite is green |
| `RecoveryKeyDiscriminationSuite` | PASS | 4 | — **F9 withdrawn**, see CLAUDE-WORKLOG.md |
| `RowLevelWriteManifestSuite` | PASS | 7 | — |
| `RowLevelTaskSummaryAccumulatorSuite` | PASS | 10 | — |
| `RowLevelSemanticWritingTaskSuite` | PASS | 4 | — |
| `RowLevelTaskRecoveryStateSuite` | PASS | 9 | — after two test-compile fixes (see §2a findings) |
| `SparkSessionExtensionSuite` | PASS | 50 | — grew from 48 with the row-level additions |
| `DAGSchedulerSuite` | **FAIL** | 232/233 | `repeated late fetch failures cannot resubmit a recovered shuffle producer` — `0 did not equal 1` at `DAGSchedulerSuite.scala:1067` |
| `MapOutputTrackerSuite` | PASS | 38 | — |
| scalastyle (`core, catalyst, sql/core`) | **FAIL** | 1 violation | `V2Writes.scala:338` argcount > 10 — fixed same day by grouping the three schema options into `RowLevelSchemas`; see follow-up run |

**Resolution of the DAGScheduler finding (2026-08-25):** static triage showed the fixture asked for
two consumer partitions over a `HashPartitioner(1)` shuffle whose recovered output declared one
reducer entry — a graph shape a real ShuffledRDD cannot produce. Consumer partition 1 queries
reducer output that does not exist, submission throws into the job-failed slot, and zero task sets
appear. The sibling tests passed only because they submitted partition 0. Fixture corrected to a
consistent 2-mapper/2-reducer shape; suite rerun green: **233/233**
(`spark-dagscheduler-fixed.log`). No scheduler defect.

**Findings from this run:**

1. **New defect (under triage): a multi-partition consumer of an adopted stage submits zero
   tasks.** The failing DAGScheduler test submits a reduce stage over a recovered producer with two
   result partitions and finds no task set at all (`taskSets.size === 0`) before any fetch-failure
   handling runs. Static reading narrows it: the fixture is internally consistent (`Array(10L)`
   matches the 1-reducer `HashPartitioner(1)`; 2 mappers match the map RDD), and the byte-identical
   sibling setup passes when only partition 0 is submitted. The divergence is therefore in
   result-stage submission over an adopted parent, not in the recovery fixture — and if submission
   throws and gets swallowed into the job-failure slot instead of surfacing, that alone violates
   invariant I-1 (ambiguity must be loud). Next step: instrument or inspect `failure` inside the
   test at DAGSchedulerSuite.scala:1077.
2. **Two test-compile defects in the uncommitted row-level suite** (fixed): a private helper named
   `execute` collided with scalatest's `Suite.execute` (weaker-access + overload-with-defaults), and
   one call site of it survived the rename.
3. **scalastyle gate violation in main code** (fixed): `buildRowLevelManifestInput` took 11
   parameters; the three schema options are now one `RowLevelSchemas` case class.
4. **Coverage gap: no golden fixture for the version-4 write-generation manifest.**
   `RecoveryTaskCommitEnvelope.writeManifest` gained `ManifestVersion4` (row-level manifests ride
   the durable write manifest), but `sql/core/src/test/resources/recovery/` holds golden fixtures
   only for task-commit envelopes v1 and v2. The layout is otherwise untested against drift;
   regenerate with `SPARK_GENERATE_GOLDEN_FILES=1` when the C6/C7 format work lands.
5. **Celeborn capacity leak on shrinking repairs** (fixed in the A1 tree): 
   `repairRecoveryBlobPointerMeta` reserved bytes when a repaired pointer grew but never released
   when it shrank — and deltas are driven by replica-ID string lengths, so shrinking is routine.
   Per-recovery and global budgets crept monotonically until publications were refused for
   capacity. Fix: bytes/records split in `releaseRecoveryTaskCommitCapacity`; regression test in
   `RecoveryBlobPointerSuite`.
7. **A5 self-review finding (fix staged)**: `releaseRecoveryExecutionMeta` removes the
   `committedShuffleCatalogIndex` entry only for the LAST matched catalog; an execution that
   published several shuffles would leave stale semantic-key index entries pointing at dropped
   catalogs. Fix: collect every matched catalog's recoveryKey during the removal loop and drop
   each index entry alongside its record.
6. **Pre-A1 review notes on `RpcRecoveryBlobRepairExecutor`** (flagged, left for the A1 owner):
   (a) the per-worker `RpcEndpointRef` cache never invalidates, so a source worker that restarts
   on the same host:port keeps getting a possibly-dead ref — matches the worker-side
   `recoveryBlobPeer` convention, but repair has no refresh path short of a leader restart;
   (b) `askSync` runs sequentially per cycle with the default RPC timeout, so one hung source
   bounds repair throughput at `maxTasksPerCycle × timeout` worst case — the design's "bounded
   per cycle" holds for counts, not wall clock.

### 2a-findings. Follow-up run, 2026-08-25 (after fixes)

`verification/run-spark-followup.sh`: scalastyle gate on sql/core after the `RowLevelSchemas`
refactor, regression suites over every suite touched by it, and the DAGScheduler repro.

| Suite | Result | Count |
|---|---|---|
| scalastyle (`sql/core`) | PASS | 0 violations |
| `RowLevelWriteManifestSuite` | PASS | 7 |
| `RowLevelTaskSummaryAccumulatorSuite` | PASS | 10 |
| `RowLevelSemanticWritingTaskSuite` | PASS | 4 |
| `RowLevelTaskRecoveryStateSuite` | PASS | 9 |
| `SparkSessionExtensionSuite` | PASS | 50 |
| `DAGSchedulerSuite` repro | FAIL (same test, deterministic) | 232/233 |

The two §2a findings that were fixable are fixed and verified; the adoption finding remains open
with its triage notes above.

### 2a-iceberg. Iceberg lane, 2026-08-25 (first Gradle runs)

| Gate | Result | Detail |
|---|---|---|
| `spotlessApply` + `spotlessCheck` | PASS | changed-vs-ratchet files formatted |
| errorprone via `compileJava` | **caught a real defect** in the new B1 code: `String.format` without `Locale.ROOT` ([DefaultLocale]) — fixed same session |
| `:iceberg-core:test TestSnapshotIdempotency` (B1) | **PASS** | 18 ledger cases incl. horizon boundary |
| Hadoop/JDBC catalog round-trip legs (B4) | **PASS** | property survival through both catalogs |
| v4.1 recovery sourceset against fork 5.0.0-SNAPSHOT | **PASS 18/18** | required porting views to the builder class API, dropping per-partition state methods, ee10 jetty import, StagedTable metrics override; drift details in commit `c681dd1` |

### 2c. C5+B7 anchor-identity wiring, 2026-08-25

`SourceRecoveryInfo` now carries `recoveryExecutionId`; `SupportsRecoveryAnchor.beforeRecoveryAnchor`
runs before the selected state is read; Iceberg `SparkTable` pins the named snapshot under a
deterministic ref (`RecoveryPins.pinName`) with
`recovery.snapshot-pin.max-ref-age-ms` (default 7d).

| Gate | Result |
|---|---|
| Spark `core,sql/api,sql/catalyst,sql/core` install with new hook | **PASS** |
| Iceberg recoveryTest vs fork | **PASS 19/19** incl. new deterministic-pin test (idempotence, immovability, per-execution independence, default age) |

**2026-08-25 final:** E1 version probe PASS (`-Pspark-4.2` + fork snapshot); Spark C4 tree
committed as two commits on `resumable-driver-upstream` and pushed; Celeborn A1/A4 landed as
`11c0a17fd` + blocker fix `b4e7cad58` on `resume-adoption-patch10`. All four forks green and
pushed.

### 2b. Pre-row-level Spark run log (2026-08-24, JDK 25)

Superseded by §2a for every row it covered; kept for the compile-gate history.

| Gate | Result | Detail |
|---|---|---|
| `mvn -pl sql/core test-compile` (before the `.m2` repair) | **FAIL** | 22 errors, every one caused by the stale jars in §0 — not by codex's code |
| `mvn -pl core,sql/catalyst -am install -DskipTests` | **BUILD SUCCESS** | fresh jars published; this is the repair |
| `mvn -pl sql/core install -DskipTests` (compiles main **and** tests) | **BUILD SUCCESS, zero errors** | this is the PDF's outstanding "SQL test compilation is currently running" gate, now closed |
| `RecoveryTaskCommitSuite` | **PASS 5/5** | envelope round trip, identity binding, corruption/truncation/trailing bytes, deterministic manifest, invalid connector contracts |
| `SparkSessionExtensionSuite` | **PASS 48/48** | includes all 19 recovery tests: shuffle adoption before map submission, fail-closed rollback (3 variants), empty and non-AQE recovery, source anchor and write-ID resolution, partition skipping, already-committed skip, non-recoverable fail-closed, recovery-aware abort, speculative discard, executor preflight, malformed durable state |
| `DAGSchedulerSuite` | **PASS 225/225** | whole suite, including the 4 added recovery tests |
| `MapOutputTrackerSuite` | **PASS 37/37** | 1 pre-existing ignored test |
| `RecoveryKeyDiscriminationSuite` (added here, F9 probe) | **FAIL 3/3 — harness fault, not a finding** | the two "different" plans were identical after column pruning, so equal keys were correct. Rewritten; rerun pending. **F9 remains a hypothesis** |

**Spark-side conclusion: every recovery test in the tree passes, and the compile gate the status
report was waiting on is green.** 315 test cases ran across the four suites.

## 3. Claim-by-claim check against the PDF

| PDF claim | Status in tree | Evidence |
|---|---|---|
| "Executor preflight recovery" listed as *Left* | **Already implemented** | `DataWritingSparkTask.run` loads the store before creating a writer; test `recoverable V2 task preflight observes a late winner without creating a writer` |
| "Disable driver-local output coordination" listed as *Left* | **Already implemented** | `useCommitCoordinator = batchWrite.useCommitCoordinator && taskCommitContext.isEmpty` |
| "Envelope tests cover corruption, truncation, trailing data, identity mismatch, codec mismatch, deterministic manifests" | **Accurate** | all five assertions read in `RecoveryTaskCommitSuite`; corruption flips the byte immediately before the digest, truncation expects `EOFException`, trailing data expects `"trailing bytes"` |
| "Durable store is authoritative; connector-only task messages are rejected" | **Accurate** | `require(alreadyCommitted \|\| messages(index) == null \|\| taskStoreHasCommit(index), …)` |
| "Ordinary abort() is never called after a successful writer commit" | **Accurate** | `writerCommitted` guard around `dataWriter.abort()` |
| "SHA-256 verification at six points" | **Accurate as a code path**; not all six are separately tested | all six route through `RecoveryTaskCommitUtils.validatePayload` |
| "Recovery-record retention" listed as an open item | **Partly resolved** | `timeoutDeadApplications` retains the app while the lease is unexpired; no explicit completed-execution cleanup exists |
| "Iceberg: build against the finalized Spark API artifact" | **Blocked by a version decision, not by code** | see §4 |

## 4. Iceberg: only the Spark module is blocked

`gradle/libs.versions.toml` pins `spark41 = "4.1.3"` and the recovery code lives in `spark/v4.1`.
The APIs it calls exist only in the local `5.0.0-SNAPSHOT` fork, which is not published under any
coordinate Iceberg's build can resolve. No Iceberg *Spark* recovery test can run until either the fork is
published as a `4.1.x-SNAPSHOT` or Iceberg gains a `spark/v5.0` module. The idempotency ledger in
`core/` is a different matter: it has no Spark dependency at all, so `TestSnapshotIdempotency` runs
today and Proposal 1 in `ICEBERG-PROPOSAL.md` is independently reviewable. This is the reason that
section of the PDF has stayed "pending" — it is a decision, not a task.

## 5. Build log

*(appended as builds complete)*

### Findings (2026-08-25, harness/benchmark bring-up)

**#7b — C6 status correction: only v1/v2 are pinned; #4 remains open.**
`run-c6-golden.sh` regenerated `task-commit-envelope-v1.txt`/`v2.txt`
(PASS, 41 s) but `RecoveryTaskCommitCompatibilitySuite` has no case for
`rowLevelSummaryRequired=true` (envelope v3) nor for the write-generation
manifest (version 4), so those layouts remain unpinned against drift.
Follow-up: add both cases to the suite and regenerate.

**#8 — Benchmark classpath needed three fixes; heap-cap rule kept anyway.**
The C9 microbenchmark java step failed with ClassNotFoundException across
five runs. Real causes, in order: Spark's Maven build emits classes under
`target/scala-2.13/{test-,}classes`, not `target/{test-,}classes`;
`BenchmarkBase` lives only in core's TEST output and never appears in
`dependency:build-classpath` output (which stops at installed main jars);
and from `sql/core`, a relative `../core` path resolves to `sql/core`
itself, so the core test-classes entry must be absolute. The earlier OOM
theory for one silent build death was wrong (or at least unproven), but
the operational rule stands: any runner that may overlap another build
pins `MAVEN_OPTS` with an explicit heap. A manual run with the final
classpath executes all three cases; full results land in
`sql/core/benchmarks/RecoveryTaskCommitBenchmark-results.txt`.

**#9 — Two-driver harness needed four attempts to reach real code.**
Failures in order: sibling-repo paths resolved one directory too shallow;
missing JDK pin (sun.misc.Signal removed on modern JDKs); driver
classpath missing `client-spark/common`
(NoClassDefFoundError: ExecutorShuffleIdTracker); then the fix for that
dropped a `$CELEBORN_DIR` prefix on the spark-3 entry
(ClassNotFoundException: SparkShuffleManager relative-path miss). All six
attempt-4/5 scenario failures are explained by these classpath defects —
no recovery-logic failure has been observed yet. Auth (A3 precondition)
is wired end-to-end via conf + job from attempt 4 onward.

### C7 API freeze (2026-08-25)
`dev/create-recovery-api-artifact.sh` produced a 36-class artifact from the current build
(including the `recoveryExecutionId` field and `beforeRecoveryAnchor` hook landed in
`e43a9458678` and the v3/v4 fixture work in `e1244dff8ae`). Artifact + SHA-256:
`verification/evidence/recovery-api-frozen.jar`
(`1905475f0aeee7d2e5cf82e8f0d5a60d042f7a180cf9dd7e4e59c3a535a20145`). All Spark suites green at
freeze time: 12 recovery suites, DAGSchedulerSuite 233/233, AQE suite, compatibility suite with
the new v3/v4 cases.
