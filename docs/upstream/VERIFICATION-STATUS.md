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
| Celeborn | `master/.../clustermeta/ApplicationLeaseSuite.scala` | 11 | not run |
| Celeborn | `master/.../ha/MasterStateMachineSuiteJ.java` | 10 | not run |
| Celeborn | `master/.../MasterSuite.scala` | 7 | not run |
| Celeborn | `common/.../protocol/ApplicationLeaseControlSuite.scala` | 5 | not run |
| Celeborn | `worker/.../ApplicationLeaseStoreSuite.scala` | 5 | not run |
| Celeborn | `worker/.../ControllerSuite.scala` | 4 | not run |
| Celeborn | `tests/spark-it/.../LifecycleManagerSuite.scala` | 4 | not run |
| Celeborn | `client/.../LifecycleManagerRecoveryBindingSuite.scala` | 2 | not run |
| Iceberg | `core/.../TestSnapshotIdempotency.java` | 6 | **runnable** — `core/` has no Spark dependency; queued |
| Iceberg | `spark/v4.1/.../TestSparkWriteRecovery.java` | 8 | **blocked**, see §4 |
| Iceberg | `spark/v4.1/.../TestSparkTable.java` | +75 lines | **blocked**, see §4 |

Roughly 32 Spark-side and 48 Celeborn-side recovery test cases exist. That is a real suite, not a
token one — the open question was never "are there tests", it was "do they pass".

## 2. Spark run log

*(filled in below as runs complete; empty rows mean not yet run, never "assumed passing")*

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
