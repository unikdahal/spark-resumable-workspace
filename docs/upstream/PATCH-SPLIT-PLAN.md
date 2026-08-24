# Splitting the work into reviewable upstream patches

Three Apache projects, three separate review processes, three separate votes. The current tree is one
logical change spread across 25 modified + 14 new files in Spark, 25 + 9 in Celeborn, and 6 + 5 in
Iceberg. Nobody will review that as a unit, and the dependency order matters: **Spark's API must land
first, because both other patches compile against it.**

## The dependency spine

```
 Spark PR-1 (SPI + write API, no implementation)
        │
        ├── Spark PR-2 (scheduler integration)      ── independent of Celeborn/Iceberg
        ├── Spark PR-3 (V2 write execution)
        │        │
        │        ├── Celeborn CIP (needs PR-1 released or a snapshot)
        │        └── Iceberg proposal (needs PR-1 released; see the version decision below)
```

Nothing downstream can be merged before Spark PR-1 is in a release, so PR-1's API shape is the
critical path for the whole project. That argues for making PR-1 as small and as defensible as
possible, and for not attaching any connector implementation to it.

## Spark

**PR-1 — recovery SPI and connector API only.** Interfaces, no behaviour change, no scheduler edits.
Reviewable as "new opt-in API, dead code until something injects a provider".

| File | Note |
|---|---|
| `core/.../shuffle/ShuffleStageRecovery.scala` | `ShuffleStageRecoveryHandler`, `RecoveredShuffleOutput` |
| `sql/catalyst/.../connector/catalog/SupportsRecoveryAnchor.java`, `SupportsRecoveryWrite.java` | |
| `sql/catalyst/.../connector/write/{SupportsBatchWriteRecovery,BatchWriteRecoveryState,RecoveryCommitMessageCodec,RecoveryDataWriter,RecoveryDataWriterFactory,SupportsRecoveryCommitDiscard}.java` | 6 interfaces |
| `sql/catalyst/.../connector/recovery/RecoveryTaskCommitStore.java` | moved into its own `o.a.s.sql.connector.recovery` package (commit `ace8a2de502`) — decide before review whether the remaining recovery types follow it or the move is reverted to keep PR-1 one-package |
| `sql/catalyst/.../analysis/RecoveryAnchorResolver.scala` | trait + info case classes |
| `sql/core/.../adaptive/ShuffleStageRecovery.scala` | SQL-level SPI |
| `sql/core/.../SparkSessionExtensions.scala` | `injectShuffleStageRecovery` |
| `LogicalWriteInfo.java`, `LogicalWriteInfoImpl.scala` | `isRecoveryEnabled` |
| `sql/core/.../RecoveryTaskCommit.scala` + `RecoveryTaskCommitSuite.scala` | envelope + its 5 tests |

Review questions to expect: why `@DeveloperApi @Experimental` rather than `@Evolving`; why the
envelope is Spark-owned rather than connector-owned; why `formatVersion` is checked for exact
equality (see `UPGRADE-AND-ROLLBACK.md` §2 — decide before submitting).

**PR-2 — scheduler integration.** `DAGScheduler`, `MapOutputTracker`, `ShuffleMapStage`,
`Dependency`, `ShuffleExchangeExec`, `AdaptiveSparkPlanExec`, `QueryStageExec`, `AdaptiveRulesHolder`,
plus `DAGSchedulerSuite`/`MapOutputTrackerSuite` additions and the shuffle-recovery half of
`SparkSessionExtensionSuite`. Self-contained: with no provider injected, every path is inert.

The load-bearing behaviours to call out in the description: recovery is consulted **before** map
tasks are submitted; a fetch failure on a recovered shuffle aborts instead of recomputing
(`mapStage.isRecovered`); failed adoption rolls back only driver-local state.

**PR-3 — V2 write execution.** `WriteToDataSourceV2Exec`, `V2Writes`, `DataSourceV2Utils`,
`RelationResolution`, `Analyzer`, `SparkOptimizer`, `BaseSessionStateBuilder`, `SessionState`,
`HiveSessionStateBuilder`, and the write half of `SparkSessionExtensionSuite`.

This is the contentious one: it disables the `OutputCommitCoordinator` for recovery writes and adds
an executor-side preflight `load`. Both need their own paragraph in the PR description, because both
look like regressions to a reviewer who has not read the failure analysis. `RETENTION-AND-SIZING.md`
§4 and `PROTOCOL-SPEC.md` §6 are the supporting material.

**Not in any PR yet:** row-level operations (fail closed today), streaming (out of scope by
construction), transactional writes.

## Celeborn

**CIP-1 — application lease and fencing.** `ApplicationLease`, lease RPCs (97–100), master lease CAS,
worker `ApplicationLeaseStore` + `FenceApplication` enforcement, `celeborn.master.applicationLease.maxDuration`,
and the app-lost retention guard in `timeoutDeadApplications`. Useful on its own: a durable driver
ownership lease is a contribution independent of Spark recovery, and it is the smallest piece that
can be argued for in isolation.

**CIP-2 — committed shuffle catalog.** Catalog publication/lookup RPCs (101–104), the replicated map,
snapshot/HA replay, the semantic `recoveryKey` index and its cleanup. Depends on CIP-1 for fencing.

**CIP-3 — recovery bindings and task-commit CAS.** Source-anchor and write-ID CAS (105–106),
task-commit CAS (107–112), `RecoveryTaskCommitUtils`, the seven inline limits, aggregate accounting.
Depends on CIP-1.

**CIP-4 — recovery blob backend.** Blob transport RPCs (113–122), the `recoveryBlobPointers`
replicated map and its Raft commands (`PublishRecoveryBlobPointer`, `RepairRecoveryBlobPointer`),
worker `RecoveryBlobStore`/`RecoveryBlobCollector`, the leader-side repair planner/cycle/executor,
the `celeborn.master.recovery.blob.*` configuration and the client replication driver. Depends on
CIP-3 (it replaces that CIP's inline storage for large payloads; the inline store remains for small
payloads and as a read fallback). Wire-level contract: `PROTOCOL-SPEC.md` §5.

**CIP-5 — Spark integration.** `CelebornShuffleStageRecoveryExtension` and the `LifecycleManager`
client surface. Depends on Spark PR-1 being released, and on the Spark-version decision below.

Reviewers will ask about the inline-payload storage in Raft. Lead with the limits and the sizing
table from `RETENTION-AND-SIZING.md` §3, then point at CIP-4: large payloads no longer ride in Raft
at all — the pointer (~200 bytes) does, and blobs are quorum-durable before any pointer exists.

## Iceberg

**Proposal 1 — `SnapshotUpdate.idempotencyKey` + the ledger.** `api/SnapshotUpdate.java`,
`core/SnapshotProducer.java`, `core/TableProperties.java`, `TestSnapshotIdempotency`. **No Spark
dependency at all** — it is reviewable, testable and mergeable today, independently of everything
else. This is the single best first contribution of the whole project: it is small, it stands on its
own merits (idempotent commits are useful without any of this), and it needs no version negotiation.

**Proposal 2 — recoverable Spark writes.** `SparkWriteRecovery*`, `SparkWrite`, `SparkTable`,
`TestSparkWriteRecovery`. Blocked on the version decision.

## The Spark-version decision that blocks two of the three projects

The fork is `5.0.0-SNAPSHOT`. Iceberg's Spark module pins `spark41 = "4.1.3"`; Celeborn's newest
profile is `spark-4.2` with `spark.version = 4.2.0`. Neither can compile against a `5.0.0-SNAPSHOT`
that exists only on one machine. Options:

1. **Target the Spark API at 4.2/4.1 and backport.** Lets Celeborn and Iceberg build against a
   released Spark, at the cost of maintaining the API on a maintenance branch.
2. **Publish the fork as a private snapshot** (`4.2.0-recovery-SNAPSHOT`) purely for integration
   testing. Fastest path to a real two-driver E2E; not a merge path.
3. **Wait for Spark PR-1 to land in 5.0 and develop connectors against nightlies.** Cleanest, slowest.

Recommendation: **2 for testing now, 3 for the actual upstream path**, and be explicit in each PR
description about which Spark version the API is proposed for. Option 1 only if a specific downstream
consumer needs it on 4.x.

## Order of operations

1. Iceberg Proposal 1 (independent, mergeable now).
2. Spark PR-1 (critical path; everything waits on the API shape).
3. Celeborn CIP-1 (independent value; can be reviewed in parallel with Spark PR-1).
4. Spark PR-2, PR-3.
5. Celeborn CIP-2, CIP-3.
6. Celeborn CIP-4 (blob backend; after CIP-3).
7. Celeborn CIP-5, Iceberg Proposal 2 — after the Spark API is available in a release.

## Session deltas awaiting commits (2026-08-25 plan, not yet executed)

How the working trees map onto that spine once the owners decide to land things. Nothing here is
committed yet except where noted.

### Celeborn (`resume-adoption-patch10`)

| Commit | State | Contents |
|---|---|---|
| `b80ba5f72` fix: refuse to install driver recovery without authentication | **committed** | extension precondition + `CelebornShuffleStageRecoveryExtensionSuiteJ`. Stands alone: touches only CIP-5 files, no overlap with in-flight blob work |
| A1: land the in-flight repair work | pending owner decision | the 11 uncommitted replicate/repair files + `RpcRecoveryBlobRepairExecutor.scala`; gates = 15-suite script |
| A5/A6/A2… | not started | blocked on A1 landing |

### Spark (`spark-resumable-upstream`, all uncommitted)

One commit is impossible to separate cleanly from another agent's row-level work — my fixes are
inside the same files. Planned split when it lands:

1. **Row-level and transaction recovery** (C4): the 11 modified/new files incl. `V2Writes`
   (with the `RowLevelSchemas` argcount fix folded in) and the test-compile fixes to
   `RowLevelTaskRecoveryStateSuite`. Gate: §2a table green.
2. **DAGSchedulerSuite fixture correction**: consumer partition count must match the partitioner
   (`HashPartitioner(2)` + matching recovered output). Gate: suite green after fix.
3. Design docs live in the workspace repo, not the fork.

### Iceberg (`oss-fixes/iceberg`, all uncommitted)

1. **Core: ledger horizon vs declared recovery window** — `SnapshotProducer`, `TableProperties`,
   `TestSnapshotIdempotency` (B1). Independent, mergeable now.
2. **Test: catalog round-trip matrix for ledger properties** — `CatalogTests`, `TestHadoopCatalog`
   (B4). Rides after 1 only because it asserts against the same property names.
3. **Spark/v4.1: drift and losing-writer recovery tests** — `TestSparkWriteRecovery` (B2+B3).
   Blocked on the version decision actually working end to end (next item).
4. **Build: point spark41 at the fork snapshot** — `gradle/libs.versions.toml` (B6 option 2,
   testing-only). Must carry a revert path note: stock 4.1.3 returns once PR-1 ships.

Each commit message follows rule 7 of `TODO.md`: what changed, why the alternative was rejected,
`Co-Authored-By` and `Claude-Session` trailers.
