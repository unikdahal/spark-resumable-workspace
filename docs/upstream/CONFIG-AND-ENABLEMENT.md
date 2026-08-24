# Enabling resumable execution: the complete configuration surface

Read out of the working tree on 2026-08-24. Every key below exists in source; defaults are the
literal defaults in the code.

## The one surprising fact

**Spark has no configuration key for this feature.** `grep -n recovery` over `SQLConf.scala` and
`core/.../internal/config/package.scala` returns only the unrelated
`spark.yarn.shuffle.server.recovery.disabled`. Recovery is enabled by *injecting a provider* through
`SparkSessionExtensions`, plus per-connector opt-in through interfaces. That has three consequences
worth stating up front:

1. There is no "flip a conf and it's on" path, and no conf to audit in a running cluster to answer
   "is this job resumable?" — you have to inspect the injected extensions.
2. Upstream review has no new public Spark conf surface to defend, which is a genuine simplification
   of the SPIP.
3. Every operational switch lives in the **provider** (Celeborn), not in Spark. If you need a kill
   switch, it is `spark.celeborn.driverRecovery.enabled=false`.

---

## 1. Spark side — injection, not configuration

```scala
// SparkSessionExtensions
def injectShuffleStageRecovery(builder: SparkSession => ShuffleStageRecovery): Unit
```

The provider trait (`org.apache.spark.sql.execution.adaptive.ShuffleStageRecovery`, `@DeveloperApi`
`@Experimental`) has five methods; only `tryRecover` is abstract:

| Method | Default | Contract |
|---|---|---|
| `tryRecover(info)` | abstract | called **synchronously before any map task is submitted**. `None` = authoritative "no committed recovery"; anything ambiguous must throw |
| `resolveSourceAnchor(info)` | throws `UnsupportedOperationException` | durably CAS the input version for this execution |
| `resolveWriteId(...)` (from `RecoveryAnchorResolver`) | — | durably CAS the sink write identity |
| `abortRecovery(info, cause)` | no-op | idempotent rollback of **driver-local** state only; must not delete the committed recovery |
| `onStageCompleted(info, result)` | no-op | may persist stats, but **must not be the only record** by which committed shuffle data is discoverable after a crash |

`ShuffleStageRecoveryInfo` gives the provider `stageId`, `shuffleId`, `numMappers`, `numPartitions`,
`plan`, `canonicalizedPlan`, `canonicalizedQueryPlan`. The doc comment is explicit that the recovery
key **should include `canonicalizedQueryPlan`**, so a changed query cannot recover only its
structurally unchanged stages and mix them with newly computed ones. `stageId`/`shuffleId` are
driver-local and must not enter the key.

Connector-side opt-in interfaces (all `@Evolving`, in `sql/catalyst`):

| Interface | Package | Meaning |
|---|---|---|
| `SupportsRecoveryAnchor` | `connector.catalog` | table exposes a stable source identity |
| `SupportsRecoveryWrite` | `connector.catalog` | table supports recoverable writes |
| `SupportsBatchWriteRecovery` | `connector.write` | the batch write itself is recoverable |
| `RecoveryDataWriterFactory` / `RecoveryDataWriter` | `connector.write` | writer contract required when recovery is active |
| `SupportsRecoveryCommitDiscard` | `connector.write` | writer can delete its own losing output |
| `RecoveryCommitMessageCodec` | `connector.write` | stable, non-executable commit encoding |
| `RecoveryTaskCommitStore` | `connector.write` | durable fenced CAS store |

A write that is `SupportsBatchWriteRecovery` but whose factory is not a `RecoveryDataWriterFactory`
fails **before** any task starts, as does a writer that cannot discard losing output.

---

## 2. Celeborn side — the operator-facing switches

### Spark-job properties (read by `CelebornShuffleStageRecoveryExtension`)

| Key | Default | Required | Meaning |
|---|---|---|---|
| `spark.celeborn.driverRecovery.enabled` | `false` | yes | master switch; the extension refuses to construct if false |
| `spark.celeborn.driverRecovery.id` | — | **yes, no default** | the stable recovery execution ID. Must identify **one immutable logical execution** and must never be reused for a later independent run |
| `spark.celeborn.client.application.uniqueId` | — | **yes, no default** | stable Celeborn application identity across driver replacement |
| `spark.celeborn.driverRecovery.leaseDuration` | `10m` | no | driver ownership lease; must be > 0 |
| `spark.celeborn.driverRecovery.probeTimeout` | `2s` | no | worker probe before adopted shuffle output is treated as readable; must be > 0 |

Plus the extension registration itself:

```
spark.sql.extensions=org.apache.spark.shuffle.celeborn.CelebornShuffleStageRecoveryExtension
```

The extension installs its provider through **reflection**
(`injectShuffleStageRecovery`, `Proxy.newProxyInstance` over the SPI interface) precisely so that an
ordinary Celeborn artifact stays binary-compatible with Spark releases that predate the SPI. On such
a release, enabling recovery fails **during session construction** with
`"Celeborn driver recovery requires a Spark build with ShuffleStageRecovery support"` — a loud, early
failure rather than a silent no-op. This is the right behaviour and worth preserving through review.

Lease renewal runs on a scheduled executor; a renewal failure is latched in an `AtomicReference` and
re-thrown on the next `tryRecover`/`onStageCompleted`, so a driver that has lost its lease cannot
keep adopting.

### Celeborn master configuration

| Key | Default | Category | What it bounds |
|---|---|---|---|
| `celeborn.master.applicationLease.maxDuration` | `24h` | master | longest lease the master will grant |
| `celeborn.master.recovery.taskCommit.maxPayloadSize` | `1m` | master | one task-commit payload at ingress |
| `celeborn.master.recovery.taskCommit.maxBatchResponseSize` | `16m` | master | serialized bytes in one batch response |
| `celeborn.master.recovery.taskCommit.maxInlineBytesPerRecovery` | `256m` | master | total inline bytes per recovery execution |
| `celeborn.master.recovery.taskCommit.maxInlineRecordsPerRecovery` | `200000` | master | total records per recovery execution |
| `celeborn.master.recovery.taskCommit.maxInlineBytesGlobal` | `512m` | master | total inline bytes cluster-wide |
| `celeborn.master.recovery.taskCommit.maxInlineRecordsGlobal` | `1000000` | master | total records cluster-wide |

All seven are declared `version("1.0.0")`, category `master`. They exist because task-commit payloads
are currently stored **inline in replicated master state** — they are the memory-safety envelope for
the whole feature, not tuning knobs. Raising them raises master heap and Raft snapshot size directly.

There is no client-side or worker-side Celeborn conf key for recovery: workers learn the lease
through `FenceApplication` rather than configuration.

---

## 3. Iceberg side — table properties

| Property | Default | Meaning |
|---|---|---|
| `commit.idempotency.entry.<hash>` | — | a ledger entry; written atomically with the snapshot. Not operator-set |
| `commit.idempotency.max-entries` | `10000` | live-entry cap. Reaching it produces **backpressure** (commit failure), never eviction of valid proof |
| `commit.idempotency.retention-ms` | `604800000` (7 days) | how long an entry stays live |

The API is `SnapshotUpdate.idempotencyKey(property, value)`, whose default implementation throws
`UnsupportedOperationException` — so support is per-implementation and discoverable, not assumed.

---

## 4. Minimum working configuration

```properties
# Spark job — recovery now refuses to construct without authentication
# (CelebornShuffleStageRecoveryExtensionSuiteJ pins this behaviour).
spark.sql.extensions                          org.apache.spark.shuffle.celeborn.CelebornShuffleStageRecoveryExtension
spark.celeborn.driverRecovery.enabled         true
spark.celeborn.auth.enabled                   true
spark.celeborn.driverRecovery.id              <stable-per-logical-execution-id>
spark.celeborn.client.application.uniqueId    <stable-per-application-id>
spark.celeborn.driverRecovery.leaseDuration   10m
spark.celeborn.driverRecovery.probeTimeout    2s
spark.shuffle.manager                         org.apache.spark.shuffle.celeborn.SparkShuffleManager
```

```properties
# Celeborn master (defaults shown; tune against RETENTION-AND-SIZING.md before raising)
celeborn.master.applicationLease.maxDuration                      24h
celeborn.master.recovery.taskCommit.maxPayloadSize                1m
celeborn.master.recovery.taskCommit.maxInlineBytesPerRecovery     256m
celeborn.master.recovery.taskCommit.maxInlineRecordsPerRecovery   200000
```

```sql
-- Iceberg table, only if the defaults do not match your recovery window
ALTER TABLE t SET TBLPROPERTIES (
  'commit.idempotency.retention-ms' = '604800000',
  'commit.idempotency.max-entries'  = '10000'
);
```

## 5. Where an operator gets this wrong

- **Reusing `driverRecovery.id` across two independent runs.** This is the one configuration mistake
  the design cannot defend against: the ID *is* the claim "these two processes are the same logical
  execution". Reuse it and the second run adopts the first run's committed output. Derive it from
  something immutable per logical execution (a workflow run ID), never from a wall clock or a
  hostname.
- **A `leaseDuration` longer than the master's `applicationLease.maxDuration`.** The master caps the
  grant; the driver believes it holds a longer lease than it does.
- **Raising the inline limits to "make it work" on a large write.** That moves task commit payloads
  into master heap and Raft snapshots. The correct fix is the worker-replicated blob backend, which
  is not implemented yet.
