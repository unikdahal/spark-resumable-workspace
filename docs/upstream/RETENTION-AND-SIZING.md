# Retention, sizing, and the TTL coupling

Read out of the working tree on 2026-08-24. This document answers two questions the code currently
answers only implicitly: **how long does recovery state survive**, and **how big can it get**.

## 1. The recovery window is the lease, not the heartbeat

The obvious reading of Celeborn's master is wrong, so state it explicitly.

`Master.timeoutDeadApplications()` runs every `celeborn.master.heartbeat.application.timeout / 2`
and would normally fire `handleApplicationLost` once an application stops heartbeating for
`celeborn.master.heartbeat.application.timeout` (**default 300s**). `handleAppLost` →
`AbstractMetaManager.updateAppLostMeta` **destroys all recovery state** for that app: committed
shuffle catalogs, the catalog index, source recovery anchors, and every task-commit record (with its
capacity accounting released).

A dead driver stops heartbeating. So without further logic the recovery window would be 5 minutes.
It is not, because of this guard:

```scala
val lease = statusSystem.applicationLeases.get(appId)
if (lease != null && currentTime < lease.expiresAtMs()) {
  logInfo(s"Application $appId heartbeat timed out, retaining it until its recovery lease expires…")
} else {
  handleApplicationLost(null, appId, requestId)
}
```

**The recovery window is therefore `max(app heartbeat timeout, remaining lease)`.** Consequences:

| If you want… | Set |
|---|---|
| a driver-restart budget of N | `spark.celeborn.driverRecovery.leaseDuration` > N (default `10m`) |
| leases that long to be grantable | `celeborn.master.applicationLease.maxDuration` ≥ that (default `24h`) |
| recovery state released promptly after a successful run | an explicit completed-execution cleanup — **not implemented**; today you wait for the lease to lapse |

The lease is also the fencing token, so lengthening it lengthens the window in which a partitioned
old driver's writes are rejected — that is the correct direction. The cost of a long lease is
retention: state for a *genuinely* dead application is held until expiry.

**Rule of thumb:** `leaseDuration ≈ 2 × (worst-case driver restart + scheduling delay)`, never less
than the orchestrator's own restart backoff. A lease shorter than the restart gap silently converts
a recoverable failure into a full recomputation, with no error anywhere.

## 2. Where the three TTLs must line up

Three independent retention settings govern one logical recovery:

| Layer | Setting | Default | Governs |
|---|---|---|---|
| Celeborn | `spark.celeborn.driverRecovery.leaseDuration` (capped by `celeborn.master.applicationLease.maxDuration`) | `10m` (cap `24h`) | shuffle catalogs, source anchors, task commits |
| Iceberg | `commit.idempotency.retention-ms` | `604800000` (7 days) | proof that the global commit already happened |
| Iceberg | snapshot expiry / `history.expire.*` | table-dependent | whether the *pinned source snapshot* still exists |

**Invariant R-1.** `iceberg commit.idempotency.retention-ms` **≥** the Celeborn recovery window.
Reason: the ledger's job is to let a replacement driver discover that the global commit already
succeeded. If the ledger entry expires first, a replacement driver that arrives inside the Celeborn
window sees committed task state but no commit proof. It fails closed rather than double-committing,
but the run is lost. With the defaults (7 days vs 10 minutes) this holds by a wide margin; it breaks
the moment someone raises the lease to `24h` **and** lowers the ledger horizon to hours.

**Invariant R-2.** The pinned source snapshot must outlive the recovery window. This one is
**currently violable**: nothing stops `expireSnapshots` from removing the snapshot a source anchor
pinned while the driver is down. Correctness holds (a replacement driver fails closed rather than
falling forward to a newer snapshot) but availability does not. The designed fix — a deterministic
recovery tag derived from `recoveryId + sourceId` with `max-ref-age-ms` longer than the recovery
window — is not implemented. Until it is, either keep the recovery window short relative to snapshot
expiry, or disable snapshot expiry for tables read by resumable jobs.

**Invariant R-3.** `commit.idempotency.max-entries` (default 10 000) is **backpressure, not
eviction**: exceeding it fails the commit rather than dropping a valid entry. A table written by many
concurrent resumable executions must therefore have
`max-entries` > (executions per retention window), or commits start failing. With the 7-day default,
10 000 entries allows ~59 commits/hour sustained on one table.

## 3. Sizing the inline task-commit store

Task commits are currently stored **inline in replicated master state** and appear in every Raft
snapshot. The stored record is `PbRecoveryTaskCommitRecord{appId, recoveryId, writeId, partitionId,
payload, sha256}` where `payload` is Spark's envelope, which is itself header + connector payload +
32-byte digest.

Per-record overhead, from the envelope layout:

```
28 bytes  fixed header fields (magic, version, partitionId, codecVersion, numRows, payloadLen)
+ |recoveryId| + 4
+ |codecId|   + 4          ("iceberg-data-files" = 18)
+ 32          payload digest
+ |payload|                 connector-encoded commit message
+ protobuf overhead: appId, recoveryId, writeId, 32-byte sha256, field tags
```

So budget roughly **|connector payload| + ~150 bytes + |identities| × 2** per partition.

For Iceberg (`codecId = "iceberg-data-files"`), the payload is the JSON representation of the data
files a task committed. One data file is on the order of 0.5–2 KiB of JSON depending on partition
values and column-level metrics; a task that produced several files scales linearly.

| Partitions | ~1 KiB/partition | ~4 KiB/partition |
|---|---|---|
| 10 000 | 10 MiB | 40 MiB |
| 100 000 | 100 MiB | 400 MiB ✗ |
| 1 000 000 | 1 GiB ✗ | 4 GiB ✗ |

against the defaults `maxInlineBytesPerRecovery = 256m` and `maxInlineRecordsPerRecovery = 200000`.
Read that table as the honest capability statement: **the inline backend is a ≤100K-partition
mechanism**, and only when commit messages stay small. `maxInlineRecordsPerRecovery = 200000` is the
hard ceiling on partitions per recoverable write regardless of size.

Cluster-wide, `maxInlineBytesGlobal = 512m` / `maxInlineRecordsGlobal = 1000000` means roughly **two
concurrent large recoverable writes**, not twenty. Capacity is reserved with `Math.addExact` before
insertion and released on failure or app-lost, so the accounting itself does not leak. Since
2026-08-25 there is also **per-application fairness** (`maxInlineBytesPerApp` /
`maxInlineRecordsPerApp`, default one full recovery each): a selfish application now hits its own
quota - and the rejection names that bound - instead of starving the cluster (T-14 mitigated).

**Do not raise these limits to make a big job work.** They bound master heap and Raft snapshot size
directly. For large payloads, enable the **worker-replicated blob backend**
(`celeborn.master.recovery.blob.enabled=true`, implemented): payloads up to 64 MiB live on workers
behind a durability quorum, Raft holds only a ~200-byte pointer per partition, and the binding
constraint moves from master memory to worker disk (`replicationFactor × payload`). Small commits
(≤ 4 KiB) stay inline regardless, so the inline table above still governs their aggregate.

Measured microbenchmarks (`RecoveryTaskCommitBenchmark`, JDK 17, i7-1165G7, one core;
regenerate with `verification/run-c9-benchmark.sh`) settle the CPU question:

| payload | wire/partition | partitions | encode | decode | batched decode |
|---|---|---|---|---|---|
| 512 B | 578 B | 1 000 | ~1.1 us | ~1.0 us | ~1.0 us |
| 512 B | 578 B | 100 000 | ~2.5 us | ~2.6 us | - |
| 4 KiB | 4 162 B | 10 000 | ~12 us | ~12 us | ~11 us |
| 4 KiB | 4 162 B | 100 000 | ~17 us | ~12 us | ~11 us |

Per-partition commit cost is microseconds - negligible next to the network round trip every task
commit already pays. The number that matters for adoption is the driver-side reload: worst case
measured, 100 000 partitions x 4 KiB decodes in **~1.1 s** with the batched path, comfortably
inside any restart budget, and batching wins over per-record decoding by ~1.6x at that scale.
Write-manifest build plus SHA-256 digest is ~3 ms. CPU therefore never binds before memory does,
and the inline table above remains the honest constraint.

## 4. Payload-size mismatch between the layers

| Layer | Cap | Constant |
|---|---|---|
| Spark envelope payload | 16 MiB | `RecoveryTaskCommitEnvelope.MaxPayloadBytes` |
| Spark compatibility metadata | 1 MiB | `MaxCompatibilityMetadataBytes` |
| Celeborn ingress payload | 1 MiB (default) | `celeborn.master.recovery.taskCommit.maxPayloadSize` |
| Celeborn batch response | 16 MiB (default) | `…maxBatchResponseSize` |

A connector payload between 1 MiB and 16 MiB passes every Spark-side check and fails at `publish` —
**after** the task has written its data and committed to the connector. That failure is correct
(fail closed) but maximally expensive. Two mitigations, neither implemented:

1. carry the store's payload limit in the manifest handshake, so an oversized configuration fails
   before any task starts; or
2. align Spark's `MaxPayloadBytes` with the store's advertised limit at context-construction time.

Practical guidance today: keep `maxPayloadSize` ≥ the largest commit message the connector can
produce for one partition, and remember that a batch `load` of 1024 partitions must fit inside
`maxBatchResponseSize` — 1024 × 16 KiB already exceeds the 16 MiB default. Spark's batch size is a
hard-coded `MaxLoadBatch = 1024`, so with commit messages above ~16 KiB, `maxBatchResponseSize` must
be raised or the batch read fails.

## 5. What is not implemented

- Explicit completed-execution cleanup. Recovery state for a *successful* run is held until the
  lease lapses; there is no "this execution is done, release it now" call.
- `applicationLeases` entries are **not** removed by `updateAppLostMeta` (every other recovery map
  is). Keeping the epoch is defensible — it preserves monotonicity for a later incarnation of the
  same `appId` — but the map then grows without bound across application lifetimes and is included
  in Raft snapshots. Either document it as intentional with a bound, or add expiry-plus-tombstone.
- Worker-replicated blob storage for large payloads (§3).
- Iceberg recovery tags for source-snapshot pinning (R-2).

---

## Appendix: measuring this instead of estimating it

The §3 table is arithmetic, not measurement. `verification/run-scale-benchmark.sh` produces the real
numbers from the actual envelope code and a real CAS store:

```
partitions  payloadBytes  envelopeBytes  totalDurableMiB  encodeMsPerK  decodeMsPerK  storeWriteMsPerK  batchLoadMs  manifestBytes
```

`envelopeBytes` is the true per-partition durable cost (connector payload + Spark header + 32-byte
digest, before Celeborn's protobuf framing), `totalDurableMiB` is what a recovery of that width would
occupy in replicated master state, and `batchLoadMs` is what every replacement driver pays before it
can schedule anything — the bounded 1024-partition batched load, including decode. Run it before
quoting §3 in a proposal; measured numbers are the ones a reviewer will ask for.


## Mapping to the integration contract

`RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md` §9 states the TTL ordering as a contract neither owner may
change alone:

> Iceberg source pin and global idempotency proof live longer than Celeborn recoverable task state,
> with a documented safety margin.

That is invariants R-1 and R-2 above, with one gap and one number attached:

| Contract clause | State here |
|---|---|
| Iceberg global idempotency proof outlives Celeborn task state | holds by default (7 days vs a 10m lease); breaks if the lease is raised toward its 24h cap while the ledger horizon is lowered |
| Iceberg **source pin** outlives Celeborn task state | **not implementable yet** — no recovery tag exists, so the pinned snapshot has no protected lifetime at all (work package C5) |
| Documented safety margin | proposed: ledger horizon >= 4x the maximum grantable lease, i.e. >= 96h when `applicationLease.maxDuration` is left at 24h |

Until C5 lands, the honest operational statement is: **the recovery window is bounded by the
lease, and source-snapshot availability is bounded by whatever the table's expiry policy happens to
be.** The second bound is not under the protocol's control.
