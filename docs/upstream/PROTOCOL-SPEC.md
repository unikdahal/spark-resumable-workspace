# Resumable execution: the cross-project protocol

**Status:** derived by reading the working tree on 2026-08-24. Every byte layout, field name, limit
and ordering rule below was read out of source in `spark-resumable-upstream`, `celeborn`, or
`oss-fixes/iceberg`. Anything not confirmed that way is marked `[unverified]`.
Nothing here is a proposal — it documents what the code currently implements, so that the three
patch sets can be reviewed against one contract instead of three separate readings.

Trees this was read from:

| Repo | Path | Base |
|---|---|---|
| Spark | `spark-resumable-upstream` | `5.0.0-SNAPSHOT` (master), 23 files modified + 14 new |
| Celeborn | `celeborn` | `main` @ `00113daf` + `fb9ccfa93`, 25 files modified + 9 new |
| Iceberg | `oss-fixes/iceberg` | `6976e02`, Spark 4.1 module, 6 files modified + 5 new |

---

## 1. The three roles

The protocol has exactly three roles, and each is owned by a different project:

- **Spark** owns *execution identity and arbitration policy*. It decides what a stage/write is, what
  a task commit means, and when output may be adopted instead of recomputed. It owns the envelope
  format and refuses to let anything else interpret it.
- **Celeborn** owns *durability and fencing*. It holds the shuffle bytes, the immutable CAS records,
  the driver lease, and the replicated (Raft) state machine that makes "first writer wins" a fact
  rather than a convention.
- **The connector (Iceberg)** owns *sink semantics*. It encodes its own commit messages with a
  stable codec, declares compatibility metadata that binds everything that could change the meaning
  of a task commit, and makes its global commit idempotent under a caller-supplied key.

The seam between them is deliberately narrow: **Spark never parses a connector payload; Celeborn
never parses a Spark envelope.** Both are opaque, length-bounded, digest-checked byte strings.

```
 executor ── envelope(bytes) ──► RecoveryTaskCommitStore (Spark API)
                                         │  implemented by
                                         ▼
                              Celeborn LifecycleManager ── PbPublishRecoveryTaskCommit ──► Master/Raft
                                                                                            (immutable CAS)
 driver   ── recover() ──► connector (Iceberg) ── codec ──► WriterCommitMessage
```

---

## 2. Identities

| Identity | Owner | Meaning | Where validated |
|---|---|---|---|
| `recoveryId` | Spark | Stable per logical execution. Defaults to `LogicalWriteInfo#queryId()`. Never a driver-local ID. | `RecoveryTaskCommitEnvelope.checkedString`, `RecoveryTaskCommitUtils.validateIdentityPart` |
| `sourceId` | connector | Stable identity of a read source (table/catalog identity, not a snapshot). | `LifecycleManager.sourceRecoveryBindingId` |
| `writeId` | connector/Spark | Stable identity of a sink write, CAS-resolved so two drivers converge. | `LifecycleManager.writeRecoveryBindingId` |
| `partitionId` | Spark | Logical output partition. `-1` is reserved for the write manifest. | `RecoveryTaskCommitUtils.validateIdentity` |
| `appId` | Celeborn | Application recovery identity (stable across driver replacement). | Celeborn ingress |

**Explicitly not identities:** local shuffle ID, stage ID, RDD ID, task attempt ID. The Spark SPI
comment states this outright — a replacement driver allocates all of them again, so they carry no
recovery meaning (`core/.../shuffle/ShuffleStageRecovery.scala`).

### Identity limits (hard protocol bounds, not config)

| Bound | Value | Source |
|---|---|---|
| Spark identity string | ≤ 64 KiB UTF-8, non-empty | `RecoveryTaskCommitEnvelope.MaxIdentityBytes` |
| Celeborn identity string | ≤ 1024 UTF-8 bytes, non-empty, strict UTF-8 (`REPORT` on malformed/unmappable) | `RecoveryTaskCommitUtils.MAX_IDENTITY_UTF8_BYTES` |
| Iceberg idempotency property/value | ≤ 4096 UTF-8 bytes each | `SnapshotProducer.MAX_IDEMPOTENCY_FIELD_BYTES` |

Celeborn's bound is the binding one in a real deployment — it is deliberately a *protocol constant*
rather than a config key, so a client and a metadata replica can never disagree about what is
storable.

### Key construction

Celeborn stores every recovery record in one replicated map per kind, so keys must be injective
across namespaces:

```java
// AbstractMetaManager.recoveryTaskCommitKey
appId.length()     + ":" + appId
  + recoveryId.length() + ":" + recoveryId
  + writeId.length()    + ":" + writeId
  + partitionId
```

Length-delimiting makes the encoding prefix-free, so no combination of identities can alias another.
Source and write bindings share one CAS primitive but disjoint namespaces:

```scala
sourceRecoveryBindingId(sourceId) = s"source:v1:${sourceId.length}:$sourceId"
writeRecoveryBindingId(sinkId)    = s"write:v1:${sinkId.length}:$sinkId"
```

The `v1` segment is the namespace version — the one place a future incompatible identity scheme can
be introduced without colliding with records already in a running cluster's Raft log.

---

## 3. The Spark task-commit envelope (format version 1)

Spark owns this format. Celeborn stores it as opaque `bytes`; the connector never sees it.
Layout, from `RecoveryTaskCommitEnvelope.encode` (`sql/core/.../v2/RecoveryTaskCommit.scala`),
all integers big-endian via `DataOutputStream`:

```
int32   magic            = 0x53525443   ("SRTC")
int32   formatVersion    = 1
int32   recoveryIdLen ; bytes recoveryId (UTF-8)
int32   partitionId      (>= 0)
int32   codecIdLen    ; bytes codecId (UTF-8)
int32   codecVersion     (> 0)
int64   numRows          (>= 0)
int32   payloadLen       (0 .. 16 MiB)
bytes   payload          (connector-encoded WriterCommitMessage)
bytes   sha256(payload)  (32 bytes)
```

**Decode rejects, in order:** wrong magic; unsupported format version; `recoveryId` mismatch;
`partitionId` mismatch; `codecId` mismatch; non-positive codec version; negative row count; payload
length out of range; short read (truncation); **any trailing byte**; digest mismatch; a codec that
decodes to null. Every one of those is a hard failure — there is no lenient path.

Note the digest covers the **payload only**, not the header. Header integrity is enforced by
identity equality instead (the reader already knows the `recoveryId`, `partitionId` and `codecId` it
expects, and compares them). A corrupted header therefore fails as an identity mismatch rather than
a checksum mismatch. Both fail closed; the error message differs.

### Write manifest (`partitionId = -1`)

Published before any writer is created, via `RecoveryTaskCommitStore.resolveWriteManifest`:

```
int32   magic         = 0x53525443
int32   formatVersion = 1
int32   recoveryIdLen ; bytes recoveryId
int32   numPartitions            (physical partition count)
int32   codecIdLen ; bytes codecId
int32   codecVersion
int32   compatMetadataLen (0 .. 1 MiB) ; bytes compatibilityMetadata
bytes   sha256(all preceding bytes)     (32 bytes)
```

Unlike the task envelope, the manifest digest covers the **whole record**, because there is no
identity round-trip to compare against — the caller compares the proposed manifest with the CAS
winner byte-for-byte (`MessageDigest.isEqual(proposedManifest, resolvedManifest)`), and a difference
aborts the write before any task starts. This is the mechanism that stops a replacement driver with
a different partition count, codec, schema, partition spec or overwrite mode from adopting the first
driver's task commits.

### Size limits

| Limit | Value | Source |
|---|---|---|
| Task payload | 16 MiB | `MaxPayloadBytes` (Spark) |
| Compatibility metadata | 1 MiB | `MaxCompatibilityMetadataBytes` (Spark) |
| Celeborn inline payload | 1 MiB default | `celeborn.master.recovery.taskCommit.maxPayloadSize` |

**Gap:** Spark permits a 16 MiB payload; Celeborn's default ingress accepts 1 MiB. A connector whose
commit message exceeds the Celeborn limit fails at publish time, i.e. *after* the writer has already
written its data. See `RETENTION-AND-SIZING.md` for the sizing rule this implies.

---

## 4. `RecoveryTaskCommitStore` — the CAS contract

```java
byte[] resolveWriteManifest(String recoveryId, byte[] proposedValue);
byte[][] load(String recoveryId, int[] partitionIds);
byte[] publish(String recoveryId, int partitionId, long taskAttemptId, int attemptNumber, byte[] value);
```

Required semantics (from the interface javadoc, and relied on by the execution path):

1. **Immutable first-writer-wins per `(recoveryId, partitionId)`.** `publish` returns `value`
   byte-for-byte when this attempt wins, or the previously published value when another won. It
   **never** overwrites.
2. **`load` is order- and length-preserving.** The returned array has the same length and order as
   `partitionIds`; `null` means an *authoritative* absence, not "unknown".
3. **Ambiguity is a failure, never a null.** Lease loss, fencing, corruption and unavailability must
   throw. Spark treats `null` strictly as "absent" and would otherwise recompute committed work.
4. **Bytes are preserved exactly.** The store must not re-encode, pad, or normalise.

Spark calls `load` in bounded batches of **1024** partitions (`MaxLoadBatch` in
`WriteToDataSourceV2Exec`), which is what makes the Celeborn batch RPC worth having.

---

## 5. Celeborn wire protocol

New message types (`common/src/main/proto/TransportMessages.proto`), IDs 97–122 (97–112 below; the
recovery-blob messages 113–122 are specified in the blob section):

| ID | Message | Purpose |
|---|---|---|
| 97/98 | `ApplicationLeaseControl` | acquire / renew / take over the driver lease |
| 99/100 | `FenceApplication` | master → worker, install the accepted lease |
| 101/102 | `PublishCommittedShuffleCatalog` | publish an immutable committed-shuffle catalog |
| 103/104 | `GetCommittedShuffleCatalog` | look up by `shuffleId` **or** by `recoveryKey` |
| 105/106 | `ResolveSourceRecoveryAnchor` | CAS a source anchor or a write ID |
| 107/108 | `PublishRecoveryTaskCommit` | CAS one task-commit envelope |
| 109/110 | `GetRecoveryTaskCommit` | read one |
| 111/112 | `BatchGetRecoveryTaskCommits` | read many, in request order |

Every request carries `applicationLeaseEpoch` + `applicationLeaseOwnerId`. Replicated state
(`Resource.proto`) gains five maps: `applicationLeases`, `applicationWorkers`,
`committedShuffleCatalogs`, `sourceRecoveryAnchors`, `recoveryTaskCommits`, and — with the blob
backend — `recoveryBlobPointers`.

### Lease and fencing

`PbApplicationLease{epoch, ownerId, expiresAtMs}` is the fencing token. Rules read from the proto and
`Master.scala`:

- Epochs are monotonically increasing per `appId`. A replacement driver must acquire a **strictly
  higher** epoch than the driver it replaces.
- `leaseDurationMs` is preferred over `expiresAtMs` on both the master and worker paths, so the
  expiry is always derived in the receiver's own clock domain. Absolute timestamps across clock
  domains are accepted only for compatibility.
- `celeborn.master.applicationLease.maxDuration` (default `24h`) caps what the master will grant.
- Workers persist the accepted lease (`ApplicationLeaseStore`) and enforce it, so a partitioned old
  driver cannot keep writing through a worker that never heard about the takeover.
- The `LifecycleManager` injects the lease itself; an executor cannot claim an arbitrary epoch.

### Committed shuffle catalog

`PbCommittedShuffleCatalog{appId, shuffleId, numMappers, numPartitions, mapperAttempts, fileGroups,
appShuffleId, recoveryKey}`. Published **only after `StageEnd` has committed every worker file**;
thereafter immutable — an exact byte-for-byte replay is idempotent, any differing replacement is
rejected. `recoveryKey` is the semantic key that lets a replacement driver find the physical shuffle
even though its own `shuffleId` allocation differs, and it is registered *before* the first map task
is submitted, so a driver that dies before its post-stage callback still leaves a discoverable
record.

`GetCommittedShuffleCatalogResponse` carries **both** `found` and `success` — this separates "the
catalog authoritatively does not exist" (recompute is correct) from "the lookup failed" (fail
closed). A single boolean here would be a correctness bug; the split is deliberate.

### Task commit records

`PbRecoveryTaskCommitRecord{appId, recoveryId, writeId, partitionId, payload, sha256}`, stored and
returned byte-for-byte. The digest is verified at **six** points: executor client, LifecycleManager,
master ingress, Raft state-machine apply, snapshot restore, and every read. `[verified in source for
ingress/apply/read; the executor-client and snapshot-restore checks are present in
`RecoveryTaskCommitUtils` callers — see `VERIFICATION-STATUS.md` for which of these a test exercises]`

`BatchGetRecoveryTaskCommitsResponse` returns **exactly one entry per requested partition, in request
order, including authoritative misses** — matching the `load` contract above without an extra
round-trip to distinguish miss from omission.

### Recovery blob transport and pointer publication

Implemented across Celeborn commits `05fbdcafe`, `71b879be6` and `67b89066b` (worker-to-worker
replicate and replicated repair are still uncommitted in-flight work). Design:
`CELEBORN-BLOB-BACKEND-DESIGN.md`.

New message types (`common/src/main/proto/TransportMessages.proto`):

| ID | Message | Purpose |
|---|---|---|
| 113/114 | `PushRecoveryBlob` | executor/client → worker, content-addressed upload |
| 115/116 | `FetchRecoveryBlob` | read a verified copy from a replica |
| 117/118 | `PublishRecoveryBlobPointer` | client → master, CAS the pointer after quorum |
| 119/120 | `GetRecoveryBlobPointer` | read one pointer |
| 121/122 | `ReplicateRecoveryBlob` | worker → worker, repair replication |

The master's Raft state machine gains two commands (`master/src/main/proto/Resource.proto`):
`PublishRecoveryBlobPointer = 35` and `RepairRecoveryBlobPointer = 36`. Replicated state gains a
fifth map, `recoveryBlobPointers`.

**The pointer record** (`PbRecoveryBlobPointer`) is what Raft holds instead of the payload:

```
appId, recoveryId, writeId, partitionId   // identity, length-delimited key as in §2
bytes   sha256          // content identity of the payload
int64   length
int64   generation      // advances only on repair; digest and length never change
int32   formatVersion   // pointer record version, NOT the envelope version
repeated string workerIds
int64   createdAtMs
```

**Quorum-before-CAS is the load-bearing ordering.** A worker accepts a blob only after fsync,
digest re-verification from the written bytes, an atomic rename to `<sha256>.blob`, and a parent-dir
fsync; content addressing makes re-upload idempotent. Publication proceeds only when at least
`celeborn.master.recovery.blob.quorum` (default 2) of `replicationFactor` (default 3) workers have
acknowledged, and only then is the pointer CASed under the lease epoch/owner. The master validates
identity bounds, digest length, `length > 0`, `workerIds.size >= quorum` and lease ownership before
applying first-writer-wins. Consequence: **a published pointer can never reference an unreadable
payload**, which is why a reader can trust Raft alone.

Reads verify length and digest against the pointer on every fetch, fail over to the next listed
replica on mismatch or loss, and treat exhausted replicas as a **hard failure — never a miss**
(invariant I-1). Payloads at or under `inlineThreshold` (default 4k) skip blobs entirely and stay
inline; the blob cap itself is `celeborn.master.recovery.blob.maxPayloadSize` (default 64m), which
is what narrows the §3 payload-size gap.

Repair (background scan on the leader every `repairInterval`, default 5m) republishes verified bytes
to enough new workers and CASes an updated pointer with the **same digest** and an incremented
generation, so repair moves only the replica list. Unreferenced blobs are deleted after
`orphanGrace` (default 1h); released pointers are tombstoned and replicated before any blob delete.
The designed remote-storage second tier (`CELEBORN-BLOB-BACKEND-DESIGN.md` §8) is **not
implemented**; a remote write alone would never satisfy publication quorum anyway.

### Inline-storage limits (all `master` category, default version `1.0.0`)

| Key | Default | Bounds |
|---|---|---|
| `celeborn.master.applicationLease.maxDuration` | `24h` | max grantable lease |
| `celeborn.master.recovery.taskCommit.maxPayloadSize` | `1m` | one record |
| `celeborn.master.recovery.taskCommit.maxBatchResponseSize` | `16m` | one batch response |
| `celeborn.master.recovery.taskCommit.maxInlineBytesPerRecovery` | `256m` | per execution |
| `celeborn.master.recovery.taskCommit.maxInlineRecordsPerRecovery` | `200000` | per execution |
| `celeborn.master.recovery.taskCommit.maxInlineBytesGlobal` | `512m` | per cluster |
| `celeborn.master.recovery.taskCommit.maxInlineRecordsGlobal` | `1000000` | per cluster |

Capacity is reserved with `Math.addExact` before insertion and released on failure, so a rejected
publish leaks no accounting. These limits exist because the **current backend stores payloads inline
in Raft state** — see §9.

---

## 6. Spark execution ordering

The ordering is the protocol. From `WriteToDataSourceV2Exec.writeWithV2`:

1. Compute `PhysicalWriteInfoImpl(rdd.getNumPartitions)`.
2. If the write is recoverable: reject a catalog transaction, reject row-level operations
   (`recoveryUnsupportedReason`), require a `HasRecoveryTaskCommitStore`.
3. Build the manifest, `resolveWriteManifest`, and **require the CAS winner to equal the proposal
   byte-for-byte**. Abort otherwise.
4. `recoverable.recover(info)` → `BatchWriteRecoveryState`.
5. `load` the durable store in batches of 1024, decode each envelope, and cross-check it against
   whatever the connector reported:
   - a connector-reported message with no store record → **fail** ("recovered partition without an
     authoritative task store commit"),
   - store payload ≠ connector payload → **fail**,
   - row counts disagree → **fail**.
   The durable store is authoritative; the connector's own view is only corroborating.
6. `partitionsToWrite = messages.indices.filter(messages(_) == null)`. Only these run.
7. If `isCommitted()` — no writer tasks, **no global commit**, restore `totalNumRows` into the metric.
8. `useCommitCoordinator = batchWrite.useCommitCoordinator && taskCommitContext.isEmpty` — the
   driver-local `OutputCommitCoordinator` is **disabled** for recovery writes, because a coordinator
   denial after an accepted-but-unacknowledged CAS would wedge the write until another restart. The
   durable CAS is the coordinator.
9. On failure with recovery active: `abortAfterRecovery(messages)` instead of `abort(messages)`,
   preserving durable commits for the next driver.

Executor task path (`DataWritingSparkTask.run`):

1. **Preflight**: `load(recoveryId, [partId])` *before* creating a writer. A hit returns the
   canonical result immediately — no writer, no re-consumption of the input. This is what makes an
   accepted-but-reply-lost publish cheap to recover from.
2. Write, `dataWriter.commit()`, set `writerCommitted = true`.
3. Encode the envelope, `publish`, decode the canonical winner.
4. If `canonical != proposed`, this attempt lost: `SupportsRecoveryCommitDiscard.discardCommittedOutput(msg)`
   deletes this attempt's output. A writer that cannot discard is a hard error.
5. `dataWriter.abort()` is called **only when `writerCommitted == false`** — abort after a successful
   commit has undefined semantics and is now unreachable.

Shuffle path (`ShuffleStageRecoveryHandler`, `core/.../shuffle/ShuffleStageRecovery.scala`):

- `tryRecover(shuffleId, numMappers, numPartitions)` is consulted **before map tasks are submitted**.
- `None` is *authoritative permission to compute*; the handler must durably record the stage intent
  before returning it, so a later lookup cannot mistake committed output for another generation.
- Anything unavailable, indeterminate or corrupt must **throw**, not return `None`.
- Before returning `Some`, the handler must have **atomically adopted the physical shuffle** under
  `shuffleId` in the active shuffle manager; the DAGScheduler then installs scheduler metadata and
  skips every map task.
- `abortRecovery` removes *only driver-local* state from a failed adoption — never durable state.
- `onStageCompleted(shuffleId, statistics)` is called synchronously once real map outputs are
  registered and readable.

---

## 7. Connector contract (as implemented by Iceberg)

`SupportsBatchWriteRecovery extends BatchWrite` requires:

| Method | Contract |
|---|---|
| `recoveryId()` | stable, normally `LogicalWriteInfo#queryId()` |
| `commitMessageCodec()` | stable `codecId` + positive `version`; **Java/Kryo serialization forbidden** — records outlive the classes that wrote them |
| `recoveryCompatibilityMetadata(info)` | stable non-executable encoding of everything that changes the meaning of a task commit |
| `recover(info)` | durable state, loaded before any writer is created |
| `abortAfterRecovery(messages)` | must preserve every durable task commit |

Iceberg binds into the compatibility metadata (`SparkWriteRecoveryCompatibility`): table UUID, full
write schema, partition spec, sort order, file format, target file size, fanout behaviour, write and
snapshot properties, branch/WAP state, operation type, base-snapshot concurrency policy, overwrite
filter and validation settings. Data files are encoded through Iceberg's stable JSON parser
representation — no executable object deserialization anywhere on the path. The codec identifies
itself as `codecId = "iceberg-data-files"`, `version = 1` (`SparkWriteRecoveryTaskCodec`), and its
own payload repeats a magic + version header, size caps, type checks and a checksum, so a payload is
validated once by Spark's envelope and again by the connector's decoder.

Supported operations: batch append, dynamic overwrite, overwrite-by-filter. Copy-on-write and
position-delta paths **fail closed before writers are created**. Spark refuses row-level operations
independently (`RowLevelWriteExec.recoveryUnsupportedReason`), so the two sides fail closed
redundantly rather than trusting each other.

### Iceberg global-commit idempotency ledger

`SnapshotUpdate.idempotencyKey(property, value)` (default implementation throws
`UnsupportedOperationException`, so it is opt-in per implementation). `SnapshotProducer` then:

- writes a ledger entry into **table metadata properties**, atomically with the snapshot, under
  `commit.idempotency.entry.<hash>`;
- before every optimistic retry, re-reads and checks the ledger; a hit returns the existing snapshot
  ID and **suppresses duplicate listener notification** (`duplicateCommit` gate);
- retains the full original property/value in the entry so a hash collision is *detected*
  (`Preconditions.checkState` on both lookup and rewrite) rather than silently accepted;
- falls back to scanning retained snapshot summaries when no ledger entry exists — which is what
  makes the mechanism work on a table written by an older Iceberg;
- expires entries older than `commit.idempotency.retention-ms` (default 7 days) and refuses to exceed
  `commit.idempotency.max-entries` (default 10 000) — **backpressure, not eviction**, so valid proof
  is never dropped to make room.

Ledger entry encoding: versioned (`IDEMPOTENCY_LEDGER_VERSION = 1`), `DataOutputStream` + Base64,
≤ 16 KiB encoded, carrying property, value, snapshot ID, commit timestamp and added-row count.

---

## 8. Invariants

- **I-1 Fail closed.** Every unavailable, ambiguous or corrupt lookup throws. `None`/`null` means an
  authoritative absence and nothing else.
- **I-2 Immutability.** No published record is ever overwritten: task commits, shuffle catalogs,
  source anchors, write IDs and manifests are all first-writer-wins.
- **I-3 The durable store is authoritative.** A connector-reported commit without a store record is
  an error, not a shortcut.
- **I-4 Identity, not locality.** No driver-local ID (shuffle/stage/RDD/task attempt) participates in
  any recovery identity.
- **I-5 Fencing precedes durability.** Every publish carries the lease epoch and owner; workers
  enforce the lease they were told about, so an old driver cannot write after takeover.
- **I-6 Manifest equality before work.** No writer is created until the CAS-resolved manifest equals
  the proposal byte-for-byte.
- **I-7 No executable deserialization.** Java/Kryo serialization is forbidden for anything that can
  outlive the process, on all three sides.
- **I-8 Bounded everything.** Identities, payloads, batch responses and per-recovery/global totals
  all have hard bounds; accounting is reserved before insert and released on failure.

---

## 9. Known gaps in the protocol as it stands

1. **Inline payload storage in Raft — superseded by the blob backend, with one tier missing.** The
   inline store remains for small payloads (≤ 4k) and as a read fallback; large payloads now go to
   worker-replicated content-addressed blobs with quorum-before-CAS publication (see §5). Still
   missing: the remote-storage second durability tier.
2. **Payload-size mismatch** between Spark's 16 MiB envelope cap and Celeborn's 1 MiB default
   ingress cap (§3).
3. **Iceberg source-snapshot retention.** A pinned snapshot can be expired while the driver is down.
   Correctness fails closed; availability does not survive. The designed fix is a deterministic
   recovery tag derived from `recoveryId + sourceId`, pinned before Celeborn accepts the source
   anchor, with a max-ref-age longer than the recovery store's TTL. Not implemented — and it needs
   Spark to expose the execution recovery identity to the connector during source anchoring.
4. **No TTL coupling.** Celeborn's recovery-record retention and Iceberg's ledger horizon are
   configured independently and can disagree. See `RETENTION-AND-SIZING.md`.
5. **Envelope-version negotiation — decided 2026-08-25.** Task envelopes use a readable set
   {1,2,3} with writers emitting the feature-derived required version and readers failing closed
   on both unknown versions and known-but-mismatched ones (see `UPGRADE-AND-ROLLBACK.md` §2).
   The write-manifest channel carries its own ManifestVersion4. What remains is fixture coverage:
   golden files exist for task envelopes v1/v2 only; v3 and manifest v4 need them before C7 freezes
   the artifact.
6. **Streaming/micro-batch is out of scope** and unsupported by construction.

---

## 10. Reading list, by concern

| Concern | File |
|---|---|
| Envelope format | `spark-resumable-upstream/sql/core/.../datasources/v2/RecoveryTaskCommit.scala` |
| Store contract | `spark-resumable-upstream/sql/catalyst/.../connector/recovery/RecoveryTaskCommitStore.java` — moved out of `connector.write` into a new `o.a.s.sql.connector.recovery` package (commit `ace8a2de502`); the remaining recovery interfaces still live in `connector.write` |
| Connector contract | `.../connector/write/SupportsBatchWriteRecovery.java`, `BatchWriteRecoveryState.java`, `RecoveryCommitMessageCodec.java` |
| Write execution ordering | `.../datasources/v2/WriteToDataSourceV2Exec.scala` |
| Shuffle SPI | `spark-resumable-upstream/core/.../shuffle/ShuffleStageRecovery.scala` |
| Scheduler integration | `core/.../scheduler/DAGScheduler.scala`, `core/.../MapOutputTracker.scala` |
| Celeborn wire | `celeborn/common/src/main/proto/TransportMessages.proto`, `master/src/main/proto/Resource.proto` |
| Celeborn blob transport | `celeborn/client/.../recovery/RpcRecoveryBlobTransport.scala`, `RecoveryBlobReplication.scala`, `celeborn/worker/.../RecoveryBlobStore.scala`, `RecoveryBlobCollector.scala` |
| Celeborn CAS + limits | `celeborn/master/.../clustermeta/AbstractMetaManager.java` |
| Celeborn identity validation | `celeborn/common/.../util/RecoveryTaskCommitUtils.java` |
| Celeborn client binding | `celeborn/client/.../LifecycleManager.scala`, `client-spark/spark-3/.../CelebornShuffleStageRecoveryExtension.java` |
| Iceberg codec/compat | `oss-fixes/iceberg/spark/v4.1/.../source/SparkWriteRecovery*.java` |
| Iceberg ledger | `oss-fixes/iceberg/core/.../SnapshotProducer.java`, `TableProperties.java`, `api/.../SnapshotUpdate.java` |
