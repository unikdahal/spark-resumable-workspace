# Design: worker-replicated storage for recovery task payloads (work package C2)

Status: **implemented** in `celeborn` commits `05fbdcafe`, `71b879be6`, `67b89066b`, with
worker-to-worker replicate and replicated repair in flight (see `VERIFICATION-STATUS.md`). This is
the design the implementation was built against; deviations from it are noted inline. Written
against `celeborn` as of 2026-08-24.

## 1. Why the inline store is not the end state

Task commit envelopes are Spark-owned opaque bytes. Today the master stores them **inline in
replicated state**, bounded by seven configuration limits (1 MiB per record, 256 MiB / 200 000
records per recovery, 512 MiB / 1 000 000 cluster-wide). Those bounds make the current design safe
against unbounded heap growth, and they also make it a **≤100 K-partition mechanism** — every byte
rides in the Raft log, in master heap, and in every snapshot.

The replacement keeps exactly one property of the current design — an immutable, lease-fenced,
first-writer-wins pointer per `(recoveryId, partitionId)` — and moves the payload out of Raft.

## 2. What Raft holds afterwards

```
key   = len(appId):appId len(recoveryId):recoveryId len(writeId):writeId partitionId
value = PbRecoveryBlobPointer {
          bytes  sha256        // 32 bytes, content identity
          int64  length        // exact payload length
          int64  generation    // monotonic per key, for tombstone ordering
          int32  formatVersion // pointer record version, not the envelope version
          repeated string workerIds   // replicas that acknowledged a durable write
          int64  createdAtMs
        }
```

Roughly 150-250 bytes per partition regardless of payload size: a 1 M-partition write becomes
~200 MiB of pointers rather than gigabytes of payload, and the payload never enters a snapshot.

The pointer, not the payload, is the CAS target. **First pointer wins**, exactly as today, so
arbitration semantics do not change and no Spark-side contract moves.

## 3. Write path

1. **Executor** encodes the Spark envelope (unchanged) and computes `sha256`.
2. **Client** asks the LifecycleManager for `replicationFactor` eligible workers, chosen by the same
   slot-eligibility rules used for shuffle data, excluding workers already holding this digest.
3. **Upload** to each selected worker under the content-addressed key `sha256`:
   - worker writes to `recovery-blobs/<sha256 prefix>/<sha256>.tmp.<uuid>`;
   - `fsync` the file, then verify length **and** recomputed digest before publishing;
   - atomically `rename` to `<sha256>.blob`; a pre-existing identical blob is success, not an error
     (content addressing makes upload naturally idempotent);
   - `fsync` the parent directory so the rename survives a crash.
4. **Quorum**: publication proceeds only when at least `celeborn.master.recovery.blob.quorum`
   workers acknowledge. Fewer than quorum is a failure, never a partial success.
5. **CAS the pointer** through the current application lease epoch/owner. The master validates:
   identity bounds, digest length, `length > 0`, `workerIds.size >= quorum`, and lease ownership —
   then applies first-writer-wins.
6. **Response** carries the canonical pointer. If this attempt lost, the caller receives the winner's
   pointer and its own blob becomes garbage (§7).

Ordering matters and is not negotiable: **blobs are durable before the pointer exists.** A pointer
can therefore never reference an unreadable payload, which is the invariant that lets a reader trust
Raft.

## 4. Read path

1. Look up the pointer (single or batched, same bounded batching as today).
2. Fetch from the first listed replica; verify length and digest against the pointer.
3. On mismatch, timeout, or worker loss, try the next replica and record a corrupt-read metric.
4. Exhausting replicas is a **hard failure** — never a miss. An absent pointer is an authoritative
   miss; an unreadable payload is an error. Collapsing the two would let a replacement driver
   recompute work that is actually committed.
5. Successful reads from a degraded replica set enqueue a repair (§6).

## 5. Fencing and leader change

- Every upload and every pointer CAS carries the lease epoch and owner; the master rejects a stale
  epoch exactly as it does today.
- A leader change between quorum acknowledgement and pointer CAS is safe by construction: the blobs
  are content-addressed and idempotent, so the client simply retries the CAS against the new leader.
  Either it wins with the same digest, or it learns the canonical winner.
- A leader change **after** the CAS is a normal Raft replay; the pointer is already committed.
- Uploads in flight during a leader change are never "half-published": without a committed pointer
  they are simply unreferenced blobs, collected by §7.

## 6. Repair

A background task on the leader scans pointers whose `workerIds` no longer satisfy quorum against
live workers, and for each: reads a verified copy, uploads it to enough new workers, and CASes an
updated pointer with the same `sha256`/`length` and an incremented `generation`. **The digest never
changes**, so a repair cannot alter what a recovery reads; only the replica list moves. Rate-limited
and bounded per cycle so repair cannot starve the write path.

## 7. Garbage collection and tombstones

Two kinds of garbage:

- **Losing speculative uploads** — a blob whose digest no pointer references. Deleted after a grace
  period (`celeborn.master.recovery.blob.orphanGrace`, default 1h) that must exceed the longest
  possible gap between upload and CAS. The grace period is what makes GC safe against a slow client.
- **Superseded or released pointers** — when a recovery's state is dropped (application loss, or an
  explicit completed-execution release), the pointer is **tombstoned first**, replicated, and only
  then are blobs deleted. Never delete a blob whose pointer is still visible to any replica.

Ordering: `tombstone(pointer) -> replicate -> delete blobs -> reap tombstone after retention`. A
crash at any point leaves either a live pointer with live blobs, or a tombstone with orphan blobs —
both recoverable, neither dangling.

## 8. Remote-storage fallback

Where `celeborn.master.recovery.blob.remoteStorage` is configured, a blob is additionally written to
remote storage under the same content-addressed key, and the pointer records that fact. Total
worker-set loss then degrades to a slower read rather than a lost recovery. The fallback is a
*second* durability tier, never a substitute for quorum: a remote write alone does not satisfy
publication.

## 9. Configuration

| Key | Default | Meaning |
|---|---|---|
| `celeborn.master.recovery.blob.enabled` | `false` | switch between inline and blob backends |
| `celeborn.master.recovery.blob.replicationFactor` | `3` | replicas targeted per blob |
| `celeborn.master.recovery.blob.quorum` | `2` | acknowledgements required before the pointer CAS |
| `celeborn.master.recovery.blob.inlineThreshold` | `4k` | payloads at or under this stay inline; small commits should not pay three network round trips |
| `celeborn.master.recovery.blob.orphanGrace` | `1h` | delay before an unreferenced blob may be deleted |
| `celeborn.master.recovery.blob.repairInterval` | `5m` | replica repair scan period |
| `celeborn.worker.recovery.blob.dirs` | worker storage dirs | where blobs live |
| `celeborn.master.recovery.blob.remoteStorage` | unset | optional second durability tier |

The seven existing inline limits remain and continue to govern pointers and small inline payloads.

## 10. Metrics

Counters: blob uploads (`outcome=ok|quorum-failed|rejected`), pointer CAS
(`outcome=winner|duplicate|fenced|rejected`), reads (`outcome=ok|replica-failover|corrupt|failed`),
repairs (`outcome=started|completed|failed`), GC (`outcome=orphan-deleted|tombstone-reaped|skipped`).
Gauges: live pointers, referenced blob bytes, orphan blob bytes, pointers below quorum.
Timers: upload, quorum wait, pointer CAS, read.

Without these, an operator cannot answer "is recovery state healthy" — see `RUNBOOK.md` §7.

## 11. Failure matrix the implementation must be tested against

| Failure | Required outcome |
|---|---|
| Worker dies after fsync, before ack | quorum not met, publication fails, blob becomes orphan |
| Worker dies after ack, before pointer CAS | CAS proceeds if remaining acks meet quorum, else fails |
| Leader change between quorum and CAS | retry converges on one canonical pointer |
| Two attempts upload identical bytes | same digest, one canonical pointer, no duplicate storage |
| Two attempts upload different bytes | first pointer wins; loser's blob is orphaned and collected |
| Replica corrupted on disk | detected on read, bypassed, repaired |
| All replicas lost, remote fallback configured | read succeeds from remote |
| All replicas lost, no fallback | hard failure, never a silent miss |
| Application lost mid-upload | tombstone-then-delete leaves nothing dangling |
| GC racing a slow client | grace period prevents deleting a blob about to be referenced |

## 12. Acceptance (from the roadmap)

- Large commit messages never enter Raft logs or master heap as inline payloads.
- A worker loss leaves the canonical payload readable.
- A corrupt replica is detected, bypassed, and repaired.
- Leader failover between blob quorum and pointer CAS is safe and idempotent.
- Orphan GC never deletes a live canonical payload.

## 13. Migration

The backend is selected per cluster. With `blob.enabled=false` nothing changes. Turning it on
affects **new** publications only; existing inline records stay readable, because the read path
checks the pointer map first and falls back to the inline map. A cluster can therefore be switched
without draining recoveries. Turning it back off leaves already-published blobs readable for as long
as their pointers live.
