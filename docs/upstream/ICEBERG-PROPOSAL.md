# Iceberg proposal: idempotent snapshot updates, and recoverable Spark writes

Draft, written against `oss-fixes/iceberg` as of 2026-08-24. Two independent proposals; the first
does not depend on Spark at all and should be submitted on its own.

---

## Proposal 1 — `SnapshotUpdate.idempotencyKey(property, value)`

### Problem

A writer that loses its response to a successful commit cannot tell whether the commit happened. On
retry it may create a second snapshot with the same data. Checking snapshot summaries for a marker
almost works, but fails in two ways: two concurrent drivers can both check, both miss, and both
commit; and snapshot expiry can remove the only proof that a commit ever happened.

### Proposal

```java
default ThisT idempotencyKey(String property, String value) {
  throw new UnsupportedOperationException(...);
}
```

`SnapshotProducer` writes a **ledger entry into table metadata properties**, atomically with the
snapshot, under `commit.idempotency.entry.<hash>`, and consults the ledger before every optimistic
retry. Same-key concurrent commits therefore converge on one snapshot through the catalog's own
atomic metadata swap, not through a best-effort scan.

Details that matter:

- The entry retains the **full original property and value**, so a hash collision is detected
  (`Preconditions.checkState` on both lookup and rewrite) rather than silently accepted.
- The entry records snapshot ID, commit timestamp and added-row count, so a caller can recover
  *completion* even after the snapshot itself has expired.
- Duplicate listener notification is suppressed: a detected duplicate returns the existing snapshot
  ID without re-notifying.
- Entries expire after `commit.idempotency.retention-ms` (default 7 days). Exceeding
  `commit.idempotency.max-entries` (default 10 000) produces **backpressure — the commit fails —
  rather than eviction**, because evicting a live entry would destroy the proof the mechanism exists
  to provide.
- When no ledger entry exists, the implementation falls back to scanning retained snapshot summaries,
  which keeps tables written by older Iceberg builds working.
- Encoding is versioned (`IDEMPOTENCY_LEDGER_VERSION = 1`), size-bounded (4 KiB per field, 16 KiB
  encoded) and non-executable.

### Compatibility

Purely additive: a default method on the public interface, and table properties an older reader
ignores. Rollback leaves at most `max-entries` unused properties behind. Needs revapi clearance and a
review of whether the ledger belongs in table properties or in a dedicated metadata field.

### Testing

`TestSnapshotIdempotency` covers races, retry, expiration, corruption, retention, capacity and event
behaviour. It lives in `core/`, which has no Spark dependency, so it can be run and reviewed today.

---

## Proposal 2 — recoverable Spark writes

### Problem

Even with idempotent global commits, a driver restart re-runs every writer task, because the task
commit messages lived only in the dead driver's memory.

### Proposal

Implement Spark's `SupportsBatchWriteRecovery` in `SparkWrite`:

- **Stable codec** (`SparkWriteRecoveryTaskCodec`, `codecId = "iceberg-data-files"`, version 1) that
  encodes committed data files through Iceberg's own JSON parser representation, with magic, version,
  size, type and checksum validation. **No executable object deserialization** on any recovery path,
  and the previous object-store task sidecars are removed — Spark plus the durable store is the
  arbitration authority.
- **Compatibility metadata** (`SparkWriteRecoveryCompatibility`) binding table UUID, write schema,
  partition spec, sort order, file format, target file size, fanout behaviour, write properties,
  snapshot properties, branch and WAP state, operation type, base-snapshot concurrency policy, and —
  for overwrite-by-filter — the canonical filter expression, isolation level and validation snapshot.
  A replacement driver whose write differs in any of these fails the manifest comparison before a
  single writer is created.
- **Losing-attempt cleanup** through `SupportsRecoveryCommitDiscard`, deleting the attempt's own data
  files with Iceberg's `FileIO`.

Supported: batch append, dynamic overwrite, overwrite by filter. Copy-on-write and position-delta
paths fail closed *before* writers are created, redundantly with Spark's own refusal.

### Open items

1. **Which Spark version.** The code lives in `spark/v4.1`, which pins Spark 4.1.3, while the
   recovery API exists only in a 5.0.0-SNAPSHOT fork. Nothing can be built or reviewed until this is
   decided — see `PATCH-SPLIT-PLAN.md`.
2. **Source snapshot retention.** A pinned snapshot can be expired while the driver is down.
   Correctness fails closed; availability does not survive. The design is a deterministic recovery
   tag derived from `recoveryId + sourceId`, pinned before the source anchor is accepted, with a
   maximum reference age longer than the recovery window, verified on takeover and never falling
   forward to a newer snapshot. Not implemented; it also requires Spark to expose the execution
   recovery identity during source anchoring.
3. **Ledger horizon vs recovery window.** `commit.idempotency.retention-ms` must be ≥ the Celeborn
   recovery window; the two are configured independently today. See `RETENTION-AND-SIZING.md` §2.
4. Row-level operations, durable custom metrics, and catalog transaction interaction.
