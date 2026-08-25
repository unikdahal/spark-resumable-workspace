# Version compatibility, rolling upgrade, and rollback

Read out of the working tree on 2026-08-24.

## 1. The four version numbers

| # | Version | Where | Current | Check on read |
|---|---|---|---|---|
| V1 | Spark envelope `formatVersion` | `RecoveryTaskCommitEnvelope` | `1` | **exact equality** (`== FormatVersion`) |
| V2 | Connector codec `version()` | `RecoveryCommitMessageCodec` | Iceberg `1` | passed to `decode(version, payload)` |
| V3 | Celeborn identity-namespace version | `source:v1:` / `write:v1:` prefixes | `v1` | part of the key; a mismatch is a miss, not an error |
| V4 | Iceberg ledger version | `IDEMPOTENCY_LEDGER_VERSION` | `1` | validated on decode |

Plus one implicit version: the Celeborn recovery key contains **`session.version()`**
(`CelebornShuffleStageRecoveryExtension.recoveryKey`), so *any* Spark version change invalidates
every shuffle recovery key. That is the safe default — a Spark upgrade between the two drivers means
no adoption, full recomputation, no risk — and it should be preserved deliberately, not
accidentally dropped as an optimisation.

## 2. Rolling upgrade: what actually works today

### Spark envelope (V1) — **decision made 2026-08-25: readable set + exact-feature match**

The mechanism the proposal sketched is now implemented in
`RecoveryTaskCommitEnvelope`: decode accepts a **readable set** (`Version1 || Version2 || Version3`,
with ManifestVersion4 on the separate write-manifest channel), while writers emit exactly
`requiredFormatVersion(context)` - derived from the execution's own feature requirements
(row-level summary -> 3, metrics schema -> 2, otherwise 1).

Two rules make this safe rather than merely lenient:

1. **Unknown versions still fail loudly** ("Unsupported ... envelope version"), so an old driver
   reading a newer record cannot silently mis-decode.
2. **A known-but-different version also fails** ("does not match required version"): if the record's
   version differs from what this driver's own execution context produces, the two drivers
   disagree about the execution's feature set, and adopting the record would mean decoding with
   the wrong layout expectations. That disagreement fails closed instead of degrading.

Consequence for rolling upgrades: replacing a driver mid-recovery with a different Spark build or
different write options aborts that write's recovery (recomputation, no corruption); upgrading the
fleet between independent executions needs nothing at all. Adding a future v4 task envelope means
adding it to the readable set **and** shipping a golden fixture per accepted version - the
write-manifest v4 fixture is still outstanding (see VERIFICATION-STATUS findings #4).

### Connector codec (V2) — contract says yes, Iceberg says no

`RecoveryCommitMessageCodec`'s javadoc is explicit: *"Implementations must remain able to decode
every version that may be present in the durable recovery store during a rolling upgrade."*
Iceberg's implementation does not do that — `SparkWriteRecoveryTaskCodec` asserts
`version == VERSION` on decode. With one version in existence this is unobservable, but it means the
reference implementation currently models the wrong pattern for the next connector author who copies
it. Fix when a v2 appears; note it now.

### Celeborn masters — **must be upgraded before the feature is enabled**

The replicated snapshot (`Resource.proto` `PbSnapshotMetaInfo`) gains five maps at fields 23–27:
`applicationLeases`, `applicationWorkers`, `committedShuffleCatalogs`, `sourceRecoveryAnchors`,
`recoveryTaskCommits`. Under proto3, a master build that does not know those fields **silently drops
them when it re-serialises a snapshot** — every recovery record in the cluster disappears without an
error. Likewise, message types 97–112 are unknown to an old master and fail at dispatch.

Therefore the supported sequence is:

1. Upgrade **all** master replicas (and workers, for `FenceApplication` enforcement).
2. Verify the ensemble is stable and a leader has been elected.
3. *Then* enable `spark.celeborn.driverRecovery.enabled` on jobs.

Enabling recovery against a partially upgraded ensemble is not a degraded mode; it is silent state
loss on the next leader-side snapshot. This constraint should be stated in the Celeborn upgrade
notes, not just in a design doc.

### Iceberg ledger (V4) — forward-compatible by construction

Ledger entries are ordinary table properties under `commit.idempotency.entry.<hash>`. An older
Iceberg reader ignores unknown properties, and `findIdempotentSnapshotID` falls back to scanning
snapshot summaries when no ledger entry exists — so a table written with the ledger stays readable
and writable by an Iceberg build without it. **Rollback-safe.** The residue is a set of unused
table properties, bounded by `commit.idempotency.max-entries`.

## 3. Rollback matrix

| Component | Roll back to a build without recovery? | Residue | Risk |
|---|---|---|---|
| Spark driver | Yes — remove the extension registration | none | job recomputes from scratch; no state read |
| Celeborn master | Yes, but **only after** no job has recovery enabled | recovery maps dropped from the snapshot on first re-serialise | rolling back with live recovery state destroys it silently |
| Celeborn worker | Yes | persisted `ApplicationLeaseStore` entries become inert | old driver fencing stops being enforced by that worker |
| Iceberg | Yes | unused `commit.idempotency.entry.*` table properties | none; the summary-scan fallback still detects duplicates while snapshots are retained |
| Iceberg table properties | `ALTER TABLE … UNSET TBLPROPERTIES` for the two tunables | ledger entries remain until expiry | none |

## 4. Kill switches, in order of bluntness

1. `spark.celeborn.driverRecovery.enabled=false` — per job, immediate, no state touched. This is the
   switch to reach for.
2. Remove `CelebornShuffleStageRecoveryExtension` from `spark.sql.extensions` — per job, also stops
   the reflection probe that fails session construction on a Spark build without the SPI.
3. Do not register a `RecoveryTaskCommitStore` — write recovery alone is disabled; shuffle recovery
   still works, because a write with `SupportsBatchWriteRecovery` and no store fails closed with
   *"A recoverable batch write has no durable task commit store"*.
4. Lower `celeborn.master.recovery.taskCommit.maxInlineBytesGlobal` to `0` — cluster-wide refusal of
   new task commits. Blunt: in-flight recoverable writes fail at publish, after doing their work.

There is no dynamic "drain recovery state" operation. See `RETENTION-AND-SIZING.md` §5.

## 5. Downgrade of a *running* execution

Not supported, and it should be stated plainly: a recovery execution is identified by
`spark.celeborn.driverRecovery.id` and its state is bound to the Spark version (through the recovery
key), the codec version, the envelope version and the manifest. A replacement driver on a different
Spark or Iceberg build will, at best, fail the manifest comparison and refuse to adopt; at worst it
simply finds no matching key and recomputes. Both are safe; neither is a downgrade path. **Restart a
resumable execution on the same binaries or accept full recomputation.**

## 6. What upstream review will ask for that does not exist yet

- **MiMa / binary-compatibility report** for the new `sql/catalyst` interfaces (`@Evolving`, so the
  bar is lower, but `LogicalWriteInfo` gained a method and that is a source-compatible-only change
  for implementors outside Spark).
- **revapi** clearance for Iceberg's `SnapshotUpdate.idempotencyKey` (a default method, so binary
  compatible; still a public API addition needing a vote).
- A statement of the **experimental/evolving status** of `ShuffleStageRecovery`
  (`@DeveloperApi @Experimental` today) and what stability it will carry at release.
- Protocol compatibility tests: an old-envelope fixture decoded by the new code, and a new-envelope
  fixture rejected by the old. Neither exists.
