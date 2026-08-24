# Design: recoverable row-level and transactional writes (Spark work package)

Status: implemented in the `spark-resumable-upstream` working tree; every named suite passes on
this machine (see `VERIFICATION-STATUS.md` §2a). Written 2026-08-25 from the code, not from intent.
Companion documents: `PROTOCOL-SPEC.md` (envelope and store contract),
`SPARK-SPIP-RESUMABLE-EXECUTION.md` (SPI).

## 1. The problem

Batch append, dynamic overwrite and overwrite-by-filter recovered end to end before this package,
but every row-level command failed closed by construction: `RowLevelWriteExec` reported
`recoveryUnsupportedReason`, and nothing durable existed to arbitrate a replacement driver's
DELETE/UPDATE/MERGE against the first driver's attempt. A crashed MERGE therefore re-ran every
task after restart, double-applying semantic actions against any sink that survived the first run.

Row-level writes differ from batch appends in three ways that make plain task-commit recovery
insufficient:

1. **The meaning of one task's output depends on the whole operation**, not just its partition —
   the same copied file bytes mean different things under different conditions, isolation levels or
   base snapshots.
2. **Semantic bookkeeping lives across tasks**: matched/unmatched counts, split-update halves, and
   the total summary a sink needs to commit exactly once.
3. **Two physical modes exist** with different failure shapes: copy-on-write (`REPLACE_DATA`) and
   position-delta (`WRITE_DELTA`).

## 2. The generation fence

`RowLevelWriteManifest.generation(recoveryExecutionId, recoveryId, sinkId, canonicalOperationSha256)`
derives `spark-row-v1-<base64(sha256)>` from length-delimited identities plus the canonicalised
operation digest. It is **Spark-owned** and computed identically by every driver incarnation of the
same logical execution, so it does not depend on when (or whether) a provider resolves a write ID.

Why not reuse the provider's `writeId`: the provider is a third party whose resolution is a remote
CAS. Deriving the fence from inputs Spark already holds means the fence exists before the first RPC
and cannot be raced between resolution and use.

## 3. The generation manifest (partition `-1`)

Before any writer is created, V2Writes resolves a Spark-owned manifest through
`RecoveryTaskCommitStore.resolveWriteManifest` — the same immutable first-writer-wins CAS used by
batch writes. `RowLevelWriteManifest.encode` pins, with magic `0x5352574d` ("SRWM"), version 1:

- identity block: execution ID, recovery ID, generation, sink ID, catalog identity, table name;
- operation block: command (`MERGE|UPDATE|DELETE`), physical mode (`REPLACE_DATA|WRITE_DELTA`),
  SHA-256 of the canonical operation, condition digest, optional conflict-filter digest;
- schema block: input/output/row/row-ID/metadata schemas as JSON;
- requirements block: encoded distribution (with strictness flag), ordering descriptions,
  required partition count, advisory partition size;
- anchor block: every source table's `sourceId -> immutable anchor`, rejecting inconsistent
  duplicates; optional transaction ID.

Caps: strings ≤ 64 KiB, ≤ 1024 entries, ≤ 16 MiB encoded. On the wire the manifest rides the
durable write manifest channel as envelope format version 4 (`ManifestVersion4`), next to — not
inside — the connector-owned compatibility metadata, preserving "Spark never parses a connector
payload". The winner must equal the proposal byte-for-byte; anything else aborts before a writer
exists. This is invariant I-6 extended from "partition count and codec" to "everything that gives
a row-level action its meaning".

## 4. Task-level semantics: action codes and the summary accumulator

Writers receive rows carrying a leading integer **semantic action code** that survives repartition,
sort and AQE. `RowLevelTaskSummaryAccumulator.record` interprets each code once, per task:

- logical actions increment exactly one counter (scanned/copied/deleted/updated/matched-*);
- control actions contribute to the durable summary but deliberately issue **no connector call**;
- split-update delete halves produce a connector delete but no logical update count — the paired
  reinsert carries the single logical update, so counting both halves would double-report.

Each task publishes its accumulator state through the durable store like any commit message; the
driver requires an **authoritative total summary** before allowing a global commit, and rejects a
recovered task whose required summary is absent (`row-level task summary presence false`) or a
globally committed write with none (`no authoritative summary`). Overflow in aggregation throws
rather than wrapping.

## 5. Recovery path

A replacement driver: resolves the manifest (§3, byte-equality or abort) → `recover(info)` loads
durable state before any writer exists → partitions with canonical commits are skipped and their
summaries restored **exactly once** → only genuinely unwritten partitions construct writers →
global commit requires the authoritative total summary. Writers that would duplicate canonical
commits are never constructed (`writersCreated === 0` is asserted in tests). A changed generation
manifest (`command` flipped DELETE→UPDATE in tests) fails adoption before writers; a missing
Spark-owned manifest fails outright rather than falling back to provider identity.

## 6. Transaction surface

`SupportsTransactionRecovery`, `TransactionRecovery{Info,Result,State}` and the
transaction-ID field in the manifest exist so catalog-atomic transactions can participate under
the same fence. Catalog-side enforcement is open (see §8).

## 7. Failure matrix covered by tests

| Failure | Required outcome |
|---|---|
| Manifest differs in any dimension | abort before writers |
| Recovered task without its required summary | fail closed |
| Globally committed write without total summary | fail closed |
| Summary overflow | throw, never wrap |
| Duplicate restore of a canonical summary | restored exactly once |
| Changed generation manifest between drivers | fail before task adoption |
| Wrong physical mode vs manifest | fail before writers |
| Missing Spark-owned manifest | fail; never fall back to provider identity |

## 8. Known gaps

1. Iceberg (and other connectors) still refuse row-level operations on their side; the redundant
   refusals are deliberate until a connector implements delta recovery against this contract.
2. Catalog-atomic transactions: manifest participates, but no catalog enforces transaction-scope
   idempotency yet.
3. `RETENTION-AND-SIZING.md` TTL coupling applies to these records unchanged.
