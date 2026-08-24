# Write-path recovery (upstream track)

The upstream track is not only "skip a completed shuffle." It is: recover the durable side
effects of a batch execution after driver loss — inputs, shuffle stages, individual write
tasks, and the final table commit.

Contract details: [../upstream/PROTOCOL-SPEC.md](../upstream/PROTOCOL-SPEC.md). Iceberg-specific
proposals: [../upstream/ICEBERG-PROPOSAL.md](../upstream/ICEBERG-PROPOSAL.md).

## Spark V2 write protocol (sketch)

```
Driver
  │
resolve durable writeId
  │
resolve / validate write manifest
  │
load durable task commit records
  │
  ├── partition already committed → reuse commit message
  └── missing partition → schedule task
                            │
                      executor preflight
                            │
                   commit already appeared?
                      /                 \
                    yes                 no
                     │                   │
               return old msg      create writer
                                         │
                                       write
                                         │
                                       commit
                                         │
                              publish immutable CAS
                                 /               \
                             won CAS           lost CAS
                               │                 │
                           canonical         discard own output;
                                             return winner's message
```

The driver loads durable records and schedules only missing partitions. The executor checks
again before creating a writer, closing the race where another attempt succeeded after the
driver's snapshot. After a physical commit, the recovery envelope is published into a durable
first-writer-wins store. A CAS loser discards its files and returns the canonical commit
message.

Ordinary `abort()` is not called after a successful writer commit. The output-commit
coordinator is disabled for recovery writes; the durable store is the coordinator.

## Iceberg final commit

A remaining hole is:

```
Iceberg commit succeeds
        │
driver dies before Spark observes success
        │
replacement retries the same logical commit
        │
idempotency key already present
        │
return existing snapshot
(do not create another)
```

The Iceberg work adds an idempotency key to `SnapshotUpdate` and a durable bounded
idempotency ledger, recorded in `SnapshotProducer` against the logical key. Spark write
recovery then pins the selected snapshot for the resumable execution.

Iceberg Proposal 1 (generic snapshot idempotency) is independently mergeable. Proposal 2
(Spark write recovery) depends on Spark PR-1. See [../upstream/PATCH-SPLIT-PLAN.md](../upstream/PATCH-SPLIT-PLAN.md).

## Open contract issues (do not add features until these are decided)

- Spark envelope max payload vs Celeborn default payload max
- Celeborn authentication as a correctness requirement, not only hardening
- completed-execution cleanup / retention; unbounded `applicationLeases`
- Spark version target for Iceberg (4.1 module vs local 5.0 APIs)
