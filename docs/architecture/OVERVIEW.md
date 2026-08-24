# How this project is shaped

Three generations exist because the hard questions arrived in order. The workspace mixed them
until they looked like one unfinished product. They are not.

## Generation 1 — original POCs

`resume-poc` answered: can a second Spark driver convince Spark that a shuffle map stage is
already complete and actually consume bytes produced by the first driver? Yes. It seeded
`MapOutputTracker`, made `findMissingPartitions` empty, and patched Celeborn so a new
`LifecycleManager` could adopt the old driver's committed shuffle.

It did not solve query identity, source identity, AQE, or DPP.

`resume-poc-digest` attacked the dangerous half: is this really the same computation? It tested
cross-JVM fingerprints on real Delta and Iceberg scans, source mutations, config changes, and
DPP-bearing plans. It found Iceberg `Broadcast[Table]` instability from serializing connector
objects and moved toward connector-aware input identities.

`resume-poc-sql` was a set of focused SQL/AQE demos (DPP, writes, results, broadcast, stage
digests, Iceberg identities). Research evidence, not a product.

`resume-poc-e2e` combined identity, AQE, real Celeborn adoption, and a second JVM. It skipped a
shuffle stage across driver restart and exposed bugs the isolated POCs missed: Celeborn
stage-end commit timing, missing SQL metrics that made AQE treat a recovered stage as empty,
and fabricated mapper statistics that prevented skew splitting.

The lasting lesson: recovery is not restoring bytes. It is restoring enough execution state
that every consumer of those bytes behaves as though the stage had really executed.

## Generation 2 — `spark-resume`

A clean extraction: `AnchorStore` (metadata) and `ExchangeStore` (bytes), an `AdmissionEngine`,
and Spark integration via `ExecutionSkipRule`. That rule hooks query-stage preparation and
replaces `ShuffleExchangeLike` with a leaf `SkippedExchangeExec` whose RDD reads one partition
from the exchange store.

That proves the abstraction. It is not the mechanism to upstream. It bypasses the scheduler
instead of teaching Spark recovery.

## Generation 3 — native Spark / Celeborn / Iceberg

Recovery moves into scheduler and connector contracts.

`ShuffleDependency` carries a `ShuffleStageRecoveryHandler`. `DAGScheduler.createShuffleMapStage`
asks it before registering the stage. A committed catalog yields synthetic `MapStatus` values,
exact reducer statistics for AQE, and zero map tasks. `None` means authoritatively no committed
recovery exists and Spark may compute. Ambiguity, corruption, provider unavailability, or failed
adoption throw — fail closed.

If a recovered shuffle later becomes unreadable, the scheduler aborts rather than recomputing
the old map stage. Recomputation could create a new physical generation and mix it with durable
recovered state.

The same track covers write recovery: skip already-committed partitions, executor preflight
against a late winner, first-writer-wins CAS, discard of losing physical output. Iceberg closes
final-commit ambiguity with an idempotency key on `SnapshotUpdate` and a durable ledger.

## Documents

| Read | For |
|---|---|
| [READ-RECOVERY.md](READ-RECOVERY.md) | Source anchors, shuffle identity, Driver A/B, Celeborn adoption |
| [WRITE-RECOVERY.md](WRITE-RECOVERY.md) | Task CAS, Iceberg snapshot idempotency |
| [../upstream/README.md](../upstream/README.md) | Contract, proposals, verification, patch split |
| [../history/README.md](../history/README.md) | POC-era design (not current) |

## What a newcomer should not do

- Do not start in a POC README or in `docs/history/`.
- Do not merge `spark-resume`'s plan-rewrite skip with the scheduler SPI. They are alternative
  implementations.
- Do not treat a missing recovery record as "try anyway" or "unknown". `None` is authoritative
  absence; everything else fails the job.
