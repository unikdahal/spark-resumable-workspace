# Test strategy

Every test lives in the tree of the project it tests, in that project's own conventions, so that it
can be reviewed and run by that project's maintainers with no extra tooling. Two harnesses remain
outside for now, for reasons stated at the end.

## Spark

| Suite | Location | Covers |
|---|---|---|
| `RecoveryTaskCommitSuite` | `sql/core/src/test/scala/.../v2/` | envelope round trip, identity binding, corruption/truncation/trailing bytes, deterministic manifest, invalid connector contracts |
| `RecoveryTaskCommitCompatibilitySuite` | same | golden-file gate on the version 1 envelope and manifest layouts; regenerate with `SPARK_GENERATE_GOLDEN_FILES=1`, fixture in `sql/core/src/test/resources/recovery/` |
| `BatchWriteRecoverySuite` | same | five driver-replacement scenarios end to end against a real immutable CAS store |
| `RecoveryKeyDiscriminationSuite` | `sql/core/src/test/scala/.../adaptive/` | that the material Spark hands a provider can actually discriminate two different stages |
| `SparkSessionExtensionSuite` (recovery tests) | `sql/core/src/test/scala/org/apache/spark/sql/` | 19 provider-level tests: adoption before map submission, fail-closed rollback, anchors, write IDs, partition skipping, preflight, discard |
| `DAGSchedulerSuite`, `MapOutputTrackerSuite` | `core/src/test/scala/...` | scheduler-side recovery installation and statistics |
| `RecoveryTaskCommitBenchmark` | `sql/core/src/test/scala/.../benchmark/` | durable bytes per partition, encode/decode throughput, the driver-side batched load |

Run one suite the way Spark runs any suite:

```bash
./build/mvn -pl sql/core -Dtest=none \
  -DwildcardSuites=org.apache.spark.sql.execution.datasources.v2.BatchWriteRecoverySuite test
```

### What `BatchWriteRecoverySuite` models, and what it does not

It runs a write in one `SparkSession`, discards that session entirely, and runs the same write again
in a new session that shares only durable state on disk. No scheduler state, catalog, or in-memory
commit message crosses that boundary — which is the boundary the protocol cares about.

It does **not** kill a JVM. A suite cannot kill its own process, so a genuine `SIGKILL` mid-write is
covered by the development harness below and, ultimately, by an integration test.

Two implementation details that are easy to get wrong and are therefore worth knowing:

- The store's CAS is `Files.createLink`, not `Files.move(ATOMIC_MOVE)`. `rename(2)` replaces the
  target, and the JDK's existence check without `REPLACE_EXISTING` is a separate `stat` — racy. A
  store that silently replaced would make every scenario pass for the wrong reason.
- The session master is `local[2, 4]`, not `local[2]`. A plain `local[N]` allows exactly one task
  attempt, so the lost-reply scenario would never reach the executor preflight it exists to test.

## Celeborn

`CelebornDriverRecoverySuite` in `tests/spark-it` is the two-driver integration test, written in
that module's conventions (`AnyFunSuite with SparkTestBase`, mini cluster from `MiniClusterFeature`,
`ShuffleClient.reset()` between tests). It runs the workload in one `SparkSession`, stops it, waits
out the ownership lease, and runs the same workload in a second session that shares only Celeborn's
durable state — then asserts that at least one stage completed having run **zero** tasks and that
the result matches the uninterrupted run. A second test asserts that a different recovery identity
adopts nothing. The lease is 5s in the test for the reason described in `RETENTION-AND-SIZING.md`:
takeover cannot happen while the previous lease is valid.

48 further recovery test cases across 8 suites already live in the Celeborn tree (lease transitions and
fencing, snapshot survival, task-commit integrity and first-writer-wins, identity ambiguity,
aggregate bounds, anchor immutability, catalog validation and replay, application-loss cleanup, HA
state-machine replay, worker lease store, client binding). The two-driver integration test belongs in
the existing `tests/spark-it` module.

**Celeborn does not compile on JDK 25** (`sun.misc.Signal` in `common/util/SignalUtils.scala`), so
its runs pin JDK 21 — see `verification/run-celeborn-suites.sh`. A first pass under JDK 25 produced
eight failures with zero tests executed, which says nothing about the code.

## Iceberg

`TestSnapshotIdempotency` in `core/` has no Spark dependency and runs today under JDK 21
(`verification/run-iceberg-ledger.sh`). The Spark-side tests (`TestSparkWriteRecovery`,
`TestSparkTable`) are blocked on which Spark version the Iceberg module targets — see
`PATCH-SPLIT-PLAN.md`.

## Running everything

```bash
verification/run-verification-queue.sh      # one serialized 1-core pass over every gate
```

Results land in `verification/logs/results.tsv` as `<gate>\t<PASS|FAIL>\t<detail>`, with full output
per gate beside it. `VERIFICATION-STATUS.md` is written from that file and nothing else.

## The two harnesses still outside a project tree

`dev-harnesses/write-recovery-process-harness/` kills a real driver JVM at an exact protocol point;
`dev-harnesses/celeborn-cluster-harness/` starts a real 3-master HA ensemble and runs two driver
processes against it. Both are development scaffolding, not merge material, and both have a stated
merge-path equivalent in `dev-harnesses/README.md`.
