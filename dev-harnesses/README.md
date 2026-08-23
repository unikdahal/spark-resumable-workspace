# Development harnesses

Neither of these is merge material. They exist because two things cannot be expressed inside a
project's own test suite, and both will be retired once their merge-path equivalents exist.

| Harness | Why it still exists | Merge-path equivalent |
|---|---|---|
| `write-recovery-process-harness/` | kills a real driver JVM with `Runtime.halt` at an exact protocol point (N durable task commits). An in-tree suite cannot kill its own JVM | `sql/core/src/test/scala/.../v2/BatchWriteRecoverySuite.scala`, which models replacement as a discarded `SparkSession` — everything except the process boundary |
| `celeborn-cluster-harness/` | starts a real 3-master Celeborn HA ensemble and workers, then runs two driver processes against it | a suite in Celeborn's `tests/spark-it` module, once the Spark-version question `probe-versions.sh` asks is answered |

Everything else that used to live out here has moved into the projects themselves:

- write-recovery scenarios → `BatchWriteRecoverySuite`
- envelope format fixtures → `RecoveryTaskCommitCompatibilitySuite` plus a golden file under
  `sql/core/src/test/resources/recovery/`
- durable-state sizing → `RecoveryTaskCommitBenchmark`, in Spark's `BenchmarkBase` form

If you are looking for the results of any of these, read `docs/upstream/VERIFICATION-STATUS.md`.
