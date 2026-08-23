# Two-driver end-to-end verification: the plan

This is the decisive missing proof. Everything else — 19 recovery tests in
`SparkSessionExtensionSuite`, 5 envelope tests, Celeborn's lease/CAS/HA suites, Iceberg's ledger
tests — verifies a component in isolation. None of it shows a **real second driver process skipping
real work after a real SIGKILL**.

## 0. Do not write this harness cold

`resume-poc-e2e/` already contains a working multi-process harness with a real Celeborn cluster, a
real Polaris REST catalog and MinIO in podman, and a kill-mid-flight test:

| Script | What it already does | Result recorded in its README |
|---|---|---|
| `run-e2e-demo.sh` | 2 query shapes, capture → adopt → adopt-cold, task-count deltas against the correct baseline | 9/9 |
| `run-e2e-kill-test.sh` | kills mid-flight, verifies fetch-failure recovery on an adopted AQE stage | 2/2, reproduced twice |
| `run-e2e-iceberg-test.sh` | Iceberg + Polaris REST + MinIO + Celeborn, including a source-mutation negative case | 10/10, reproduced twice |

Reusable as-is: the podman MinIO/Polaris bring-up (`run-e2e-iceberg-test.sh` lines ~79–125), the
Celeborn master/worker launch from `target/classes` + `dependency:build-classpath`, the
`wait_for_port` helper, the per-phase log capture with a grep-able prefix, and the trap-based
teardown.

**Not reusable:** the mechanism under test. Those scripts drive the *POC* (Spark 3.5.10-SNAPSHOT,
digest/anchor-store hooks, `E2EStageHook`). The upstream track is Spark 5.0.0-SNAPSHOT with the
`ShuffleStageRecovery` SPI and Celeborn's own recovery RPCs. So: **fork the scaffolding, replace the
driver program and the assertions.** Target layout:

```
resume-e2e-upstream/
  run-two-driver-test.sh        # phases + kill points, from run-e2e-iceberg-test.sh
  lib/cluster.sh                # celeborn HA (3 masters) + workers + podman polaris/minio
  lib/assert.sh                 # assertion helpers, one line per check
  src/.../TwoDriverJob.scala    # the job both drivers run, identical bytes
  scenarios/*.env               # one file per row of the failure matrix
```

## 1. Prerequisites

| Component | Build | Note |
|---|---|---|
| Spark | `spark-resumable-upstream`, `5.0.0-SNAPSHOT` | `mvn -pl core,sql/catalyst,sql/core -am install -DskipTests` (pin to 1 core) |
| Celeborn | `celeborn`, `1.0.0-SNAPSHOT` | needs the `spark-3` client module built too |
| Iceberg | `oss-fixes/iceberg`, Spark 4.1 module | **blocked**: builds against the finalized Spark API jar, which is `5.0.0-SNAPSHOT` here. Reconcile the Spark version before this phase can run |
| Catalog | Polaris via podman | already scripted |
| Storage | MinIO via podman | already scripted |

The Iceberg/Spark version mismatch (`spark/v4.1` module vs `5.0.0-SNAPSHOT` Spark) is the first thing
to resolve — it blocks every write-side scenario below. Shuffle-only scenarios (§3, S1–S4) can run
before it is resolved.

## 2. The canonical sequence

```
 phase 0  start 3 Celeborn masters (HA) + 2 workers + polaris + minio
 phase 1  create iceberg table T with >1 partition, populate source S (pinned snapshot)
 phase 2  driver A: spark.celeborn.driverRecovery.id=RUN-1
            reads S, one non-empty shuffle stage, writes T
          pause after: shuffle catalog published AND >=1 writer partition committed
 phase 3  SIGKILL driver A   (kill -9, not SIGTERM: no shutdown hooks)
 phase 4  leave executors / worker storage / catalog state as the scenario dictates
 phase 5  driver B: identical program, identical driverRecovery.id=RUN-1
 phase 6  assert
 phase 7  control run: same program, fresh recovery id, no kill
 phase 8  compare outputs byte-for-byte
```

### Assertions (each must be a separate, independently reported check)

| # | Assertion | How it is observed |
|---|---|---|
| A1 | Driver B acquires a **strictly higher** lease epoch | `PbApplicationLeaseControlResponse.epoch` in driver B's log |
| A2 | Completed shuffle stages submit **zero** map tasks | `SparkListenerTaskEnd` count per stage == 0 for adopted stages |
| A3 | Committed write partitions submit **zero** writer tasks | `partitionsToWrite.size` == total − committed |
| A4 | Only missing work runs | total task count of B < total of A + control delta |
| A5 | Exactly **one** Iceberg snapshot for the write | `table.snapshots()` count and the ledger entry's snapshot ID |
| A6 | Output equals the control run | full result comparison, not a count |
| A7 | No `abort()` after a successful writer commit | absence of the abort log line for committed partitions |
| A8 | Driver A's fenced writes are rejected if it revives | scenario S9 only |

A2 and A3 are the load-bearing ones: they are the difference between "recovery worked" and "the
query just ran again and produced the same answer". Count tasks; never infer from wall time.

## 3. The failure matrix

Each row is one scenario file. `KILL_AT` names a deterministic kill point; the harness needs an
injection hook (an env-var-driven `System.exit(137)` inside the job, or a `SparkListener` that kills
the JVM on a named event) rather than a sleep-and-hope.

| # | Scenario | `KILL_AT` | Expected |
|---|---|---|---|
| S1 | Crash **before** shuffle catalog publication | after last map task, before `onStageCompleted` | B recomputes the stage; no adoption; correct result |
| S2 | Crash **after** shuffle catalog publication | after publish, before next stage | B adopts, zero map tasks (A2) |
| S3 | Multiple recovered shuffles with AQE stage reuse | after 2 stages published | both adopted; AQE coalescing still applies |
| S4 | Partial worker loss + probe | kill 1 of 2 workers before B starts | probe fails closed → recompute, not corrupt read |
| S5 | Crash **before** task CAS acceptance | inside `DataWriter.commit()`, before `publish` | B re-runs that partition |
| S6 | CAS accepted but RPC reply lost | drop the response in a test store wrapper | B's executor **preflight** finds the canonical commit and creates no writer |
| S7 | Crash **during** global commit | inside `SnapshotProducer.commit`, after metadata write | B finds the ledger entry, does not double-commit (A5) |
| S8 | Global commit succeeds, response lost | fail the catalog reply after commit | same as S7 |
| S9 | Old executor publishes after takeover | resume a paused executor of A after B takes the lease | publish rejected as fenced |
| S10 | Celeborn leader change during publish | `kill -9` the leader master mid-phase-2 | publish either fails cleanly or is replayed exactly once |
| S11 | Source snapshot expiry attempt | run `expireSnapshots` between phases 3 and 5 | B **fails closed**; must not fall forward to a newer snapshot |
| S12 | Ledger expiry boundary | set `commit.idempotency.retention-ms=1s` | documented failure, not a double commit |
| S13 | Speculation / task retries | `spark.speculation=true` | exactly one canonical winner; loser calls `discardCommittedOutput` |
| S14 | Empty input / zero-file commit | empty source partition | recovery of an empty write is a no-op, not an error |
| S15 | Concurrent unrelated commit to T | second writer during phase 2 | Iceberg retry converges; ledger unaffected |
| S16 | Envelope corruption in the store | flip a byte in a stored record | decode fails closed with a digest error |
| S17 | Lease expiry before B starts | delay B beyond `leaseDuration` | state released; B recomputes; **no** partial adoption |

S6, S9, S11 and S13 are the four that justify the design; if only four can be run, run those.

## 4. Two harness facts that will bite

**Takeover waits for the old lease to expire.** `Master.handleApplicationLease` rejects a
non-renewal takeover while `nowMs < currentLease.expiresAtMs()` and the owner differs. Driver A
renews every `leaseDuration / 3`, so after a SIGKILL the lease survives up to `leaseDuration`
(default `10m`). **Set `spark.celeborn.driverRecovery.leaseDuration` to something small — 20–30s —
in the harness**, or every scenario takes ten minutes and S17 becomes untestable in practice. Also
verify the state-retention side: state is held while the lease is valid *or* heartbeats continue, so
driver B's own heartbeat (same `spark.celeborn.client.application.uniqueId`) keeps state alive once
it starts.

**Nothing may be shared between the two drivers except durable state.** The two runs must be separate
JVMs launched from the same jar with the same conf, differing in nothing. A harness that reuses a
`SparkSession`, or that passes anything from A to B in a file, proves nothing.

## 5. Reporting

One line per check, machine-greppable, e.g. `E2E-UPSTREAM: [S6][A3] PASS zero writer tasks for 3/4
partitions`. Scenario summary at the end with a non-zero exit on any failure. The POC scripts already
use exactly this pattern (`RESUME-POC-E2E:` prefix) and it made their results auditable after the
fact — keep it.

---

## Implementation status

`resume-e2e-upstream/` implements the shuffle-only subset of this plan against a real Celeborn HA
ensemble. It is a separate project; it modifies nothing in any patch repo.

| Piece | State |
|---|---|
| `probe-versions.sh` | **done** — answers the blocking question: does Celeborn's Spark client build against the fork, under `spark-4.2` or `spark-4.1` with `-Dspark.version=<fork>`? Prints a verdict and the recommended fallbacks |
| `conf/celeborn-defaults.conf` | **done** — 3-master HA ensemble, laptop-sized, **30s lease** deliberately (takeover is rejected until the previous lease expires, so a production 10m lease makes every scenario take ten minutes) |
| `lib/cluster.sh` | **done** — builds Celeborn, assembles master/worker classpaths, starts 3 masters + N workers, tears down on exit |
| `TwoDriverShuffleJob.scala` | **done** — one driver process; deterministic crash at `Runtime.halt` inside `onStageCompleted`, which is *after* the stage's shuffle output is committed and its catalog published |
| `run-two-driver-shuffle-test.sh` | **done** — C0 control, S2 crash-then-adopt, S3 different identity must not adopt |
| Write-side scenarios (S5–S8, S12, S14, S15) | blocked on the Iceberg/Spark version decision; the store-only subset already runs in `spark-write-recovery-e2e` |
| S4, S9–S11, S13, S16, S17 | not implemented |

### Why the crash point is `onStageCompleted`

A `sleep`-based kill proves nothing reproducible. `SparkListenerStageCompleted` fires after the
stage's map outputs are registered and, for a Celeborn-backed shuffle, after `StageEnd` has committed
every worker file and the catalog has been published. Halting inside that callback is therefore a
crash at an exact protocol point: everything the replacement driver should be able to adopt is
durable, and nothing later has happened yet.

### The measurement

`E2E-STAGE-COMPLETED stage=<id> tasks=<n>`, one line per stage, plus a final
`E2E-RESULT SUCCESS rows=<n> stageTasks=<stage:count,…>`. An adopted stage is one that completes with
`tasks=0`. Task counts, never wall time.
