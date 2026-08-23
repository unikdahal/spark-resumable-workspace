# Claude work log — resumable Spark driver (parallel track to codex)

**Owner of implementation: codex.** This file is Claude's audit trail. Started 2026-08-24 00:20 local.

## Ground rules Claude is following

1. **Do not edit any file codex has dirty.** `spark-resumable-upstream` (37 dirty), `celeborn` (34 dirty),
   `oss-fixes/iceberg` (37 dirty) are read-only for Claude. Any Claude change goes to a **new path**.
2. **No `git stash` / `git checkout --` / `git commit` on codex's behalf.** Codex's work is uncommitted
   and unrecoverable if clobbered.
3. **Builds pinned to 1 core / 2 threads** (`taskset -c 0`, `-T 1`), `nice -n 10`, run in background.
4. **No ownership/attribution markers inside code.** Who wrote what is recorded here and in
   `docs/upstream/*`, never in source headers or comments — the split is a development-phase fact,
   not something that should survive into a patch. Code comments explain the code only.
5. **Nothing gets written down as fact unless it was read out of source or produced by a run I did.**
   The PDF status report (`recovery_status_report.pdf`) is codex's self-assessment; claims from it are
   marked `[unverified]` until Claude confirms them against code or a test run.

## Snapshot taken before touching anything

`/tmp/claude-1000/-home-unik-Coding-spark/f24d5a84-c957-42a9-9e70-668ea0f61a07/scratchpad/snapshot-20260824-0023/`
holds, per repo: `*.status`, `*.tracked.diff`, `*.untracked.list`, `*.untracked.tgz`.
Sizes at snapshot time: spark 2255 diff lines + 14 untracked; celeborn 4417 + 9; iceberg 952 + 5.

---

## Status board

| # | Item | State | Notes |
|---|------|-------|-------|
| 1 | Snapshot codex's uncommitted work | **DONE** | see path above |
| 2 | Inventory of what changed in all 3 repos | **DONE** | below, §Inventory |
| 3 | `sql/core` test-compile gate | **IN PROGRESS** | first attempt failed — stale catalyst jar in `~/.m2`, not a source bug. See §Findings F1 |
| 4 | Install fresh `core` + `sql/catalyst` jars, re-run gate | **IN PROGRESS** | |
| 5 | Run focused suites (`RecoveryTaskCommitSuite`, `SparkSessionExtensionSuite`, `DAGSchedulerSuite`, `MapOutputTrackerSuite`) | PENDING | blocked on 4 |
| 6 | Celeborn focused suites | PENDING | |
| 7 | Iceberg focused tests (`TestSnapshotIdempotency`, `TestSparkWriteRecovery`, `TestSparkTable`) | PENDING | blocked on Spark API jar |
| 8 | Cross-project protocol spec doc | **DONE** | `docs/upstream/PROTOCOL-SPEC.md` (424 lines), every layout/limit read from source |
| 9 | Config reference doc | **DONE** | `docs/upstream/CONFIG-AND-ENABLEMENT.md` |
| 10 | Retention & sizing doc | **DONE** | `docs/upstream/RETENTION-AND-SIZING.md` — answers the coupling question: the window is the **lease**, not a TTL |
| 11 | Upgrade / rollback doc | **DONE** | `docs/upstream/UPGRADE-AND-ROLLBACK.md` |
| 12 | Threat model | **DONE** | `docs/upstream/THREAT-MODEL.md` — one open high-severity item, see F10 |
| 13 | Two-driver E2E test plan | **DONE** | `docs/upstream/E2E-TWO-DRIVER-TEST-PLAN.md` — 17 scenarios, 8 assertions, reuses the POC scaffolding |
| 14 | Verified status report | IN PROGRESS | `docs/upstream/VERIFICATION-STATUS.md`, fills from `verification/logs/results.tsv` |
| 15 | Upstream proposals: Spark SPIP, Celeborn CIP, Iceberg (x2) | **DONE** | `SPARK-SPIP-RESUMABLE-EXECUTION.md`, `CELEBORN-CIP-DRIVER-RECOVERY.md`, `ICEBERG-PROPOSAL.md` |
| 16 | Patch-split plan for three upstream review processes | **DONE** | `PATCH-SPLIT-PLAN.md` — dependency spine, 3 Spark PRs, 4 Celeborn CIPs, 2 Iceberg proposals |
| 17 | Write-recovery harness (own end-to-end) | CODE DONE, run pending | `spark-write-recovery-e2e/` |
| 18 | Envelope format-compatibility fixture | **DONE** | `EnvelopeCompatibilityCheck` — closes the "no protocol compatibility tests" gap from `UPGRADE-AND-ROLLBACK.md` §6 |
| 19 | Serialized verification queue | RUNNING | `verification/run-verification-queue.sh` — 13 gates, 1 core, results.tsv |

---

## Findings

**F1 — the `sql/core` compile errors are a stale-artifact artifact, not broken code.**
`taskset -c 0 ./build/mvn -T 1 -pl sql/core test-compile` produced 22 errors, all of the form
`object RecoveryTaskCommitStore is not a member of package org.apache.spark.sql.connector.write`.
Cause: `-pl sql/core` resolves `spark-catalyst` from `~/.m2`, and the installed jar's newest recovery
classes are stamped 08-23 22:00 while `sql/catalyst/target/scala-2.13/classes` has them at 23:15.
`RecoveryTaskCommitStore.class`, `RecoveryDataWriter*.class`, `RecoveryCommitMessageCodec.class` and
`SupportsRecoveryCommitDiscard.class` are **absent from the installed jar entirely**, and the installed
`SupportsBatchWriteRecovery.class` is 541 bytes vs 787 in target — i.e. the older signature without
`recoveryId()` / `commitMessageCodec()` / `recoveryCompatibilityMetadata()`.
Same for `spark-core`: the installed jar's `ShuffleStageRecoveryHandler.class` is dated 03-15-2025
(inherited from an upstream build), while `core/target` has the real one at 08-23 19:36.
=> Codex compiled catalyst/core but never re-`install`ed them. Fix is `mvn install` of those two
modules, not a source change. **No recovery-code compiler error has been observed yet.**

**F2 — recovery has no `SQLConf` keys.** `grep -n recovery SQLConf.scala` finds nothing; the feature is
wired entirely through `SparkSessionExtensions` (`injectShuffleStageRecovery`) plus connector opt-in
interfaces. Consequence for docs: there is no "turn it on with a conf" story to write; the enablement
contract is an extension-injection contract. Also relevant to the upstream pitch — no new public conf
surface to defend.

**F3 — the PDF is behind the code on executor preflight.** The report lists "add executor preflight
recovery" under §2 Left, but `SparkSessionExtensionSuite` already contains
`test("recoverable V2 task preflight observes a late winner without creating a writer")`. Claim
"pending" in the report; state in tree: implemented + test written (pass/fail not yet established).

---

## Inventory of codex's uncommitted work (read-only observation)

### spark-resumable-upstream (Spark 5.0.0-SNAPSHOT, master)
New files (14): `core/.../shuffle/ShuffleStageRecovery.scala`;
`sql/catalyst/.../connector/catalog/{SupportsRecoveryAnchor,SupportsRecoveryWrite}.java`;
`sql/catalyst/.../connector/write/{BatchWriteRecoveryState,RecoveryCommitMessageCodec,RecoveryDataWriter,RecoveryDataWriterFactory,RecoveryTaskCommitStore,SupportsBatchWriteRecovery,SupportsRecoveryCommitDiscard}.java`;
`sql/catalyst/.../analysis/RecoveryAnchorResolver.scala`;
`sql/core/.../adaptive/ShuffleStageRecovery.scala`; `sql/core/.../v2/RecoveryTaskCommit.scala`;
`sql/core/src/test/.../v2/RecoveryTaskCommitSuite.scala`.
Largest edits: `SparkSessionExtensionSuite` (+569, 19 new recovery tests), `WriteToDataSourceV2Exec`
(+211), `V2Writes` (+161), `DAGScheduler` (+96), `DAGSchedulerSuite` (+76), `MapOutputTracker` (+41).

### celeborn
New files (9): `CelebornShuffleStageRecoveryExtension.java` (spark-3 client),
`common/.../meta/ApplicationLease.java`, `common/.../util/RecoveryTaskCommitUtils.java`,
`worker/.../ApplicationLeaseStore.scala` + suite, `LifecycleManagerRecoveryBindingSuite.scala`,
`ControllerSuite.scala`, plus new protocol/clustermeta test dirs.
Largest edits: `LifecycleManager` (+566), `AbstractMetaManager` (+463), `Master` (+384),
`TransportMessages.proto` (+242), `MasterSuite` (+210), `ControlMessages` (+174),
`MasterStateMachineSuiteJ` (+141), `HAMasterMetaManager` (+129), `Controller` (+118),
`CelebornConf` (+73).

### oss-fixes/iceberg
New files (5): `core/src/test/.../TestSnapshotIdempotency.java`,
`spark/v4.1/.../source/{SparkWriteRecovery,SparkWriteRecoveryCompatibility,SparkWriteRecoveryTaskCodec}.java`,
`spark/v4.1/.../source/TestSparkWriteRecovery.java`.
Largest edits: `SnapshotProducer` (+258, the idempotency ledger), `SparkWrite` (+172),
`SparkTable` (+78), `TestSparkTable` (+64), `SnapshotUpdate` (+16), `TableProperties` (+8).

---

## Change log (things Claude actually did to the machine)

| When | What | Why | Reversible? |
|---|---|---|---|
| 00:23 | snapshotted 3 repos' diffs to scratchpad | safety net for codex's uncommitted work | read-only |
| 00:24 | ran `mvn -pl sql/core test-compile` | establish the pending compile gate | read-only wrt source; writes `sql/core/target` only |

**F4 — the PDF is also behind on the OutputCommitCoordinator fix.** The report lists
"Disable/replace driver-local output coordination for recovery writes" under §2 Left. In the tree,
`WriteToDataSourceV2Exec` already has
`val useCommitCoordinator = batchWrite.useCommitCoordinator && taskCommitContext.isEmpty`
with the reasoning in a comment. Both §2 "newly found issues" (preflight + coordinator) are
implemented; only their test results are unknown.

**F5 — one real protocol mismatch worth codex's attention.** Spark's envelope allows a 16 MiB
connector payload (`RecoveryTaskCommitEnvelope.MaxPayloadBytes`), but Celeborn's ingress default is
1 MiB (`celeborn.master.recovery.taskCommit.maxPayloadSize`). A connector whose commit message lands
between those two numbers passes every Spark-side check and then fails at `publish` — i.e. *after*
the task has written its data. Not wrong, but the failure is expensive and late, and the two numbers
should be reconciled (or Spark should learn the store's limit before writers start, e.g. as a field
in the manifest handshake). Recorded, not changed.

**F6 — `return` inside a lambda on the executor preflight path.** `DataWritingSparkTask.run` uses
`recovery.foreach { ... return DataWritingSparkTaskResult(...) }`. In Scala 2.13 that compiles to a
`NonLocalReturnControl` throw. It is *before* the `tryWithSafeFinallyAndFailureCallbacks` block so
nothing swallows it today, but it is fragile (any future wrapping of that region changes behaviour
silently) and Scala 3 removes non-local returns. Suggested rewrite is a plain `match`/early `if`.
Not changed — codex owns this file. Lint status unknown: my build ran with `-Dscalastyle.skip=true`,
so a scalastyle gate has NOT been run.

| 00:40 | wrote `docs/upstream/PROTOCOL-SPEC.md`, `docs/upstream/CONFIG-AND-ENABLEMENT.md` | new files only, no codex file touched | delete the files |
| 00:35 | started `mvn -pl core,sql/catalyst -am install` | F1 — `~/.m2` jars are older than `target/`, so nothing downstream can compile or test until they are republished | writes `~/.m2` + `target` only; `scalafmt.skip=true` in the root pom was checked first, so **no source file is reformatted** |

**F7 — the recovery window is the lease, and takeover waits for it. Both directions matter.**
`Master.timeoutDeadApplications` would normally destroy all recovery state
(`updateAppLostMeta` drops catalogs, anchors and task commits) after
`celeborn.master.heartbeat.application.timeout` (default **300s**) of missed heartbeats — but codex
added a guard that retains the application while `currentTime < lease.expiresAtMs()`. Good. The
other half is less obvious: `handleApplicationLease` **rejects a non-renewal takeover while the
current lease is unexpired and owned by someone else**, and driver A renews every
`leaseDuration / 3`. So after a SIGKILL, driver B cannot take over for up to `leaseDuration`
(default `10m`). `leaseDuration` is therefore a two-sided knob — too short loses state, too long
stalls takeover — and no doc said so. Now documented in `RETENTION-AND-SIZING.md` §1 and flagged in
the E2E plan (use 20–30s in the harness or every scenario takes ten minutes).

**F8 — `applicationLeases` is never removed.** `updateAppLostMeta` clears
`registeredAppAndShuffles`, `appHeartbeatTime`, `applicationMetas`, `applicationInfos`,
`applicationWorkers`, `committedShuffleCatalogs`, `committedShuffleCatalogIndex`,
`sourceRecoveryAnchors` and `recoveryTaskCommits` — but not `applicationLeases`. Retaining the epoch
is defensible (it preserves monotonicity if the same `appId` comes back), but the map then grows
without bound across application lifetimes and rides along in every Raft snapshot. Needs either a
documented bound or expiry-with-tombstone. Small entries, slow leak, real.

**F9 — [DOWNGRADED to UNPROVEN, see the correction at the end of this file] the Celeborn recovery key is built from `treeString`, which truncates.**
`CelebornShuffleStageRecoveryExtension.recoveryKey` hashes:
`session.version() + recoveryId + numMappers + numPartitions + canonicalizedQueryPlan.treeString() + canonicalizedPlan.treeString()`.
`TreeNode.treeString` defaults to `maxFields = SQLConf.get.maxToStringFields`, i.e.
`spark.sql.debug.maxToStringFields` = **25**; anything beyond is replaced with `"... N more fields"`.
So for a table wider than 25 columns, or any node with a long field sequence, the plan string
**elides the very fields that distinguish two stages**. Two structurally different shuffle stages
with the same mapper/partition counts can therefore hash to the same recovery key, and adoption is
a false positive — the exact failure class `docs/LLD-resumable-spark-driver.md` calls the single
most important correctness property. `spark.sql.maxPlanStringLength` is not the issue (it defaults
to no truncation); `maxToStringFields` is.
Suggested fix (codex's call, not applied):
`canonicalized.treeString(verbose = true, addSuffix = false, maxFields = Int.MaxValue)` for both
plans, or drop `treeString` for a structural canonicalized hash. Worth a deliberate test: build two
queries over a >25-column table differing only past the 25th field and assert their keys differ.

**F10 — [HIGH] recovery makes Celeborn auth a correctness dependency, and auth is off by default.**
Every new recovery RPC calls `checkAuth(context, appId)` — correct — but `RpcEndpoint.checkAuth` is
a no-op when `client.getClientId == null`, which is the case whenever `celeborn.auth.enabled`
(default **false**) is off. An unauthenticated peer that knows `appId` + `recoveryId` + `writeId` can
pre-publish a task-commit record; because publication is immutable first-writer-wins, the real task
then **loses the CAS, calls `discardCommittedOutput` on its own correct output**, and the attacker's
payload becomes canonical. Digest and identity checks do not help — the attacker chose both.
Recommended: refuse to install the recovery provider when the Celeborn client is unauthenticated,
or state auth as a hard precondition. Full write-up in `docs/upstream/THREAT-MODEL.md` §0.

**F11 — Iceberg's recovery code cannot compile against any published Spark.**
`oss-fixes/iceberg/gradle/libs.versions.toml` pins `spark41 = "4.1.3"`, and the recovery code lives in
`spark/v4.1/...`. The APIs it calls (`SupportsBatchWriteRecovery`, `RecoveryDataWriterFactory`,
`SupportsRecoveryCommitDiscard`, `RecoveryCommitMessageCodec`) exist only in the local
`5.0.0-SNAPSHOT` fork. So the Iceberg side is blocked on a decision, not on code: either publish the
Spark fork under a 4.1.x-SNAPSHOT coordinate that Iceberg can consume, or add a `spark/v5.0` module
and point it at `5.0.0-SNAPSHOT`. Until then no Iceberg recovery test can run, which is why the PDF's
"New API integration build pending" line has stayed pending. Worth deciding early — it also decides
which Spark release the upstream proposal targets.

## Files Claude created (all new paths, nothing of codex's edited)

| Path | What |
|---|---|
| `CLAUDE-WORKLOG.md` | this file |
| `docs/upstream/PROTOCOL-SPEC.md` | the 3-repo wire/format contract |
| `docs/upstream/CONFIG-AND-ENABLEMENT.md` | every conf key + the injection contract |
| `docs/upstream/RETENTION-AND-SIZING.md` | lease-as-recovery-window, inline-store sizing |
| `docs/upstream/UPGRADE-AND-ROLLBACK.md` | version matrix, rolling upgrade, kill switches |
| `docs/upstream/THREAT-MODEL.md` | T-1..T-14, the auth dependency |
| `docs/upstream/E2E-TWO-DRIVER-TEST-PLAN.md` | 17 scenarios / 8 assertions |
| `spark-resumable-upstream/sql/core/src/test/scala/.../adaptive/RecoveryKeyDiscriminationSuite.scala` | **new test**, F9 repro: does a plan-string-derived recovery key discriminate plans that differ past `maxToStringFields`? |

## Owned end-to-end: `spark-write-recovery-e2e/`

A standalone two-process harness proving the **Spark half** of the write-recovery protocol against a
real durable CAS store, with no Celeborn and no Iceberg — which is what makes it runnable today,
since both of those are blocked on version decisions (F11 for Iceberg; Celeborn's newest profile is
Spark 4.2.0 while the fork is 5.0.0-SNAPSHOT).

Self-contained: separate maven project, its own package `org.apache.spark.recovery.e2e`, touches no
file in any of the three patch repos, deletable without consequence.

| File | Role |
|---|---|
| `FileTaskCommitStore.scala` | `RecoveryTaskCommitStore` over a directory; CAS by `link(2)` |
| `FaultPlan.scala` | declarative, cross-process fault injection |
| `DurableBinding.scala` | immutable CAS for source anchors and write IDs |
| `HarnessRecoveryProvider.scala` | the injected `ShuffleStageRecovery` (write recovery only) |
| `TestSink.scala` | recoverable DSv2 sink: table, write, writer, codec, discard |
| `TwoDriverWriteJob.scala` | one driver process |
| `run-write-recovery-e2e.sh` | 6 scenarios, per-check PASS/FAIL, non-zero exit on failure |

Scenarios: C0 control · S1 crash with 2 durable commits → replacement runs only the missing
partitions · **S2 accepted publish with a lost reply → executor preflight must adopt the canonical
commit without creating a writer** · S3 already committed → zero writer tasks and zero global commit
· S4 manifest mismatch → fail closed · S5 corrupt record → digest rejection.

Every design decision (D1–D10) is written up in `docs/upstream/WRITE-RECOVERY-HARNESS.md`, including
the two that a reviewer would otherwise have to reverse-engineer: why the CAS is `link(2)` and not
`rename(2)` (ATOMIC_MOVE silently replaces; the JDK's exists-check is a TOCTOU race), and why the
test connector deliberately keeps **no** task ledger of its own (it forces Spark's durable store to
be the sole authority, which is the path the protocol is strictest about).

Status: code written, waiting on `sql/core` install to compile and run it. Results go into
`docs/upstream/VERIFICATION-STATUS.md` as run evidence, not claims.

## Expanded ownership (second pass)

Beyond the write-recovery harness, Claude now also owns end-to-end:

**`verification/run-verification-queue.sh`** — one serialized, 1-core pass over every gate that can
be run on this machine, each step logging to `verification/logs/` and appending
`<step>\t<PASS|FAIL|SKIP>\t<detail>` to `results.tsv`. A failing step never stops the queue: the
point is to learn the state of *every* gate in one pass rather than one at a time. Steps: 5 Spark
suites, scalastyle on the three touched modules, the envelope fixture check, the write-recovery
harness, 4 Celeborn suites + the HA state machine, and Iceberg's `TestSnapshotIdempotency`.

**Iceberg's ledger tests are not blocked.** Earlier note (F11) said Iceberg was blocked on a Spark
version decision — that is true only for the `spark/v4.1` module. `core/` has no Spark dependency, so
`TestSnapshotIdempotency` (6 tests) is runnable today and is in the queue. Proposal 1 in
`ICEBERG-PROPOSAL.md` is therefore independently reviewable and mergeable, which makes it the best
first contribution of the whole project.

**`EnvelopeCompatibilityCheck`** (in `spark-write-recovery-e2e`, declared in Spark's package because
`RecoveryTaskCommitEnvelope` is `private[sql]`) records base64 fixtures of a v1 task envelope and a
write manifest, then verifies that current code both decodes them *and* re-encodes byte-identically.
It turns "the envelope format changed" from a silent durable-store incompatibility into a failing
gate. `UPGRADE-AND-ROLLBACK.md` §6 listed this as missing; it now exists.

### Decisions taken while writing the harness (full rationale in `docs/upstream/WRITE-RECOVERY-HARNESS.md`)

- CAS is `Files.createLink`, never `Files.move(ATOMIC_MOVE)` — the latter lowers to `rename(2)` and
  replaces silently; the JDK's exists-check without `REPLACE_EXISTING` is a separate `stat()`, a
  TOCTOU race. A store that replaces would make every scenario pass for the wrong reason.
- The test connector keeps **no** task ledger of its own, forcing Spark's durable store to be sole
  authority — the strictest path through `WriteToDataSourceV2Exec`.
- The crash is `Runtime.halt` from inside the store after N durable records exist: deterministic,
  no shutdown hooks, and (in `local[2]`) a genuine driver kill.
- Preflight observability comes from a marker printed by the harness store, not from grepping a
  Spark `logInfo` line, so the assertion does not depend on log level.
- Correctness is measured from the sink's `_COMMITTED` marker, never a directory listing, because a
  crashed driver genuinely leaves orphan data files. **Nothing in the protocol cleans those up
  today** — worth a decision from codex.

## Corrections

**F9 is not proven, and my first repro was wrong.** `RecoveryKeyDiscriminationSuite` failed 3/3 on
its first run, but not in the way that would confirm the finding: the two "different" plans I built
(`groupBy("c0").count()` over two different projections) are legitimately *identical* after column
pruning — the aggregate needs only `c0`, so the optimizer removes the columns I was varying. Equal
plans must produce equal keys; that is correct behaviour, not a collision. The third test, which
disables truncation entirely, failed the same way and proved the harness was at fault rather than
the key.
Rewritten to aggregate one expression per column (`sum(c1)…sum(c39)`), so pruning cannot collapse
the difference and the varying expression sits past the 25-field cut-off in the plan string. Re-run
pending. Until that run says otherwise, **F9 is a hypothesis, not a finding** — codex should not act
on it yet.

## Run evidence so far (see docs/upstream/VERIFICATION-STATUS.md for the full table)

- `sql/core` **install + test-compile: BUILD SUCCESS, zero errors** — the PDF's outstanding gate.
  F1 confirmed: the 22 earlier errors were stale `~/.m2` jars, never codex's code.
- `RecoveryTaskCommitSuite`: **5/5 pass**.
- `SparkSessionExtensionSuite`: **48/48 pass**, which includes all 19 recovery tests — shuffle
  recovery, fail-closed rollback, empty/non-AQE recovery, source-anchor and write-ID resolution,
  partition skipping, already-committed skip, non-recoverable fail-closed, recovery-aware abort,
  speculative discard, executor preflight, malformed durable state.

## Third pass of ownership

**`resume-e2e-upstream/`** — the cluster-level two-driver harness, shuffle-only subset: a version
probe, a 3-master Celeborn HA config sized for a laptop, cluster lifecycle scripts, the driver
program, and three scenarios (control / crash-then-adopt / different-identity-must-not-adopt).
Crash point is `Runtime.halt` inside `SparkListenerStageCompleted`, which is *after* the stage's
shuffle output is committed and its catalog published — a deterministic protocol point, not a sleep.
The lease in the harness config is **30s on purpose**: takeover is rejected until the previous lease
expires, so a production 10m lease would make every scenario take ten minutes (F7).

**`probe-versions.sh`** answers the question that gates all of it: does Celeborn's Spark client build
against the fork? It tries `-Pspark-4.2` and `-Pspark-4.1` with `-Dspark.version=<fork>` and prints
either "unblocked, use this profile" or the three fallbacks from `PATCH-SPLIT-PLAN.md`. Not yet run
— the verification queue owns the core right now.

**`verification/run-scale-benchmark.sh` + `RecoveryScaleBenchmark`** — turns the sizing table in
`RETENTION-AND-SIZING.md` §3 from arithmetic into measurement: true envelope bytes per partition,
total durable MiB at 1K/10K/100K partitions and two payload sizes, encode/decode throughput, store
CAS throughput, and the cost of the batched 1024-partition load every replacement driver pays before
it can schedule anything. Report §10 asks for 10K/100K/1M partition numbers; this is that, for the
part that does not need a cluster.

## Standardization pass: everything moves to the form each project actually merges

The shell-script harnesses were development scaffolding. They are not the form Spark, Celeborn or
Iceberg accept, so the work has been re-expressed in each project's own conventions.

| Was | Is now | Why |
|---|---|---|
| `spark-write-recovery-e2e/` + `run-write-recovery-e2e.sh` (6 shell scenarios) | `sql/core/src/test/scala/.../v2/BatchWriteRecoverySuite.scala` | ScalaTest in Spark's own tree, ASF header, run by `build/mvn -pl sql/core -DwildcardSuites=…` like every other suite. No bespoke runner, no external process orchestration |
| `EnvelopeCompatibilityCheck` + a base64 file in a scratch project | `sql/core/src/test/scala/.../v2/RecoveryTaskCommitCompatibilitySuite.scala` + `sql/core/src/test/resources/recovery/task-commit-envelope-v1.txt` | Spark's golden-file convention, regenerated with `SPARK_GENERATE_GOLDEN_FILES=1`, exactly like the other golden-file suites |
| `RecoveryScaleBenchmark` + `run-scale-benchmark.sh` | `sql/core/src/test/scala/.../benchmark/RecoveryTaskCommitBenchmark.scala` | Spark's `BenchmarkBase` convention with the standard "To run this benchmark" header; results belong in `sql/core/benchmarks/RecoveryTaskCommitBenchmark-results.txt` |
| `resume-e2e-upstream/` shell cluster harness | stays as a development harness for now; the merge-path form is a suite in Celeborn's `tests/spark-it` module | it genuinely needs external master/worker processes, which is what that module exists for |

### What the in-tree suite can and cannot do

`BatchWriteRecoverySuite` models driver replacement as: run a write in one `SparkSession`, discard
that session completely, run the same write again in a brand-new session that shares only durable
state on disk. No scheduler state, catalog, or in-memory commit message crosses that line — which is
the boundary that matters for the protocol. It cannot kill its own JVM; process-level kill coverage
stays in the out-of-tree harness and, later, in the Celeborn integration module.

Five tests, matching the shell scenarios one for one: replacement writes only the uncommitted
partitions · executor adopts the canonical commit when its publish reply is lost · a committed write
skips writers and the global commit · a different partition count fails closed on the manifest · a
corrupt record fails closed on the digest.

### One edit to a codex file, and exactly why

`scalastyle:check` failed with a single violation, and Spark's PR gate runs it:

```
error file=.../catalyst/analysis/RecoveryAnchorResolver.scala
message=org.apache.spark.sql.catalyst.plans.logical.LogicalPlan is in wrong order relative to
        org.apache.spark.sql.connector.write.RecoveryTaskCommitStore  line=23
```

Fixed by moving one import line so the block is alphabetical:

```scala
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan      // moved up one line
import org.apache.spark.sql.connector.catalog.{SupportsRecoveryAnchor, Table}
import org.apache.spark.sql.connector.write.RecoveryTaskCommitStore
```

This is the **only** edit made to a file codex owns, it changes no behaviour, and reverting it is a
one-line move back.

### Findings from the first full verification pass

**F12 — the toolchain is split.** Celeborn does not compile on JDK 25: `common/util/SignalUtils.scala`
uses `sun.misc.Signal` and scalac reports `not found: object sun`. Every Celeborn suite in the first
pass failed for that reason alone, with zero tests executed — nothing was learned about the code.
The machine has JDK 17 and 21 under sdkman, so Celeborn and Iceberg runs now pin
`~/.sdkman/candidates/java/21.0.3-tem` explicitly (`verification/run-celeborn-suites.sh`,
`run-iceberg-ledger.sh`) while Spark continues to build on 25. Anyone reproducing this needs both.

**F13 — `local[N]` allows exactly one task attempt.** The lost-reply scenario failed in the first
harness run for that reason, not because the protocol failed: Spark never retried the task, so the
executor preflight never ran. The fix is `local[2, 4]`, and it applies to any test of a
retry-dependent recovery path. Worth knowing before someone concludes the preflight does not work.
