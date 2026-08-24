# Low-Level Design — Resumable Spark Driver on Spark 3.5.8 with Apache Celeborn

**Source of truth for every citation in this document:** `apache/spark` tag `v3.5.8`
(`5a48a37b2dbd7b51e3640cd1d947438459556cc6`) and `apache/celeborn` `main`
(`00113daf3a5d2476aaa04a8b576152893d377fda`), read directly. Behavioural claims about the JVM were
executed on JDK 21.0.11. Line numbers refer to those trees.

---

## Table of contents

1. [Scope and problem statement](#1-scope-and-problem-statement)
2. [Architecture](#2-architecture)
3. [Identity and verification](#3-identity-and-verification)
4. [Dynamic partition pruning](#4-dynamic-partition-pruning)
5. [The hard cases](#5-the-hard-cases)
6. [Adoption mechanics](#6-adoption-mechanics)
7. [State model, store, fencing](#7-state-model-store-fencing)
8. [Restore lifecycle](#8-restore-lifecycle)
9. [Feature flags](#9-feature-flags)
10. [Edge case register](#10-edge-case-register)
11. [Stack interactions](#11-stack-interactions)
12. [Failure mode analysis](#12-failure-mode-analysis)
13. [Test plan](#13-test-plan)
14. [Patch inventory and rollout](#14-patch-inventory-and-rollout)
15. [Metrics and operability](#15-metrics-and-operability)
16. [Scoping](#16-scoping)
17. [Invariants](#17-invariants)

---

## 1. Scope and problem statement

### 1.1 Goal

A Spark driver dies five hours into a six-hour query. Today the restarted driver recomputes all six
hours. The goal is that it recomputes only what it must: in-flight tasks, unverifiable stages, and
stages whose inputs demonstrably changed.

### 1.2 What actually dies and what survives

| Component | On driver death | Consequence |
|---|---|---|
| Shuffle **data** on Celeborn workers | **survives** until app expiry | this is the entire opportunity |
| Celeborn `LifecycleManager` catalog | **dies** — it is driver-embedded (`SparkShuffleManager.java:158`) and holds no persistence (`grep -c 'recover\|persist' LifecycleManager.scala` → 0) | the bytes exist, nothing can name them |
| `MapOutputTrackerMaster.shuffleStatuses` | dies | scheduler cannot know a stage finished |
| `DAGScheduler` stage/job state | dies | — |
| `AdaptiveSparkPlanExec` stage cache and `resultOption` | dies | AQE replans from scratch |
| Broadcast blocks in the driver `BlockManager` | die | every broadcast rebuilds |
| `OutputCommitCoordinator.stageStates` | dies (`OutputCommitCoordinator.scala:73`, plain `mutable.Map`) | commit authority is ambiguous across a restart |
| Executor JVMs | survive briefly, then self-terminate on driver disconnect | executor pool must re-form |
| Query-scoped constants (folded timestamps, resolved random seeds) | die | see §3.3 — this is why more things are "nondeterministic" than need to be |

The asymmetry — **data survives, metadata dies** — is the whole design space. There is no need to
persist a single byte of shuffle payload. The task is to rebuild a *catalog* that can point at
surviving bytes, and to *prove* those bytes are still the right answer to the question being asked.

### 1.3 Non-goals

- **Zero rework.** In-flight tasks are lost. Recovery is minutes, not zero.
- **Standby driver / hot failover.** Out of scope; this is restart-and-resume, not HA.
- **Structured Streaming.** It has a checkpoint mechanism with stronger semantics already (§5.8).
- **Surviving a Spark or connector upgrade mid-query.** Refused explicitly, loudly (§3.6, P7).
- **Cross-query result caching.** Adjacent and tempting; deliberately excluded (§16.3).

### 1.4 Threat model

The feature introduces exactly one new failure class: **a stage is skipped whose output would have
differed had it run.** Everything in this document exists to make that class unreachable rather than
unlikely. Concretely, four adversaries:

1. **The mutating table.** Someone commits to, compacts, or expires snapshots on an input table
   between crash and restart.
2. **The drifting plan.** Statistics, configs, or executor counts differ on the second planning pass,
   so the regenerated plan is not the plan whose output you hold.
3. **The zombie driver.** The old driver was paused or partitioned, not dead, and resumes writing.
4. **The lying store.** Redis outlives, evicts, or partially serves state that Celeborn no longer backs.

### 1.5 The governing invariant

> **Losing state is safe. Wrong state is catastrophic.**

Losing a record costs a recompute — correct answer, wasted CPU. Inventing one costs a silently wrong
answer. Every ambiguous decision in this document resolves toward dropping the anchor. A design that
adopts nothing is useless; a design that adopts one thing it should not is worse than not existing.

---

## 2. Architecture

### 2.1 Principle: regenerate the plan, adopt the bytes

There are two ways to make a restarted driver skip completed work.

**Restore the plan.** Serialise `(stage.rdd, stage.shuffleDep)`, rehydrate them in the new driver,
rebind the transient `SparkContext`, and pin ID counters so restored identifiers stay valid.

**Adopt the shuffle.** Let the new driver re-run `main()` and rebuild the plan *naturally*, taking
fresh RDD/shuffle/stage IDs. At each exchange, fingerprint the subtree, look it up, and on a hit
register the surviving map outputs under the *new* shuffle ID and alias it onto the surviving
Celeborn shuffle.

This design takes the second path. The first is not merely harder; it is unsound on 3.5.8 for
mechanical reasons.

`Dependency.scala:78-209` — `ShuffleDependency` performs four constructor side effects:

```scala
val shuffleId: Int = _rdd.context.newShuffleId()                                  // :100
val shuffleHandle: ShuffleHandle =
  _rdd.context.env.shuffleManager.registerShuffle(shuffleId, this)                // :102
private[this] val numPartitions = rdd.partitions.length                           // :105
_rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))              // :209
```

**Java deserialisation does not run constructors.** A `ShuffleDependency` rehydrated from a blob
therefore has:

| Field / effect | Post-restore state | Consequence |
|---|---|---|
| `@transient private val _rdd` | `null` | `dep.rdd` throws; `DAGScheduler.getOrCreateShuffleMapStage` calls it |
| `val shuffleHandle` | deserialised, **stale** | for Celeborn this is a `CelebornShuffleHandle` carrying `lifecycleManagerHost` and `lifecycleManagerPort` (`CelebornShuffleHandle.scala:24-33`) — the **dead driver's** RPC endpoint. Every task built from it dials a corpse. |
| `registerShuffle` on the ShuffleManager | never happened | the new `LifecycleManager` has no record of the shuffle |
| `registerShuffleForCleanup` | never happened | restored shuffles are never cleanup-registered and leak on the workers permanently |

Repairing all four means reflective writes into `final` instance fields of Spark and Celeborn
internals on every dependency in the tree, plus a Java-serialisation compatibility surface that
breaks on any minor version bump. And the failure mode when a repair is missed is a stale plan
producing wrong output — not an exception.

Adoption has none of these problems because the constructor runs normally. Its failure mode when a
check is missed is a fingerprint miss, which recomputes.

### 2.2 Layers

```
L5  Restore coordinator     phase ordering, fencing, preconditions, kill switches
L4  SQL anchors             AQE stage anchors, partition specs, DPP subquery results,
                            broadcast payloads, query-scoped constants
L3  Input anchors           what each leaf actually read — the correctness boundary
L2  Adoption                MapOutputTracker seeding + Celeborn shuffle aliasing
L1  Anchor store            fenced, schema-versioned, hot-path only
```

L1–L2 alone give nothing useful. L1–L4 is the shippable unit. L5 is the only layer that touches
scheduler control flow, and it does so strictly before the first job is submitted.

### 2.3 Chain of custody

The property that makes this composable, and that is easy to miss:

> **Each adopted anchor pins the inputs to the next planning decision.**

AQE's decisions downstream of a stage are functions of that stage's `MapOutputStatistics`. If the
statistics are restored verbatim rather than recomputed, the coalescing, skew-splitting and
join-strategy decisions taken above it are reproducible; therefore the *fingerprints* of the
exchanges above it are reproducible; therefore their anchors can hit.

Determinism propagates **up** the DAG from pinned leaves through pinned statistics. This is why
partition specs and `MapOutputStatistics` are mandatory anchor fields rather than optimisations
(§10.C6), and why a single rejected anchor low in the plan quietly poisons the hit rate above it
even when nothing is wrong up there. It is also the reason the acceptance ladder rejects
transitively (§3.6, invariant V-2).

---

## 3. Identity and verification

### 3.1 `PlanDigest`

The resume key must be stable across JVMs and hosts, sensitive to every planning difference that can
change output bytes, and insensitive to everything else.

**Do not use `plan.canonicalized.hashCode()`.** Two reasons, both disqualifying:

*Stability.* `BatchScanExec` — the DSv2 path, which is the Iceberg path — overrides:

```scala
override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)   // BatchScanExec.scala:57
@transient lazy val batch: Batch = if (scan == null) null else scan.toBatch
```

The fingerprint delegates to a connector-supplied object. If that object inherits identity hashing,
the key is JVM-instance-dependent and every anchor misses on every restart. That is safe and
completely useless, and it is invisible in a test that uses `spark.range(...)`, whose leaves are
pure case classes.

*Width.* 32 bits collides at roughly 77 000 distinct anchors by the birthday bound. A platform
running thousands of concurrent queries against one store reaches that.

```
PlanDigest = SHA-256(
      schemaVersion
   || structuralString(plan.canonicalized)      // leaves replaced by their input-anchor digest
   || sortedLeafAnchorDigests
   || configDigest
   || queryConstantsDigest                      // §3.3
)  truncated to 128 bits
```

`structuralString` is a deterministic pre-order walk emitting, per node: the structural path, node
name, simple class name, and the canonicalized form of each expression. It **must not** call
`toString` on connector `Scan`/`Batch`/`Table` objects, `Broadcast`, `RDD`, `Future`, or anything
transitively holding a `SparkContext`. Leaves are replaced wholesale by their input-anchor digest:
the connector's own opinion of its identity is never trusted.

Fields added by this feature (pinned splits, injected results) must be excluded from
`structuralString`, from `equals`, and from `hashCode`, exactly as `@transient scan` already is —
otherwise the act of adopting changes the digest that authorised the adoption.

### 3.2 Input anchors and pin strength

Every anchor carries an `InputAnchor` per leaf beneath it. An anchor is reusable only if every leaf
re-verifies.

```
InputAnchor
  leafId          structural path within the canonicalized subtree — never an object identity
  kind            V1_FILE | V2_VERSIONED | V2_GENERIC | INMEMORY | RANGE | ONE_ROW
  identity        table identifier / relation identity / root paths
  pinning         PINNED_SNAPSHOT | PINNED_SPLITS | PINNED_FILELIST | UNPINNABLE
  snapshotToken   Iceberg snapshot-id / Delta version / Hudi instant
  splitManifest   serialized Seq[Seq[InputPartition]] or Array[PartitionedFile]   (§5.2)
  fileDigest      SHA-256 over sorted (path, length, modTime, partitionValues)
  schemaDigest    resolved read schema + name mapping + partition spec id
  pruningDigest   SHA-256 over sorted, type-tagged DPP pruning key values           (§4)
  numFiles, totalBytes, numSplits          cheap tripwires, checked before the digest
```

Four strengths, in preference order:

| Strength | Applies to | Restore behaviour | Guarantee |
|---|---|---|---|
| **PINNED_SNAPSHOT** | any connector that accepts a read version (Iceberg, Delta, Hudi) | re-plan with the recorded version **forced**, then verify `fileDigest` | byte-identical inputs *by construction*, even against a concurrently written table |
| **PINNED_SPLITS** | any DSv2 source; v1 file sources | replay the captured splits, bypassing connector re-planning entirely (§5.2) | identical splits by construction; residual risk is file deletion, which fails loudly |
| **PINNED_FILELIST** | v1 file sources, non-versioned v2 | re-list, recompute `fileDigest`, compare | detects drift; cannot prevent it |
| **UNPINNABLE** | streaming sources, sockets, leaves whose identity cannot be established | anchor never written | fail closed |

`PINNED_SNAPSHOT` is the only strength that *prevents* divergence rather than detecting it. That
distinction matters most for DPP (§4.3) and it is why `requirePinnedSnapshot` defaults to true.

### 3.3 Query-scoped constants

A large fraction of what looks like nondeterminism is actually **a constant chosen once per planning
pass**. Spark assigns these during analysis and optimisation, then folds them into the plan:

`Analyzer.scala:3243-3255`
```scala
object ResolveRandomSeed extends Rule[LogicalPlan] {
  private lazy val random = new Random()
  ...
  case e: ExpressionWithRandomSeed if e.seedExpression == UnresolvedSeed =>
    e.withNewSeed(random.nextLong())
}
```

`finishAnalysis.scala:110-132`
```scala
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val instant = Instant.now()
    val currentTime = Literal.create(instantToMicros(instant), TimestampType)
    ...
    case CurrentTimestamp() | Now() => currentTime
    case cd: CurrentDate => Literal.create(microsToDays(currentTimestampMicros, cd.zoneId), DateType)
    case CurrentTimeZone() => timezone
```

So `rand()`, `randn()`, `shuffle()`, `uuid()`, `current_timestamp()`, `now()`, `current_date()` and
`current_timezone()` are **not** re-evaluated per row or per task — they are constants baked into
the plan at planning time. Within one driver's lifetime they are perfectly deterministic. They break
across a restart only because the *second planning pass picks different constants*.

**Therefore: capture them and replay them.** The capture is a small map:

```
QueryConstants
  currentTimestampMicros   Long     from ComputeCurrentTime
  sessionLocalTimeZone     String
  randomSeeds              Map[exprPath -> Long]     from ResolveRandomSeed
```

Replay is a post-analysis rule that substitutes the captured values before optimisation completes.
The effect is disproportionate: a query using `current_timestamp()` as a load-stamp column — which
is the overwhelmingly common pattern in ELT/CTAS workloads — moves from *permanently unanchorable*
to *fully anchorable*, and moves from producing a **different value on restart** to producing the
same one, which is a correctness improvement independent of resume.

`queryConstantsDigest` enters `PlanDigest` so that a failure to replay is a miss, not a silent
divergence. Constants that cannot be captured (a nondeterministic UDF, `input_file_name()` in an
unpinned scan, `spark_partition_id()` above a repartition) remain in the indeterminate lattice (§3.5).

### 3.4 Config anchoring

Any configuration read during planning that changes physical shape must be in the digest.

```
spark.sql.shuffle.partitions                spark.sql.adaptive.*                      (all)
spark.sql.autoBroadcastJoinThreshold        spark.sql.optimizer.dynamicPartitionPruning.*
spark.sql.files.maxPartitionBytes           spark.sql.files.openCostInBytes
spark.sql.files.minPartitionNum             spark.sql.sources.v2.bucketing.*
spark.sql.join.preferSortMergeJoin          spark.sql.cbo.*
spark.sql.codegen.wholeStage                spark.sql.legacy.*                        (all set keys)
spark.sql.session.timeZone                  spark.sql.parquet.*                       (filter pushdown)
spark.shuffle.minNumPartitionsToHighlyCompress                                         (see below)
spark.default.parallelism                   spark.comet.*                             (§11.1)
+ every key the session set via SET, from SQLConf.getAllConfs at capture time
```

Two entries earn specific comment.

`spark.shuffle.minNumPartitionsToHighlyCompress` (`config/package.scala:1627`, default 2000) decides
whether a `MapStatus` is a `CompressedMapStatus` with per-partition sizes or a
`HighlyCompressedMapStatus` with an average above a threshold. That choice feeds the accuracy of
`MapOutputStatistics`, which feeds every downstream AQE decision. A restart that crosses the
threshold plans differently.

`spark.sql.adaptive.coalescePartitions.parallelismFirst` (`CoalesceShufflePartitions.scala:57-61`):

```scala
val minNumPartitions = conf.getConf(COALESCE_PARTITIONS_MIN_PARTITION_NUM).getOrElse {
  if (conf.getConf(COALESCE_PARTITIONS_PARALLELISM_FIRST)) session.sparkContext.defaultParallelism
  else 1
}
```

`defaultParallelism` tracks **currently registered executors**. On a restart, executors register
gradually, so planning at t+5 s with 30 executors coalesces differently than at t+60 s with 200. The
digest therefore records the *effective* value, not the configured one, and the restore lifecycle
blocks on executor registration before any planning begins (§8, phase 2). The digest is the backstop
when both fail.

### 3.5 The determinism lattice

Classify every candidate subtree before writing an anchor:

```
DETERMINATE                every leaf pinnable; every expression deterministic or a replayable
                           query-scoped constant; no order-sensitive repartitioning; RDD
                           outputDeterministicLevel == DETERMINATE

CONDITIONALLY_DETERMINATE  order-sensitive but reproducible IF nothing beneath it recomputes:
                           RoundRobinPartitioning / repartition(n), rand() with a replayed seed,
                           shuffle(), local sort without a total order, first()/last() aggregates

INDETERMINATE              nondeterministic UDF, non-replayable external state, streaming source,
                           input_file_name() over an unpinned scan

UNKNOWN                    anything the classifier cannot prove  -> treated as INDETERMINATE
```

The middle class is the one that unlocks real workloads. `repartition(200)` is classified
indeterminate by Spark because *recomputation* can reassign rows to different partitions
(`DAGScheduler.scala:1489, 1802, 2064-2076, 2240-2301` roll back indeterminate stages on fetch
failure and abort the job when they cannot). But adoption is precisely the act of *not*
recomputing. A wholly adopted round-robin stage emits the same bytes it emitted before, because
they are the same bytes.

The safety condition is a closure property, not a node property:

> **Rule D-2 (indeterminacy closure).** For any `CONDITIONALLY_DETERMINATE` stage `S`, the adoption
> decision must be **uniform** across the entire set of anchors that transitively depend on `S`, and
> `S` itself must be adopted **atomically** — every map output present, zero partial recompute.

The failure this forbids is subtle and worth stating explicitly. Partial adoption of a round-robin
stage means some partitions hold pre-crash assignments and the recomputed ones hold new
assignments; rows are duplicated and dropped with no error anywhere. And *non-uniform* adoption
across two consumers of the same indeterminate stage — a self-join over a repartitioned relation
where one side is adopted and the other recomputed — produces two mutually inconsistent views of
one relation. Implemented as: compute the dependency closure of every conditionally-determinate
stage, and reject the entire closure if any member is rejected.

`INDETERMINATE` and `UNKNOWN` are never anchored.

### 3.6 The acceptance ladder

An anchor is adopted only if **all** of the following hold, evaluated in this order because each is
cheaper than the next:

```
1  schemaVersion == runtime schemaVersion            else SCHEMA_MISMATCH
2  determinism class is adoptable (§3.5)             else INDETERMINATE
3  no ancestor and no closure member rejected        else ANCESTOR_REJECTED       (V-2)
4  cheap tripwires match: numFiles, totalBytes,
   numSplits, numMaps, numReduces                    else INPUT_DRIFT
5  PlanDigest == recomputed digest                   else DIGEST_MISS
6  every InputAnchor re-verifies at its strength     else INPUT_DRIFT             (this is DPP)
7  Celeborn confirms every celebornShuffleId live    else CELEBORN_EXPIRED
8  Celeborn does not classify it as a skew or child
   shuffle                                           else SKEW_SHUFFLE            (§5.5)
```

Failure at any rung drops that anchor and, by rung 3 on the next pass, everything downstream and
everything in its indeterminacy closure. There is no path from a failed check to a reused byte.

Rung 7 exists because **the store is never the authority; Celeborn is.** Redis TTLs can outlive the
data they describe.

---

## 4. Dynamic partition pruning

DPP is the case most likely to produce a silently wrong answer, because it makes the *set of files a
scan reads* a runtime value. Reuse a shuffle built from one file set while the scan now reads
another, and nothing throws.

### 4.1 The three shapes

`PlanDynamicPruningFilters.scala:46-88` rewrites every `DynamicPruningSubquery` into exactly one of:

| # | Condition | Result | Treatment |
|---|---|---|---|
| **D1** | `canReuseExchange` — a `BroadcastHashJoinExec` already broadcasts the same build side | `DynamicPruningExpression(InSubqueryExec(value, SubqueryBroadcastExec(...), exprId))` | anchorable |
| **D2** | `onlyInBroadcast`, no reusable exchange | `DynamicPruningExpression(Literal.TrueLiteral)` | inert — no pruning occurred |
| **D3** | otherwise | `DynamicPruningExpression(InSubquery(value, ListQuery(Aggregate(...))))`, planned to `InSubqueryExec` over `SubqueryExec` | anchorable |

D1 and D3 both terminate in an `InSubqueryExec`. That is the single injection point for all of DPP.

Which shape you get depends on `conf.exchangeReuseEnabled` and on whether the join planned as a
broadcast hash join — which under AQE depends on runtime statistics. A restart that plans D3 where
the original planned D1 yields a structurally different subtree, hence a different `PlanDigest`,
hence a miss. Correct, merely lossy. Pinning `MapOutputStatistics` (§2.3) and the join-strategy
configs (§3.4) is what keeps that from happening on every restart.

### 4.2 The injection point

`sql/core/.../execution/subquery.scala:112-171`:

```scala
case class InSubqueryExec(
    child: Expression, plan: BaseSubqueryExec, exprId: ExprId,
    shouldBroadcast: Boolean = false,
    private var resultBroadcast: Broadcast[Array[Any]] = null,
    @transient private var result: Array[Any] = null) extends ExecSubqueryExpression ... {

  def updateResult(): Unit = {
    val rows = plan.executeCollect()                        // the entire cost
    result = if (plan.output.length > 1) rows.asInstanceOf[Array[Any]]
             else rows.map(_.get(0, child.dataType))
    if (shouldBroadcast) resultBroadcast = plan.session.sparkContext.broadcast(result)
  }

  def values(): Option[Array[Any]] = Option(result)         // "used only by DPP"

  override lazy val canonicalized: InSubqueryExec =
    copy(child = child.canonicalized, plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
         exprId = ExprId(0), resultBroadcast = null, result = null)
}
```

Three properties fall out, all favourable:

1. **`result` is a plain `var`.** Setting it is the whole of DPP restore — one field write, exactly
   analogous to AQE's single `resultOption.set` at `AdaptiveSparkPlanExec.scala:327`. No futures, no
   broadcast, no exchange.
2. **`canonicalized` already nulls `result` and zeroes `exprId`.** Spark itself defines the identity
   of a DPP subquery independently of its value. That is the anchor key, free, with the same
   provenance as AQE's `stageCache` key.
3. **The payload is small.** `result` is the distinct values of one join key on the dimension side —
   hundreds to low thousands of scalars for a normal star schema. Kilobytes.

The patch:

```scala
private[sql] def hasResult: Boolean = result != null

/** Adopt a persisted pruning-key set instead of executing the subquery. */
private[sql] def injectResult(values: Array[Any]): Unit = {
  require(result == null, "InSubqueryExec result already set")
  result = values
}
```

The `require` is not defensive decoration; it is the enforcement mechanism for §4.5.

### 4.3 Why pinning the keys is necessary and not sufficient

Two consumer shapes, and neither is honest about what it read.

**v1 file sources** (`DataSourceScanExec.scala:251-296`):

```scala
@transient lazy val selectedPartitions: Array[PartitionDirectory] = { ...listFiles(static filters)... }

@transient protected lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
  val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)
  if (dynamicPartitionFilters.nonEmpty) {
    val predicate = dynamicPartitionFilters.reduce(And)
    selectedPartitions.filter(p => boundPredicate.eval(p.values))                 // :280
  } else selectedPartitions
}
```

Pinning `result` makes `boundPredicate` identical. But `selectedPartitions` is a **live catalog
listing**. An identical predicate over a mutated table is still a different scan.

**DSv2** (`datasources/v2/BatchScanExec.scala:60-112`) — the Iceberg path, and worse:

```scala
val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
filterableScan.filter(dataSourceFilters.toArray)      // MUTATES the connector's Scan in place
val newPartitions = scan.toBatch.planInputPartitions()
```

The pruned partition set is produced **inside the connector**, from the connector's own snapshot
resolution. Spark can neither observe, reconstruct, nor constrain it. Pinning the DPP keys
constrains the connector's *input*; it says nothing about its *output*.

> **Therefore the correctness boundary sits on what the leaf resolved to, not on what was fed to it.**

That boundary is the `InputAnchor` (§3.2). With `PINNED_SNAPSHOT` you are not hoping the DPP keys
reproduce — you are forcing the scan back onto exactly the table state the reused shuffle was built
from, so a divergent DPP result cannot change the bytes underneath it. With `PINNED_SPLITS` (§5.2)
you skip the connector's planner entirely and replay the exact splits. With `PINNED_FILELIST` you
only detect, which on a busy table means a hit rate near zero — which is itself the argument for the
first two.

### 4.4 What the pruning digest is for

Given that the input anchor is the real boundary, `pruningDigest` is still worth computing, for two
reasons. It is a **cheap early rung** (rung 4/6 of the ladder) that rejects the common case before
you pay for a file digest. And it is the **only** signal in the `PINNED_SPLITS` case, where the
splits are replayed and would otherwise mask a genuine semantic change in the dimension table.

The digest is over sorted, type-tagged values, so ordering never matters and a `Long` 1 never
collides with a `String` "1".

### 4.5 Ordering

`selectedPartitions`, `dynamicallySelectedPartitions`, `filteredPartitions`, `inputRDD` and
`InSubqueryExec.inSet` are all `lazy val`. Once forced, frozen.

> **Invariant DPP-1.** Every `injectResult` for a subtree completes **before** any node in that
> subtree is prepared, executed, or has `computeStats` / `inputRDD` / `selectedPartitions` /
> `filteredPartitions` touched.

Practically the hook sits in `SparkPlan.prepareSubqueries` in front of `updateResult()`, and in
`AdaptiveSparkPlanExec` before `getFinalPhysicalPlan`'s prepare phase:

```scala
runningSubqueries.foreach { sub =>
  if (!resumeHook.exists(_.tryInject(sub))) sub.updateResult()
}
```

Violating DPP-1 does not corrupt data — the injected value is ignored and the subquery simply runs.
It silently converts every DPP-bearing query into a full recompute. That is exactly the class of bug
that survives code review and lives in production for a year, which is why `injectResult` throws
rather than logging: a mis-ordered hook fails the test suite loudly instead of degrading quietly.

### 4.6 Residual DPP risk

- **D2** needs nothing; no pruning happened.
- **Connectors implementing `SupportsRuntimeV2Filtering` with neither a read version nor
  serialisable splits** are `UNPINNABLE`. There is no sound reuse over them. `requirePinnedSnapshot`
  refuses to arm.
- **Build sides containing non-replayable nondeterminism** are excluded by §3.5 before reaching an
  anchor.
- **A dimension table mutated such that pruning keys shrink** is caught by `pruningDigest`; one
  where they are unchanged but the *fact* table changed is caught by the fact leaf's anchor. Both
  rungs are required; neither subsumes the other.

---

## 5. The hard cases

This section covers the cases a naive design declares out of scope. Each has a mechanism.

### 5.1 Broadcast exchanges

**Why it looks impossible.** `Broadcast` blocks live in the dead driver's `BlockManager`. A
`TorrentBroadcast` restored into a new driver fetches blocks that no longer exist.

**What actually happens** (`BroadcastExchangeExec.scala:131-199`):

```scala
val (numRows, input) = child.executeCollectIterator()
...
val relation = mode.transform(input, Some(numRows))          // HashedRelation | Array[InternalRow]
...
val broadcasted = sparkContext.broadcastInternal(relation, serializedOnly = true)
```

The broadcast is a pure function of three things: the **collected rows**, the **`BroadcastMode`**,
and `numRows`. The mode is part of the plan, hence part of the digest. The rows are `UnsafeRow`s —
flat byte arrays. And their size is bounded by construction: `maxBroadcastRows` and
`MAX_BROADCAST_TABLE_BYTES` are hard ceilings, and in practice `autoBroadcastJoinThreshold` (default
10 MB) is the operative bound.

**Mechanism — payload anchoring.** Persist `input` as raw `UnsafeRow` bytes plus `numRows`. On
restore:

```scala
val rows  = deserializeUnsafeRows(anchor.payload, schema)
val rel   = mode.transform(rows.iterator, Some(anchor.numRows))
val bcast = sparkContext.broadcastInternal(rel, serializedOnly = true)
```

and set the stage's materialisation result to `bcast`. Persist the *rows*, not the built relation:
rows are mode-agnostic, so a plan that legitimately rebuilds with a different `BroadcastMode` (D1
versus D3 in §4.1) still hits, and you avoid depending on `HashedRelation`'s internal serialised
format across versions.

**Where to hook.** `relationFuture` is a `lazy val`, so a guard at its head covers both AQE and
non-AQE paths in one place — preferable to hooking only `BroadcastQueryStageExec`, which would leave
non-AQE plans and `SubqueryBroadcastExec` uncovered.

**What this buys.** A broadcast build side is frequently an entire dimension subtree: a scan, filters,
an aggregate, sometimes a join. Recomputing it costs whatever that subtree costs. Payload anchoring
replaces it with a bounded read plus a re-transform — from "recompute a subtree" to "read ten
megabytes."

**Caps and caveats.** Bound by `spark.resume.broadcast.maxPayloadBytes` (default 64 MB); above it,
fall back to recompute. Schema must match exactly, since `UnsafeRow` deserialisation is
schema-dependent and a mismatch is a memory-safety issue rather than a wrong-answer issue — so
verify `schemaDigest` before deserialising, and treat a mismatch as a hard reject, not a warning.
`SubqueryBroadcastExec` used purely for DPP needs no payload at all: §4.2 already persists the keys
it would have extracted.

### 5.2 Unpinnable scans — split capture

**Why it looks impossible.** A connector that exposes no read version cannot be asked to reproduce
its planning.

**The lever.** `sql/catalyst/.../connector/read/InputPartition.java:38`:

```java
public interface InputPartition extends Serializable {
```

`InputPartition` is **required by contract** to be serialisable, because Spark ships it to
executors. So the exact splits the crashed driver planned can be captured and replayed. The
connector's planner is bypassed rather than re-invoked.

**Mechanism.** Capture `filteredPartitions: Seq[Seq[InputPartition]]` after DPP filtering. On
restore, inject it into `BatchScanExec` through an optional field excluded from `equals`,
`hashCode`, and `structuralString` (§3.1) — the same treatment `@transient scan` already receives.
The v1 analogue is `Array[PartitionDirectory]` / `PartitionedFile`.

This converts `UNPINNABLE` to `PINNED_SPLITS` for essentially every DSv2 source, and it is
*stronger* than `PINNED_FILELIST`: instead of detecting that the listing changed, it makes the
listing irrelevant.

**Four real costs, and how each is bounded.**

1. **Size.** A serialised `InputPartition` for Iceberg carries a whole `ScanTaskGroup` — file paths,
   positions, delete files. On a 200 000-file scan this is not kilobytes. Bound with
   `spark.resume.scan.maxSplitBytes` (default 64 MB) and degrade to `PINNED_FILELIST` or
   `UNPINNABLE` above it. Compress; the payload is highly repetitive path text.
2. **Version fragility.** These are *connector-internal* serialised objects. An Iceberg upgrade can
   break deserialisation. Therefore `schemaVersion` must include the **connector version**, not just
   the Spark version, and a mismatch is a hard refusal (§3.6 rung 1) — never a best-effort
   deserialisation attempt.
3. **Deleted files.** Compaction, `expire_snapshots`, or orphan cleanup can remove files the splits
   reference. This fails as `FileNotFoundException` at read time — loud, not silent, which puts it
   in the acceptable class. Add a pre-flight validation mode
   (`spark.resume.scan.validateSplits = exists | sample | none`, default `sample`) that checks a
   bounded random subset of paths before adopting, so the common case fails at restore time rather
   than an hour in.
4. **Bypassed connector logic.** Replaying splits skips whatever the connector would have done
   during planning — residual filter derivation, metrics, schema projection re-resolution. Mitigate
   by requiring `schemaDigest` equality and by preferring `PINNED_SNAPSHOT` whenever available;
   `PINNED_SPLITS` is the fallback for connectors that offer no version, not the default.

### 5.3 Indeterminate stages

Covered structurally in §3.5. The three mechanisms, in order of how much they unlock:

1. **Query-scoped constant replay** (§3.3) moves `rand()`, `uuid()`, `current_timestamp()`,
   `current_date()` out of the indeterminate class entirely. This is the largest single win because
   `current_timestamp()` as an audit column is near-universal in ELT workloads.
2. **Atomic adoption** makes `repartition(n)` and friends safe: the bytes are not recomputed, so
   they cannot differ. Enforced by requiring every map output present — a single missing map output
   in a conditionally-determinate stage rejects the whole anchor rather than triggering a partial
   recompute.
3. **Closure uniformity** (Rule D-2) prevents the cross-branch inconsistency where one consumer of
   an indeterminate relation is adopted and another recomputed.

What remains genuinely indeterminate: nondeterministic UDFs (including Python UDFs whose behaviour
depends on worker state), reads of external mutable services, and anything the classifier cannot
prove. These are never anchored, and `spark.resume.determinism.strict` (default true) ensures
`UNKNOWN` lands here rather than in the adoptable set.

### 5.4 Result stages and writes

The genuinely dangerous case, because the failure is a **duplicated or lost commit**, and because
`OutputCommitCoordinator.stageStates` (`OutputCommitCoordinator.scala:73`) is an in-memory
`mutable.Map` that dies with the driver, leaving commit authority ambiguous across a restart.

**The lever.** `sql/catalyst/.../connector/write/WriterCommitMessage.java:36`:

```java
public interface WriterCommitMessage extends Serializable {}
```

`V2TableWriteExec` collects one message per partition at the driver and then commits them as a set:

```scala
sparkContext.runJob(rdd, writeTask, rdd.partitions.indices,
  (index, result: DataWritingSparkTaskResult) => { messages(index) = result.writerCommitMessage; ... })
batchWrite.commit(messages)
```

**Mechanism — commit-message anchoring.** Persist each `WriterCommitMessage` as it arrives, keyed by
partition index under the write's digest. On restore, submit only the missing indices, merge with
the persisted set, and commit the union once.

This is sound only under three conditions, all of which must be checked, not assumed:

- **The writer is a DSv2 `BatchWrite` with an atomic commit.** Iceberg, Delta and Hudi qualify:
  the commit is a single optimistic metadata operation, and data files written by tasks whose
  messages were never persisted become unreferenced orphans that existing maintenance procedures
  reclaim. DSv1 `InsertIntoHadoopFsRelationCommand` with a `FileOutputCommitter` does **not**
  qualify — staging directories, `_temporary` layouts and job-ID-derived paths make partial
  reconstruction unsafe. Excluded by precondition.
- **File names are collision-free across attempts.** Iceberg uses UUID-bearing names, so a re-run
  partition cannot overwrite the file whose commit message you hold. Verify per connector; do not
  generalise.
- **The old driver cannot also commit.** Fencing (§7.4) plus the connector's own optimistic
  concurrency: a zombie's commit attempt fails on snapshot conflict rather than silently
  interleaving. Both layers required — the fence is not sufficient alone because the zombie may have
  begun its commit before the fence was taken.

`spark.resume.write.commitMessageAnchoring` defaults to **false**. It is the highest-value and
highest-risk capability here: it is the only thing that makes the *final* stage of a long ETL job
resumable, and it is the only place where a mistake writes wrong data to a table rather than merely
returning a wrong result.

**`collect()`-style result stages.** `ActiveJob.finished: Array[Boolean]` tracks partition
completion, and the values live in `JobWaiter`. For results under
`spark.resume.result.maxBytes` (default 64 MB, well under `spark.driver.maxResultSize`), persist
per-partition result bytes and prefill on restore. Above the cap, the result stage recomputes.
Cheap, bounded, and it covers the common "six-hour query ending in a small aggregate" shape.

### 5.5 Skew splits and Celeborn child shuffles

AQE's `OptimizeSkewedJoin` produces `PartialReducerPartitionSpec` and `PartialMapperPartitionSpec`
(`ShuffledRowRDD.scala:33-62`) that reference *ranges within* map outputs. Restoring correct
`MapStatus`es while recomputing different specs means downstream partitions read different byte
ranges of the same data — no error, wrong aggregates. Specs are therefore mandatory anchor state
(§10.C6), not an optimisation.

Celeborn has its own opinion here. `LifecycleManager.scala:1041-1045`:

```scala
if (determinate && !isBarrierStage && !isCelebornSkewShuffleOrChildShuffle(appShuffleId))
  shuffleIds.values.toSeq.reverse.find(e => e._2 == true)
else None
```

Celeborn explicitly refuses to reuse a shuffle it classifies as a **skew shuffle or a child of one**,
even in its own stage-rerun path. If Celeborn will not reuse it for a stage rerun, this design must
not reuse it for a driver restart. That is rung 8 of the acceptance ladder.

`PartialMapperPartitionSpec` also implies local shuffle reading, which assumes co-location with the
mapper's host — meaningless under a remote shuffle service. Assert that local shuffle reader is off
when Celeborn is the backend, and reject any anchor carrying those specs.

### 5.6 Non-AQE plans

Without AQE there are no query stages, so the `resultOption` hook does not exist. The path is
actually *simpler*: `ShuffleExchangeExec` creates its `ShuffleDependency`, the `DAGScheduler` builds
a `ShuffleMapStage` from it, and `findMissingPartitions` decides what runs. Seed the tracker for that
shuffle ID and the stage is skipped with no scheduler change at all.

Hook at `ShuffleExchangeExec.shuffleDependency` construction: compute the digest, and if anchored,
adopt into the freshly minted dependency's shuffle ID. Everything else — Celeborn aliasing, the
acceptance ladder, DPP injection — is unchanged.

This matters more than it appears, because AQE is disabled for some plans by
`InsertAdaptiveSparkPlan` regardless of configuration, and a design that only hooks AQE silently
does nothing for those.

### 5.7 In-flight tasks

Not solvable without task-level checkpointing, which is a different and much larger project. Bound
the loss instead:

- Anchor granularity is **per map output**, not per stage, so only genuinely running tasks are lost.
- Because per-map records are written as tasks complete, a stage that was 95% done resumes 95% done,
  provided the stage is not conditionally-determinate (§3.5, atomic adoption).
- Long tasks dominate the residual loss. If a single task runs for 40 minutes, a crash costs up to
  40 minutes regardless of anything here. That is an argument for partition sizing, not for this
  feature.

This, plus broadcast payload caps and rejected anchors, is where "minutes, not zero" comes from.
Any claim of zero rework is ignoring in-flight work.

### 5.8 Structured Streaming

Out of scope, and it should stay out. Streaming already has a checkpoint with stronger guarantees:
offsets and state stores are durable by design, and the recovery unit is a micro-batch, not a
shuffle. Bolting shuffle adoption onto it would create two overlapping recovery mechanisms with
different consistency models — a reliable way to produce a correctness bug at the seam. Streaming
sources are classified `UNPINNABLE` so the classifier refuses them structurally rather than by
convention.

---

## 6. Adoption mechanics

### 6.1 Spark side

For each accepted anchor, before the `DAGScheduler` sees the stage:

```scala
mapOutputTracker.registerShuffle(newShuffleId, numMaps, numReduces)      // MapOutputTracker.scala:803
anchor.mapStatuses.zipWithIndex.foreach { case (bytes, i) =>
  if (bytes != null) mapOutputTracker.registerMapOutput(newShuffleId, i, deserialize(bytes))  // :824
}
assert(mapOutputTracker.findMissingPartitions(newShuffleId).forall(_.isEmpty))
```

`MapOutputTrackerMaster` accepts `MapStatus` objects deserialised in a foreign JVM, after which
`findMissingPartitions` returns empty and the scheduler skips the work. That is the single
core-scheduler fact the entire design rests on, and it requires no scheduler change.

Keep the assertion at `MapOutputTracker.scala:337` loud:

```scala
assert(missing.size == numPartitions - _numAvailableMapOutputs,
  s"${missing.size} missing, expected ${numPartitions - _numAvailableMapOutputs}")
```

An inconsistent restore trips it. When it fires during development the temptation is to relax it;
that instinct is exactly backwards — it is the cheapest correctness tripwire in the system.

### 6.2 Celeborn side

`LifecycleManager.scala:109-115`:

```scala
// app shuffle id -> LinkedHashMap of (app shuffle identifier, (shuffle id, fetch status))
private val shuffleIdMapping = JavaUtils.newConcurrentHashMap[
    Int, scala.collection.mutable.LinkedHashMap[String, (Int, Boolean)]]()
private val celebornShuffleIdToAppShuffleIdMap = JavaUtils.newConcurrentHashMap[Int, Int]()
private val appShuffleDeterminateMap = JavaUtils.newConcurrentHashMap[Int, Boolean]()
```

`SparkCommonUtils.java:65-67`:

```java
return appShuffleId + "-" + context.stageId() + "-" + context.stageAttemptNumber();
```

This indirection already exists for stage rerun, and it is exactly the adoption hook: Celeborn
already maintains a mapping from Spark's shuffle identity to its own, and already tracks determinacy
per app shuffle. Nothing needs inventing; it needs to be **seedable** and **durable**.

The reader path is what decides everything (`LifecycleManager.scala:998-1022`):

```scala
val shuffleIds = if (isWriter) computeIfAbsent(...) else shuffleIdMapping.get(appShuffleId)
if (shuffleIds == null) {
  logWarning(s"unknown appShuffleId $appShuffleId, maybe no shuffle data for this shuffle")
  ... UNKNOWN_APP_SHUFFLE_ID ...
}
```

Seed `shuffleIdMapping[newSparkShuffleId] = LinkedHashMap(identifier -> (survivingCelebornShuffleId, true))`
before any task runs, and:

- a **reader** hits the mapping and reads the surviving files;
- a **writer** (a straggler retry) finds the identifier present and appends into the same Celeborn
  shuffle — which is Celeborn's existing determinate-stage-rerun semantics, not a new behaviour.

Required API:

```scala
def adoptShuffle(appShuffleId: Int, appShuffleIdentifier: String,
                 celebornShuffleId: Int, determinate: Boolean): Boolean
def exportShuffleCatalog(): PbShuffleCatalog     // shuffleIdMapping + determinacy + commit info
def confirmAlive(celebornShuffleIds: Seq[Int]): Set[Int]
def isSkewOrChildShuffle(appShuffleId: Int): Boolean          // exposes §5.5 rung 8
def unregisterSuperseded(celebornShuffleIds: Seq[Int]): Unit
```

`getShuffleIdMapping` already exists at `LifecycleManager.scala:2104`, so the read half is partly in
tree.

### 6.3 Hard precondition: the mapping layer must be on

`SparkUtils.java:143-165`:

```java
if (handle.stageRerunEnabled()) { ... client.getShuffleId(...) ... }
else                            { return handle.shuffleId(); }
```

With `celeborn.client.spark.stageRerun.enabled = false` (alias
`celeborn.client.spark.fetch.throwsFetchFailure`, `CelebornConf.scala:5246-5248`) **there is no
indirection at all** — the Celeborn shuffle ID is the raw Spark shuffle ID. Adoption would then
require the restarted driver to reissue identical Spark shuffle IDs in identical order, which
reintroduces exact-ID pinning and every fragility that comes with it.

The design refuses to arm without the flag. This single config check eliminates an entire class of
failure, and it is the most valuable precondition in the document.

### 6.4 Application identity

Adoption requires the restarted driver to present the **same** `appUniqueId` to Celeborn.
`SparkUtils.java:136-141`:

```java
public static String appUniqueId(SparkContext context) {
  return context.applicationAttemptId()
      .map(id -> context.applicationId() + "_" + id)
      .getOrElse(context::applicationId);
}
```

Two consequences that determine whether this feature can work on a given deployment at all:

- **The attempt-ID suffix is fatal where it exists.** On a cluster manager that increments an
  application attempt ID across driver restarts, `appUniqueId` changes on restart and adoption is
  structurally impossible. That is precisely the platform where driver restart is a first-class
  feature, which is an irony worth internalising before committing to a deployment target.
- **`spark.app.id` is an output, not an input.** `SparkContext.scala:603` does
  `_conf.set("spark.app.id", _applicationId)` — the context publishes the ID it was given. Pinning it
  by configuration is deployment-specific and must be verified on the actual platform, not assumed.

Also required: `celeborn.client.application.uuidSuffix.enabled = false`, or Celeborn appends a random
UUID (`CelebornConf.scala:978-984`) and the identity is lost on every start.

The plumbing to override this exists — `SparkUtils.fromSparkConf` maps any `spark.celeborn.*` key to
the corresponding `celeborn.*` key — but an operator-supplied stable identity is a small Celeborn
change, not a configuration trick. Treat it as part of the Celeborn workstream.

### 6.5 The restart budget

`CelebornConf.scala:2519-2526` — `celeborn.master.heartbeat.application.timeout` defaults to `300s`.
When the driver dies its `ApplicationHeartbeater` stops; after the timeout, workers reclaim the
app's files.

That is the entire budget: pod failure detection, rescheduling, image pull, JVM start, and
`main()` re-execution up to the first anchor lookup. Raise it for the resume window — 900 s is a
defensible ceiling — but never remove it, because it is also what reclaims storage for genuinely
dead applications. And never trust the anchor store over Celeborn: store TTLs can outlive the data
they describe, which is why rung 7 of the ladder exists.

---

## 7. State model, store, fencing

### 7.1 Anchor record

```
Anchor
  schemaVersion        string   spark version + connector versions + record schema     (§5.2)
  queryId              string   stable across restart, unique across runs              (§7.5)
  generation           long     fencing token
  kind                 SHUFFLE | BROADCAST | WRITE | RESULT
  planDigest           hex128   primary key
  parentDigests        [hex128] for transitive rejection
  closureId            hex128   indeterminacy closure membership                       (§3.5)
  determinism          DETERMINATE | CONDITIONALLY_DETERMINATE
  sparkShuffleId       int      pre-crash, informational only
  celebornShuffleId    int      the thing actually adopted
  numMaps, numReduces  int
  mapStatuses          bytes[]  per mapIndex, may be sparse
  mapOutputStatistics  bytes    restored verbatim -- pins downstream AQE decisions      (§2.3)
  partitionSpecs       bytes    mandatory, never optional                              (§10.C6)
  coalesceParams       map      effective minPartitionNum / advisorySize / parallelismFirst
  inputAnchors         []       per leaf                                               (§3.2)
  dppAnchors           []       subqueryDigest -> pruning keys                         (§4.2)
  broadcastPayload     bytes?   UnsafeRow bytes + numRows + schemaDigest                (§5.1)
  commitMessages       bytes[]? per partition, WRITE anchors only                      (§5.4)
  queryConstants       map      folded timestamps and resolved seeds                   (§3.3)
  epochAtCapture       long
  createdAtMs, ttl
```

### 7.2 Hot path only

There is no cold path. The plan is regenerated rather than restored, so there are no task-binary
blobs to store. What remains is per-map `MapStatus` bytes (~2 KB each) and the anchor record itself
(kilobytes, plus bounded broadcast payloads). Object storage is not in the design: one fewer single
point of failure, one fewer credential surface, one fewer schema to version.

Write cost must stay off the critical path. `EventLoop.scala:42` runs the `DAGScheduler` on a single
`eventThread`; blocking it stalls the entire scheduler. The writer is a bounded queue drained by a
daemon thread in pipelined batches, and **overflow drops rather than blocks** — consistent with the
governing invariant, since a dropped record costs a recompute.

### 7.3 Key layout

```
{qid}:gen                        String    generation, Lua-CAS guarded
{qid}:a:{planDigest}             Hash      anchor record fields
{qid}:a:{planDigest}:ms          Hash      mapIndex -> MapStatus bytes  (idempotent overwrite)
{qid}:a:{planDigest}:done        Bitmap    HINT ONLY -- never the source of truth
{qid}:a:{planDigest}:inval       Stream    XADD removals
{qid}:a:{planDigest}:cm          Hash      partitionIndex -> WriterCommitMessage bytes
{qid}:index                      Set       all planDigests for this query
```

Hash tags `{qid}` keep MULTI and Lua same-slot on Redis Cluster. `maxmemory-policy` must be
`noeviction`, checked at arm time via `CONFIG GET`.

The `:done` bitmap deserves its comment. It is a **hint**, never authority. Availability is derived
from `:ms` alone at read time. The reason is a specific eviction failure: if a partial eviction
removed half the `:ms` hash while `:done` survived intact, a design that trusted `:done` would
report a stage complete that is not. Deriving from `:ms` makes that failure a recompute instead of a
wrong answer — the same trade the whole document makes, applied to a cache-eviction corner.

### 7.4 Fencing

A paused or partitioned old driver that wakes and writes over a new driver's state is the canonical
split-brain, and it is not hypothetical on Kubernetes where a node can be unreachable long enough
for a replacement pod to complete a restore.

```
restore:      gen = INCR {qid}:gen
every write:  EVAL "if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end; <write>; return 1"
```

A losing write returns 0, and the writer **stops writing for the remainder of the query** rather
than retrying. A zombie that keeps retrying is a zombie that eventually wins a race.

Fencing the store is necessary and not sufficient. The zombie may still hold Celeborn: two live
`LifecycleManager`s for one `appUniqueId` is undefined behaviour. Before adopting, the coordinator
must confirm the old driver is gone — pod `Terminated` via the Kubernetes API, or expiry of the
Celeborn Master's registration. Confirm it; never race it.

### 7.5 Query identity and run identity

`queryId` must be **stable across restarts** (or adoption is impossible) and **unique across runs**
(or a previous failed run's anchors poison the current one). These pull in opposite directions and
the resolution is two fields: a stable `queryId` for adoption, and a monotonically increasing
`generation` for fencing, with anchors from generations older than the one being resumed treated as
candidates and anchors from *concurrent* generations rejected outright.

The re-submission case is worth calling out: a human re-running the same logical query as a brand-new
application must not adopt the previous application's anchors, because the previous application may
have been killed for producing wrong results. Adoption is scoped to a restart of the same
application, never a resubmission — enforced by tying `queryId` to the Celeborn `appUniqueId`, which
a new submission cannot forge.

### 7.6 Write ordering

> **Invariant W-1.** The Celeborn commit for a map output happens-before the anchor write that
> records it. Never inverted, never concurrent.

Because losing a record costs a recompute and inventing one costs a wrong answer, the writer may
batch and pipeline freely — as long as it never runs ahead of the commit.

---

## 8. Restore lifecycle

```
P0  ARM         read flags; evaluate preconditions; abort loudly on violation
P1  FENCE       gen = INCR {qid}:gen; confirm the previous driver is Terminated
P2  STABILISE   block until registered executors >= minRegisteredResourcesRatio (or maxWaitMs)
P3  EPOCH       advance MapOutputTracker epoch to >= capturedEpoch + 1
P4  LOAD        HSCAN anchors for {qid}; drop schemaVersion mismatches
P5  CONFIRM     Celeborn confirmAlive + isSkewOrChildShuffle; drop what it will not vouch for
P6  CONSTANTS   install captured query-scoped constants into the analyzer/optimizer  (§3.3)
P7  INSTALL     register the anchor hook on every AdaptiveSparkPlanExec context, on
                SparkPlan.prepareSubqueries, on BroadcastExchangeExec.relationFuture, and on
                ShuffleExchangeExec.shuffleDependency (non-AQE)
P8  RUN main()  user code re-executes; the plan rebuilds naturally
                per exchange:  digest -> ladder -> Celeborn adopt -> tracker seed ->
                               install partition specs -> set materialisation result -> stageCache
                per subquery:  digest -> injectResult, strictly before any scan lazy val forces
                per broadcast: digest -> rebuild relation from payload -> re-broadcast
P9  RECONCILE   pin strong refs to adopted dependencies; unregister superseded Celeborn mappings;
                emit the rejection histogram; release pins on query completion
```

Three orderings are load-bearing rather than cosmetic:

**P2 before P8.** `defaultParallelism` feeds `CoalesceShufflePartitions`, which changes the plan,
which changes the digest. Planning before executors settle guarantees a digest miss — a
correct-but-useless resume, and one that looks like the feature is broken rather than misconfigured.

**P3 before everything.** The epoch governs whether executors honour invalidations
(`MapOutputTracker.scala:533-534, 1497-1505`). A fresh driver starting at zero while surviving
infrastructure holds a higher epoch means invalidations are ignored.

**P5 before P8.** The tracker must never describe shuffles the backend has never heard of.

**P6 before P8** for a subtler reason: constants are folded during analysis, which happens the
instant user code builds a `DataFrame`. Install them before `main()` runs, not lazily on first use.

### 8.1 Identifier handling

Adoption needs no exact-ID restoration, because no restored object carries an old identifier into
the regenerated plan. Two counters want a **monotonic watermark**, not an absolute set:

| Counter | Why | Action |
|---|---|---|
| `MapOutputTracker.epoch` | executors compare received epochs against their highest-seen and clear caches on increase | `advanceAtLeast(capturedEpoch + 1)` |
| `SparkContext.nextShuffleId` | only in the degraded configuration where Celeborn's mapping layer is off (§6.3), which the preconditions forbid | `advanceAtLeast(maxAdoptedShuffleId + 1)` |

`nextRddId`, `nextJobId`, `nextStageId`, `AccumulatorContext.nextId`, `nextBroadcastId` and
`SQLExecution._nextExecutionId` need nothing at all.

A watermark cannot move a live counter backwards, so a mis-sequenced restore can only waste
identifiers, never reissue one:

```scala
def advanceAtLeast(ai: AtomicInteger, floor: Int): Unit = {
  var cur = ai.get()
  while (cur < floor && !ai.compareAndSet(cur, floor)) cur = ai.get()
}
```

A note on mechanism, because it is a place where a plausible-sounding conclusion is wrong. These
counters are declared `private val ... = new AtomicInteger(0)` (`SparkContext.scala:2713, 2717`),
which is `private final` in bytecode. That does **not** block restoration: the field is a final
*reference* to a *mutable* object, so mutating it involves no final-field write. Separately,
`Field.set` on a **non-static** final field of a non-hidden, non-record class is legal after
`setAccessible(true)` on every JDK through 21 — only *static* final is blocked. Both were verified
by execution on JDK 21.0.11. A `private[spark]` accessor is still worth landing upstream, because
reflecting into internals is not a thing to ship, but it is hygiene rather than a blocker.

---

## 9. Feature flags

Four levels, each independently killable: **arm**, **capture**, **restore**, **per-capability**.

### 9.1 Master switches

| Key | Default | Meaning |
|---|---|---|
| `spark.resume.enabled` | `false` | master kill switch; everything below is inert when false |
| `spark.resume.mode` | `off` | `off` \| `capture` \| `restore` \| `shadow` |
| `spark.resume.queryId` | required when enabled | stable across restart, tied to the Celeborn `appUniqueId` |
| `spark.resume.failFast` | `true` | precondition violation aborts at context init instead of degrading silently |

`shadow` is the mode that gets this into production safely. It captures normally and, on restart,
computes every digest and runs every rung of the acceptance ladder **while adopting nothing**, then
reports how many anchors *would* have hit and why the rest failed. You get hit rate and drift
characteristics with zero correctness exposure. Run shadow for a month before anyone enables
`restore`; the rejection histogram it produces is the only honest evidence that the feature would
ever pay for itself.

### 9.2 Preconditions, evaluated at arm time

| # | Precondition | Rationale |
|---|---|---|
| P1 | `ShuffleDriverComponents.supportsReliableStorage() == true` | otherwise restored `MapStatus.loc` addresses dead executors. Celeborn returns `!shuffleForceFallbackEnabled` (`CelebornShuffleDataIO.java:61`), so this is a real check |
| P2 | `spark.resume.queryId` == Celeborn `appUniqueId` == app identity | §6.4 |
| P3 | `celeborn.client.application.uuidSuffix.enabled == false` | §6.4 |
| P4 | `celeborn.client.spark.stageRerun.enabled == true` | §6.3 — no mapping layer without it |
| P5 | driver identity survives restart: `spark.driver.port` pinned, driver Service not owned by the driver pod, executor pods not cascade-deleted by `ownerReference` | otherwise the executor pool is destroyed with the driver |
| P6 | no `UNPINNABLE` leaf when `requirePinnedSnapshot = true` | §3.2 |
| P7 | store `schemaVersion` == runtime `schemaVersion`, including connector versions | §5.2 |
| P8 | push-based shuffle disabled | it maintains its own merge state (`shuffleMergeId`, `mergerLocs`, finalisation) that adoption does not reconstruct |
| P9 | local shuffle reader disabled | §5.5 — meaningless with a remote shuffle service |
| P10 | for `WRITE` anchors: the writer is a DSv2 `BatchWrite` with an atomic commit and collision-free file naming | §5.4 |

### 9.3 Capability gates

| Key | Default | Effect |
|---|---|---|
| `spark.resume.aqe.enabled` | `true` | AQE stage anchors |
| `spark.resume.aqe.pinPartitionSpecs` | `true` | **do not disable.** Disabling it must also disable AQE anchoring — never "reuse shuffle, recompute specs" |
| `spark.resume.nonAqe.enabled` | `true` | hook `ShuffleExchangeExec` for plans AQE skipped (§5.6) |
| `spark.resume.dpp.enabled` | `true` | false = DPP subqueries always recompute; scans still anchored |
| `spark.resume.dpp.injectSubqueryResults` | `true` | false = verify-only: detect key drift, never inject |
| `spark.resume.scan.requirePinnedSnapshot` | `true` | recommended for versioned tables |
| `spark.resume.scan.allowSplitCapture` | `true` | §5.2 |
| `spark.resume.scan.maxSplitBytes` | `64MB` | above it, degrade the pin strength |
| `spark.resume.scan.validateSplits` | `sample` | `exists` \| `sample` \| `none` |
| `spark.resume.scan.fileDigest.maxFiles` | `200000` | above it, degrade to `UNPINNABLE` |
| `spark.resume.broadcast.payloadAnchoring` | `true` | §5.1 |
| `spark.resume.broadcast.maxPayloadBytes` | `64MB` | above it, recompute |
| `spark.resume.determinism.strict` | `true` | `UNKNOWN` treated as `INDETERMINATE` |
| `spark.resume.determinism.allowConditional` | `true` | enables §3.5's middle class under Rule D-2 |
| `spark.resume.constants.replay` | `true` | §3.3 |
| `spark.resume.write.commitMessageAnchoring` | **`false`** | §5.4 — highest value, highest risk |
| `spark.resume.result.maxBytes` | `64MB` | `collect()`-style result anchoring |
| `spark.resume.anchorMinStageDurationMs` | `60000` | do not anchor stages cheaper than their anchor |
| `spark.resume.maxAnchorsPerQuery` | `2000` | bound blast radius and store footprint |

### 9.4 Store and timing

| Key | Default |
|---|---|
| `spark.resume.store` | required |
| `spark.resume.store.batchSize` | `200` |
| `spark.resume.store.queueCapacity` | `100000` — bounded; overflow drops, never blocks |
| `spark.resume.store.onUnavailable` | `degrade` \| `fail`; default `degrade` |
| `spark.resume.store.ttlSeconds` | `43200`, refreshed on every write |
| `spark.resume.restore.minRegisteredResourcesRatio` | `0.8` |
| `spark.resume.restore.maxWaitMs` | `120000` |
| `celeborn.master.heartbeat.application.timeout` | raise to `900s` for the resume window |

### 9.5 Interaction rules

- **Monotonicity.** Turning any flag **off** can only reduce the set of adopted anchors. A flag whose
  disablement could *widen* adoption is a design bug; assert this property in tests, not in review.
  The trap is real: disabling `dpp.enabled` must **reject** DPP-bearing anchors, not silently accept
  them on the grounds that DPP is "not in play."
- **Degradation never widens.** `requirePinnedSnapshot = false` permits weaker strengths; it must
  never permit `UNPINNABLE`.
- **`shadow` overrides everything.** No capability gate can cause adoption while `mode = shadow`.

---

## 10. Edge case register

Grouped by failure family. `SEV-1` denotes silently wrong data.

### C — Correctness

**C1 — Invalidation, not just addition. `SEV-1`**
`removeOutputsOnExecutor` / `removeOutputsOnHost` invalidate map outputs when an executor is lost.
An append-only store claims data that no longer exists.
*Mitigation:* every `unregisterMapOutput` and `removeOutputsOn*` path emits an invalidation under
the same fence; restore replays the stream after loading `:ms` and derives availability from the
surviving hash. Note that with reliable storage, executor loss does not invalidate map output
(`DAGScheduler.scala:2630`: `fileLost = !supportsReliableStorage() && ...`), which shrinks but does
not eliminate this path — explicit `unregisterShuffle` and stage rollback still reach it.

**C2 — Speculation and duplicate attempts.**
`registerMapOutput` is last-write-wins by `mapIndex`; `HSET` by `mapIndex` mirrors it exactly. The
losing attempt's Celeborn data may already be reclaimed, which is why rung 7 re-confirms liveness.

**C3 — Indeterminate stages. `SEV-1`** — §3.5, §5.3. The specific forbidden state is *partial*
adoption of a conditionally-determinate stage, and *non-uniform* adoption across its closure.

**C4 — Partial-DAG poisoning. `SEV-1`**
An anchor whose ancestor was rejected must not be used; its inputs are no longer the inputs it was
built from. Digests chain bottom-up so this is largely automatic, with an explicit `parentDigests`
check as the second layer.

**C5 — DPP key drift over an unpinned scan. `SEV-1`** — §4.3. Residual case is `PINNED_FILELIST`
against a mutating table: correctly rejected, but at a hit rate approaching zero, which is the
argument for snapshot pinning rather than a defect in it.

**C6 — AQE partition specs. `SEV-1`**
`AQEShuffleReadExec(child, partitionSpecs)` (`AQEShuffleReadExec.scala:41`) carries specs into
`ShuffledRowRDD`. Correct `MapStatus`es plus recomputed specs — 52 where the original produced 47 —
means every downstream partition reads a different byte range. No error, wrong aggregates.

**C7 — `defaultParallelism` restart race.** §3.4, §8 P2.

**C8 — Broadcast payload schema mismatch. `SEV-1`**
`UnsafeRow` deserialisation is schema-dependent; a mismatched schema does not throw, it reinterprets
bytes. Verify `schemaDigest` before deserialising and treat a mismatch as a hard reject.

**C9 — Double commit. `SEV-1`** — §5.4. Default-off, three preconditions, fence plus connector-level
optimistic concurrency.

**C10 — Serialisation version fragility.** Java-serialised `MapStatus` and connector-serialised
`InputPartition` do not survive upgrades. `schemaVersion` includes both Spark and connector versions;
mismatch is a hard refusal, never a best-effort attempt.

**C11 — Reused exchange.** Anchors are keyed on the canonicalized exchange, which is what
`stageCache` keys on (`AdaptiveSparkPlanExec.scala:526, 545, 881`), so reuse maps to one anchor by
construction. Reinstall the adopted stage into `context.stageCache` so `ReusedExchangeExec` keeps
working.

**C12 — Barrier stages.** All tasks re-execute together; Celeborn treats barrier like indeterminate
(`LifecycleManager.scala:1041-1045`). Never anchored.

**C13 — Empty shuffles.** `filteredPartitions.isEmpty` with `SinglePartition` yields an empty RDD
(`BatchScanExec.scala:132`). Availability keys on `MapStatus` presence, not byte counts, so
`numMaps = 0` is a valid anchor rather than an indistinguishable absence.

**C14 — Stale listing asymmetry.**
`FileStatusCache` may have served the crashed driver a stale listing; the restarted driver's cache
is cold and lists fresh. The anchor records what was *actually used*, so a legitimate difference
appears as drift and is rejected — conservative, and it vanishes entirely under snapshot pinning.

**C15 — View and temp-view resolution.**
A temp view registered in a different order, or a persistent view whose definition changed, resolves
the same name to different data. Anchoring on the **resolved relation identity** rather than the
name is what catches this; a name-based identity would not.

**C16 — Session timezone.** Affects date-partition pruning and `current_date` folding. In the config
digest, and captured among query-scoped constants.

**C17 — Multiple jobs per query.** A single `df.write` can trigger several jobs. Anchors are per
exchange, not per job, so this needs no special handling — but the *fence* is per query, so a second
job must not re-acquire a generation.

### F — Fencing and liveness

**F1 — Zombie driver writing to the store.** §7.4.
**F2 — Zombie driver holding Celeborn.** §7.4; confirm termination, never race.
**F3 — Restart outside the 300 s window.** Detected at rung 7; every anchor drops and the query runs
cold. Self-healing.
**F4 — Double restart.** A restore that itself crashes must be restartable. The generation counter
makes this safe; the coordinator must be idempotent through P9, and adoption must be
all-or-nothing per anchor rather than incremental.

### O — Operational

**O1 — Store eviction.** `noeviction`, checked at arm time; availability never derived from `:done`
(§7.3).
**O2 — Recovery read cost.** `HGETALL` on a 20 000-field hash blocks the server; use `HSCAN`.
**O3 — Store as a single point of failure.** `onUnavailable = degrade`. Failing a query because the
*optional recovery layer* is down is strictly worse than not having the feature.
**O4 — TTL versus long queries.** A TTL long enough to survive a six-hour query is long enough to
leak. Refresh on write; sweep on completion via a listener; bound with `maxAnchorsPerQuery`.
**O5 — Memory footprint.** 20 000 maps × 10 000 reducers is on the order of 192 MB of `MapStatus`
for one shuffle. Measure against real `CompressedMapStatus` bytes, which are highly repetitive, not
against random payloads — synthetic sizing here overestimates badly.
**O6 — Cluster resharding mid-query.** Hash tags keep a query's keys in one slot, but a resharding
event can still move that slot. Treat store errors as `degrade`, and never hold a partially applied
multi-key state.
**O7 — Superseded Celeborn mappings.** An adopted shuffle is registered under a new app shuffle ID;
the old entry is orphaned. Reconcile at completion via `unregisterSuperseded`.
**O8 — UI and history-server gaps.** Adopted stages never emit `SparkListenerStageCompleted`, so the
history server shows holes and metrics read zero. Emit synthetic listener events marked `RESUMED` so
an operator reading the UI at 3 a.m. sees "adopted," not "missing."

### S — SQL layer

**S1 — `ContextCleaner` weak references.**
`ContextCleaner.scala:233-241` performs `shuffleDriverComponents.removeShuffle(shuffleId, blocking)`
then `mapOutputTrackerMaster.unregisterShuffle(shuffleId)`, driven by weak references. If an adopted
dependency becomes unreachable, Celeborn deletes recovered data mid-query. Under this design
dependencies are constructed normally and stay reachable through the live plan, but hold an explicit
strong-reference pin for the query's lifetime and release it in a `QueryExecutionListener`. The
failure is intermittent, timing-dependent, and will present as Celeborn corruption rather than as a
bug in this code — which is exactly why it is worth pinning rather than reasoning about reachability.

**S2 — Subquery ordering.** Invariant DPP-1, §4.5.
**S3 — CTE and `ReusedSubqueryExec`.** Same shape as C11; key on the canonicalized subquery plan.
**S4 — Nested AQE.** Subqueries get their own `AdaptiveSparkPlanExec` with their own `stageCache`.
The hook installs on every context, not just the outermost.
**S5 — AQE re-optimisation.** The physical plan mutates between stage materialisations, so a
downstream exchange's digest must be computed at the same point in AQE's loop on both runs — at
`newQueryStage`, after re-optimisation. This is reproducible precisely because
`MapOutputStatistics` is pinned (§2.3); the two facts are the same fact.
**S6 — `InsertAdaptiveSparkPlan` bail-out.** AQE is skipped for some plans regardless of config;
§5.6 covers them.

---

## 11. Stack interactions

### 11.1 Comet

Comet replaces plan nodes with native equivalents and, when native shuffle is enabled, changes the
shuffle write path. Three consequences:

- **Digest safety is automatic and good.** A restart where Comet falls back to JVM execution —
  because of a version difference, a config difference, or an unsupported-type gate — produces
  structurally different plan nodes, hence a different digest, hence a miss. Safe by construction.
- **`spark.comet.*` and the Comet version belong in the digest and in `schemaVersion`.** Otherwise a
  fallback that happens to preserve node structure would not be detected.
- **Native shuffle adoption is unproven.** Where Comet uses its own shuffle writer, the `MapStatus`
  shape and the Celeborn integration path differ from the JVM path. Gate it:
  `spark.resume.comet.allowNativeShuffleAdoption`, default `false`, until a real test exists.

### 11.2 Iceberg

The best case for this design. Snapshot pinning is native (`snapshot-id`), so `PINNED_SNAPSHOT` is
available for every leaf, which is what makes DPP genuinely solved rather than merely detected.
Atomic commits make `WRITE` anchoring viable (§5.4). Schema and partition-spec IDs give
`schemaDigest` real content.

Two Iceberg-specific hazards. `expire_snapshots` can remove the pinned snapshot between crash and
restart, turning a pin into a `FileNotFoundException` — detected at rung 6 if the snapshot is gone,
and by split validation otherwise. And `CREATE OR REPLACE TABLE ... AS SELECT` deserves care: the
*read* side anchors normally and benefits fully, while the *write* side is an atomic replace that
must either complete or not — never partially adopt a replace. Treat RTAS/CTAS write stages as
non-anchorable unless `commitMessageAnchoring` is on and the connector's replace is genuinely atomic.

### 11.3 Kubernetes

The deployment constraints are not incidental; they determine whether the feature can function.

- Executor pods must not be owned by the driver pod, or they are garbage-collected the moment the
  driver dies and the 300 s budget is spent re-acquiring capacity.
- `spark.driver.port` must be pinned and the driver Service must outlive the pod, so surviving
  executors can reconnect rather than self-terminating.
- Application identity must survive the restart (§6.4). A pod restart within one submission is the
  supported shape; a fresh submission is not resumable and must not pretend to be.
- Node-level failure that takes executors with the driver reduces this to a cold start regardless of
  anything here.

### 11.4 Kyuubi and Flight SQL

Where the driver is a Kyuubi engine, "driver restart" and "engine relaunch" are different events
with different identities. An engine relaunch is a **new submission**: new application ID, new
Celeborn `appUniqueId`, no adoption. Only an in-place restart of the same submission is resumable.

Additionally, the client's session dies with the engine: temp views, cached tables and session
configs are gone, and a Flight SQL client must reconnect and re-issue. Resume therefore recovers
*compute*, not *session state*. If session-scoped objects participate in the plan — a temp view over
a subquery, say — their re-creation must be deterministic or the digest will legitimately miss. This
is a good reason to scope early adoption to queries whose inputs are catalog tables rather than
session-local objects.

---

## 12. Failure mode analysis

| Failure | Detected by | Result | Severity if the control fails |
|---|---|---|---|
| Input table mutated | `InputAnchor` rung 6 | recompute | `SEV-1` |
| DPP keys changed | `pruningDigest` + input anchor | recompute | `SEV-1` |
| Plan replanned differently | `PlanDigest` rung 5 | recompute | `SEV-1` |
| Partition specs recomputed | specs restored verbatim | correct reuse | `SEV-1` |
| Partial adoption of a round-robin stage | atomic-adoption rule | recompute | `SEV-1` |
| Non-uniform adoption across an indeterminacy closure | Rule D-2 | recompute closure | `SEV-1` |
| Broadcast payload schema mismatch | `schemaDigest` before deserialise | recompute | `SEV-1` |
| Double commit on a write stage | fence + connector optimistic concurrency | one commit wins | `SEV-1` |
| Zombie driver writes anchors | generation fence | zombie stops writing | `SEV-1` |
| Store evicts half a map-status hash | availability derived from `:ms` | recompute | `SEV-1` |
| Celeborn expired the app | rung 7 | cold run | SEV-3 (wasted) |
| Skew or child shuffle | rung 8 | recompute | `SEV-1` |
| Spark or connector upgraded | `schemaVersion` rung 1 | cold run | SEV-2 (crash) |
| Split files deleted | split validation, else read-time error | loud failure | SEV-2 |
| Store unavailable | `onUnavailable = degrade` | capture disabled | SEV-3 |
| Digest unstable for a connector | shadow-mode hit rate near zero | feature no-ops | SEV-4 (useless) |

The last row is the one that ends the project rather than breaking it, and it is the cheapest to
test (§13.1).

---

## 13. Test plan

### 13.1 The question to answer first

Fingerprint stability for **real connector-backed scans**, cross-JVM. `BatchScanExec.hashCode`
delegates to a connector object; the structural digest of §3.1 is designed to avoid that, but the
design is only worth building if the digest is in fact stable across processes for the connectors
actually in use. Two processes, real tables, a DPP-bearing query, assert digest equality — and
assert inequality across every drift-matrix row below.

If digests are unstable and cannot be made stable, the hit rate on the target stack is zero and the
correct decision is to stop. That is a day of work to learn, and it should precede everything else.

### 13.2 Drift matrix

For each mutation the required outcome is: **digest or anchor differs, anchor rejected, stage
recomputes.** A single row that yields "matches, but bytes differ" invalidates the design.

| # | Mutation between capture and restart | Must be caught by |
|---|---|---|
| 1 | new snapshot on the fact table | `snapshotToken` |
| 2 | new snapshot on the dimension table (changes DPP keys) | `pruningDigest` + dim leaf pin |
| 3 | compaction rewrites files, same rows | `fileDigest`; moot under snapshot pin |
| 4 | `spark.sql.shuffle.partitions` changed | `configDigest` |
| 5 | `autoBroadcastJoinThreshold` changed, D1 becomes D3 | `PlanDigest` (structure) |
| 6 | executors register slowly, different `defaultParallelism` | `coalesceParams` + `configDigest` |
| 7 | DPP disabled | `PlanDigest` |
| 8 | AQE coalesces to a different spec count | specs restored verbatim — unchanged digest is *correct* here |
| 9 | Spark upgraded | `schemaVersion` |
| 10 | connector upgraded | `schemaVersion` (connector component) |
| 11 | Celeborn reclaimed the app | rung 7 |
| 12 | query rewritten, `groupBy` order swapped | `PlanDigest` |
| 13 | `rand()` added | determinism classifier |
| 14 | `current_timestamp()` present, constants replay disabled | `queryConstantsDigest` |
| 15 | session timezone changed | `configDigest` |
| 16 | temp view redefined between runs | resolved relation identity |
| 17 | dimension row deleted, pruning keys shrink | `pruningDigest` |
| 18 | broadcast build side schema changed | `schemaDigest` |
| 19 | pinned split files deleted | split validation |
| 20 | skew join triggered on one run only | rung 8 + specs |

Assert **both** directions for every row: the drift is caught, *and* the no-drift control still
hits. A design that rejects everything is safe and worthless, and only the second assertion
distinguishes the two.

### 13.3 Layer tests

- **Metadata path** — cross-JVM `MapStatus` restore leaves `findMissingPartitions` empty. This is
  the core-scheduler fact everything rests on and it is cheap to test in local mode.
- **Data path** — a real cluster, real Celeborn workers, driver killed, adoption performed, bytes
  read back. This is the only test that proves anything beyond metadata; every local-mode result is
  a proxy for it, because a restored `BlockManagerId(driver, localhost, …)` addresses nothing.
- **DPP injection** — inject captured keys, assert the file set read is byte-identical to a
  live-subquery run; then mutate the dimension table and assert rejection.
- **Broadcast payload** — round-trip a payload, assert the rebuilt relation produces identical join
  output.
- **Determinism closure** — build a self-join over a repartitioned relation, force partial adoption,
  assert the closure rule rejects rather than producing duplicated or dropped rows. This test is the
  one most likely to catch a real bug, because the failure has no exception attached to it.
- **Flag monotonicity** — property test: for every flag, disabling it never increases the adopted set.
- **Chaos** — kill the driver at randomised points, including mid-commit and mid-anchor-write, and
  assert the result equals a cold run byte-for-byte.

---

## 14. Patch inventory and rollout

| # | Repo | Content | Risk | Blocking |
|---|---|---|---|---|
| 1 | fork | config surface, digest, input anchors, store | none — new package | no |
| 2 | spark/core | `advanceAtLeast` on the two counters that need it; bulk tracker seeding helper | trivial | no |
| 3 | spark/sql | AQE anchor hook at `newQueryStage`; capture at `AdaptiveSparkPlanExec.scala:327` | medium — AQE control flow | yes |
| 4 | spark/sql | `InSubqueryExec.injectResult` / `hasResult`; `prepareSubqueries` hook | low — one guarded field | yes (DPP) |
| 5 | spark/sql | input-anchor extraction for `FileSourceScanExec` and `BatchScanExec`; read-version pinning SPI; split capture and replay | medium — connector-facing | yes (DPP) |
| 6 | spark/sql | `BroadcastExchangeExec.relationFuture` guard; payload capture | low | no |
| 7 | spark/sql | query-scoped constant capture and replay (`ComputeCurrentTime`, `ResolveRandomSeed`) | low, high leverage | no |
| 8 | spark/core+sql | `ShuffleExchangeExec` hook for non-AQE plans | low | no |
| 9 | spark/sql | write-stage commit-message anchoring | **high** | no — default off |
| 10 | **celeborn** | `adoptShuffle`, `exportShuffleCatalog`, `confirmAlive`, `isSkewOrChildShuffle`, plus persistence for `shuffleIdMapping` and commit state | high | **yes, hard prerequisite** |
| 11 | fork | restore coordinator, strong-reference pinning, metrics, synthetic listener events | none upstream | yes |

A connector-facing SPI is needed for read-version pinning:

```scala
trait SupportsReadVersionPinning {
  def currentReadVersion(): Option[String]
  def pinReadVersion(token: String): Unit
}
```

Versioned connectors implement it trivially. One that cannot answer yields `UNPINNABLE`, and with
`requirePinnedSnapshot = true` the query refuses to arm — which is the correct default.

**Rollout order.** Patch 1 → patch 2 → shadow mode via patches 3 and 8 with adoption disabled →
collect the rejection histogram for a month → patches 4, 5, 6, 7 → patch 9 only after everything
else is boring → patch 11 last. Patch 10 runs in parallel from day one.

Patch 10 is a different repository, a different mailing list and a different review culture. It is
not "one more patch." Open that conversation **before** the Spark-side work: a `LifecycleManager`
persistence layer is plausibly something the Celeborn community already wants, and if so it is a
strong standalone contribution regardless of whether driver resume ever ships. If the answer is no,
the Spark side has no consumer and the rest of this document is unbuildable.

---

## 15. Metrics and operability

Per query:

```
resume.anchors.written            resume.anchors.offered           resume.anchors.adopted
resume.anchors.rejected{reason}   reason ∈ SCHEMA_MISMATCH | INDETERMINATE | ANCESTOR_REJECTED |
                                            INPUT_DRIFT | DIGEST_MISS | CELEBORN_EXPIRED |
                                            SKEW_SHUFFLE | PAYLOAD_TOO_LARGE | STORE_UNAVAILABLE
resume.bytes.adopted              resume.stages.skipped            resume.wallClock.saved.estimate
resume.store.writeLatency.p50/p99 resume.store.queueDepth          resume.store.dropped
resume.restore.phaseDurationMs{phase}
```

The rejection histogram is the product in shadow mode. Two shapes to watch for, because they look
like success until read carefully: **`DIGEST_MISS` near 100%** means the fingerprint is unstable and
the feature is a no-op; **`INPUT_DRIFT` near 100%** means the tables mutate faster than restarts
complete, and the honest response is snapshot pinning or abandonment, not tuning.

Every adopted stage emits a synthetic `SparkListenerStageCompleted` tagged `RESUMED` (§10.O8), and
the driver logs a single structured line per anchor decision with the digest, the rung that failed,
and the specific drift. A resume feature whose decisions are not legible is a resume feature nobody
will trust enough to leave enabled.

---

## 16. Scoping

### 16.1 What this buys

For a six-hour query that dies at hour five: completed shuffle stages are skipped; recomputed are
in-flight tasks, stages whose anchors failed verification, broadcasts above the payload cap, and any
write stage while commit anchoring is off. Minutes, not zero.

### 16.2 What it costs

Two codebases, one of which is not yours. A `SEV-1` failure class introduced deliberately and then
fenced by eight rungs, four pin strengths, a determinism lattice and a closure rule. A verification
ladder that has to be right on every branch, forever, including branches added by future Spark
versions.

### 16.3 Three things that should happen before the bulk of the work

1. **Test digest stability against the real connector** (§13.1). A day. If it fails, stop.
2. **Ask the Celeborn community about patch 10 before writing it.** One email. It gates everything,
   and it may be a better standalone contribution than the Spark work regardless of the outcome
   here. See `design-aqe-and-corrupted-rerun.md` §2 for the specific question this now includes:
   whether `adoptShuffle`-registered shuffle interacts correctly with native `stageRerun`/revive,
   and whether a second client can check a partition's current epoch without owning the
   `LifecycleManager`. That doc also covers the stage-granular successor to `PlanDigest` needed
   before this design is useful against AQE-heavy long-running queries — §3.1 as built is
   whole-plan and stops matching after the first mid-query replan.
3. **Be able to say why decomposition does not apply.** The reason no major vendor ships this,
   despite the incentive and the headcount, is that the usual answer to "long queries redo
   everything" is to materialise intermediate results to durable tables at natural boundaries and
   make the orchestrator restart idempotent: most of the benefit, near-zero engineering risk, no
   correctness exposure. If the queries are monolithic statements you do not control, decomposition
   genuinely does not apply and this project is well motivated. That sentence should be sayable out
   loud before quarters are spent.

### 16.4 Framing

This is not "Spark driver HA." It is **fingerprint-verified shuffle reuse across driver restarts,
where a verification miss degrades to recompute.** The central property is that the failure mode is
wasted work rather than corruption, and the architecture exists to make that true by construction
rather than by discipline. Cross-query result caching is the obvious next step and is deliberately
excluded: it changes the security model, since content-addressed anchors would let one tenant's
shuffle bytes serve another tenant's query.

---

## 17. Invariants

```
A-1    Losing state is safe. Wrong state is catastrophic. When in doubt, drop the anchor.
W-1    The Celeborn commit happens-before the anchor write that records it. Never inverted.
DPP-1  Every injectResult completes before any node in that subtree is prepared or executed.
V-1    An anchor is adopted only if all eight rungs of the acceptance ladder pass.
V-2    Rejecting an anchor transitively rejects its descendants and its indeterminacy closure.
D-1    Only DETERMINATE and CONDITIONALLY_DETERMINATE subtrees are anchored; UNKNOWN is
       INDETERMINATE.
D-2    A CONDITIONALLY_DETERMINATE stage is adopted atomically, and uniformly across its
       entire dependency closure.
F-1    Every store write is gated on the generation fence; a losing writer stops permanently.
F-2    Adoption never races the previous driver; termination is confirmed, not assumed.
M-1    Turning any flag off can only reduce the set of adopted anchors, never widen it.
S-1    The store is never the authority. Celeborn is.
```
