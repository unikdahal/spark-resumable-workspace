# Patch inventory — verified hunks

Line numbers verified against `apache/spark` `v3.5.8` (`5a48a37b2dbd`) and `apache/celeborn`
`main` (`00113daf`). Each hunk is the smallest change that exposes an existing mechanism; none
introduces new scheduler control flow.

Section references point at `LLD-resumable-spark-driver.md`.

---

## Patch 2 — monotonic ID watermarks (Spark core, optional)

Semantics are `advanceAtLeast`, **not** `set` (LLD §8.1). A watermark cannot move a live counter
backwards, so a mis-sequenced restore can only waste identifiers. That is also what makes it
defensible upstream: an absolute setter on a live `SparkContext` is a footgun.

`core/src/main/scala/org/apache/spark/SparkContext.scala` (~:2713)
```scala
  private val nextShuffleId = new AtomicInteger(0)
+ private[spark] def advanceShuffleIdAtLeast(floor: Int): Unit = {
+   var cur = nextShuffleId.get()
+   while (cur < floor && !nextShuffleId.compareAndSet(cur, floor)) cur = nextShuffleId.get()
+ }
```

`core/src/main/scala/org/apache/spark/MapOutputTracker.scala` (~:1173)
```scala
+ private[spark] def advanceEpochAtLeast(floor: Long): Unit = epochLock.synchronized {
+   if (epoch < floor) { epoch = floor; logInfo(s"Advanced epoch to $floor on resume") }
+ }
```

Only these two. Not blocking — the same effect is reachable on stock 3.5.8 (LLD §8.1) — but
reflecting into internals is not a thing to ship.

## Patch 2b — bulk map-output seeding

`MapOutputTracker.scala`, after `:824`
```scala
+ private[spark] def seedShuffle(shuffleId: Int, numMaps: Int, numReduces: Int,
+                                statuses: Array[MapStatus]): Unit = {
+   registerShuffle(shuffleId, numMaps, numReduces)
+   var i = 0
+   while (i < statuses.length) {
+     if (statuses(i) != null) registerMapOutput(shuffleId, i, statuses(i))
+     i += 1
+   }
+   assert(findMissingPartitions(shuffleId).forall(_.isEmpty))
+ }
```

Keep the existing assertion at `MapOutputTracker.scala:337` loud. An inconsistent restore trips it,
which is the whole point. When it fires during development the instinct is to relax it; that
instinct is exactly backwards — it is the cheapest correctness tripwire in the system.

---

## Patch 3 — AQE anchor hook

Capture, `AdaptiveSparkPlanExec.scala:326-327` — the *sole* way AQE learns a stage finished:
```scala
  case StageSuccess(stage, res) =>
    stage.resultOption.set(Some(res))
+   resumeHook.foreach(_.captureStageAnchor(stage, res))
```
`res` is a `MapOutputStatistics` — small, serialisable. The `Future` is never touched.

Restore, `AdaptiveSparkPlanExec.scala:520-548`, in front of the `stageCache` lookup:
```scala
+ resumeHook.flatMap(_.tryAdopt(e, leafAnchors, effectiveDefaultParallelism, ...)) match {
+   case Some(stats) =>
+     val stage = newShuffleQueryStage(e, ...)
+     stage.resultOption.set(Some(stats))                  // born materialised
+     installPartitionSpecs(stage, anchor.partitionSpecs)  // mandatory, LLD §10.C6
+     context.stageCache.put(e.canonicalized, stage)       // preserves exchange reuse, LLD §10.C11
+     CreateStageResult(stage, allChildStagesMaterialized = true, newStages = Seq(stage))
+   case None => // existing path unchanged
+ }
```

`context.stageCache` is a `TrieMap[SparkPlan, ExchangeQueryStageExec]` (`:881`). AQE already solves
"is this stage equivalent to that one" for exchange reuse; reinstalling the adopted stage into it
keeps `ReusedExchangeExec` correct. **Do not use that key's `hashCode` for persistence** (LLD §3.1).

The hook installs on **every** `AdaptiveSparkPlanExec` context, not just the outermost — subqueries
get their own instance with their own cache (LLD §10.S4).

---

## Patch 4 — DPP result injection

`sql/core/src/main/scala/org/apache/spark/sql/execution/subquery.scala` (~:141)
```scala
  def values(): Option[Array[Any]] = Option(result)
+
+ private[sql] def hasResult: Boolean = result != null
+
+ /** Adopt a persisted pruning-key set instead of executing the subquery. Must run before any
+   * consumer lazy val is forced (invariant DPP-1). The require() is the enforcement: a
+   * mis-ordered hook fails the test suite instead of silently degrading every DPP query to a
+   * full recompute. */
+ private[sql] def injectResult(values: Array[Any]): Unit = {
+   require(result == null, "InSubqueryExec result already set")
+   result = values
+ }
```

`SparkPlan.prepareSubqueries` / `waitForSubqueries`:
```scala
  runningSubqueries.foreach { sub =>
+   if (!resumeHook.exists(_.tryInject(sub))) sub.updateResult()
-   sub.updateResult()
  }
```

`result` is a `var`, so even on stock 3.5.8 this is reachable with no final-field write. The
`require` is the reason to land the real API rather than reflect.

---

## Patch 5 — input anchors, read-version pinning, split capture

Two extraction points.

`DataSourceScanExec.scala:251-296` — `FileSourceScanExec`. Anchor the *listing*, not just the
predicate: `:280` filters a live catalog listing, so an identical predicate over a mutated table is
still a different scan.

`datasources/v2/BatchScanExec.scala:60-112` — the DSv2 path, and the harder one:
```scala
    val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
    filterableScan.filter(dataSourceFilters.toArray)      // MUTATES the connector's Scan
    val newPartitions = scan.toBatch.planInputPartitions()
```
The pruned set is produced inside the connector. Hence `PINNED_SNAPSHOT` (force the read version
back) and `PINNED_SPLITS` (replay the captured splits, bypassing the planner entirely).

Split capture is legal for every DSv2 source by contract —
`sql/catalyst/.../connector/read/InputPartition.java:38`:
```java
public interface InputPartition extends Serializable {
```

The injected field must be excluded from `equals`, `hashCode`, and the structural digest, exactly
as `@transient scan` already is — otherwise adopting changes the digest that authorised it.

New SPI:
```scala
trait SupportsReadVersionPinning {
  def currentReadVersion(): Option[String]
  def pinReadVersion(token: String): Unit
}
```

---

## Patch 6 — broadcast payload anchoring

`BroadcastExchangeExec.scala:131-199` shows the broadcast is a pure function of the collected rows,
the `BroadcastMode`, and `numRows`:
```scala
    val (numRows, input) = child.executeCollectIterator()
    val relation = mode.transform(input, Some(numRows))
    val broadcasted = sparkContext.broadcastInternal(relation, serializedOnly = true)
```

Guard at the head of `relationFuture` (a `lazy val`, so one guard covers AQE, non-AQE, and
`SubqueryBroadcastExec`):
```scala
  override lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
+   resumeHook.flatMap(_.tryRebuildBroadcast(this, mode, schema)) match {
+     case Some(b) => Future.successful(b)
+     case None =>
      SQLExecution.withThreadLocalCaptured(...) { ... }
+   }
  }
```

Persist the **rows**, not the built relation: rows are mode-agnostic and avoid depending on
`HashedRelation`'s internal format across versions. Verify `schemaDigest` before deserialising —
`UnsafeRow` deserialisation is schema-dependent and a mismatch reinterprets bytes rather than
throwing (LLD §10.C8).

---

## Patch 7 — query-scoped constant capture and replay

`Analyzer.scala:3243-3255` and `finishAnalysis.scala:110-132` assign timestamps and random seeds
**once per planning pass** and fold them in. So `rand()`, `uuid()`, `current_timestamp()`,
`current_date()` are constants, deterministic within a driver's lifetime, and break across a
restart only because the second pass picks different ones. Capture and replay them.

Install the replay rule **before** user code builds any DataFrame — constants are folded during
analysis, so a lazily installed rule already lost.

Independently of resume, this is a correctness improvement: a restarted query that stamps rows with
`current_timestamp()` produces the same stamp rather than a new one.

---

## Patch 8 — non-AQE hook

Without AQE there are no query stages, and the path is simpler: seed the tracker for the shuffle ID
and `findMissingPartitions` skips the stage with no scheduler change. Hook at
`ShuffleExchangeExec.shuffleDependency` construction. This matters because
`InsertAdaptiveSparkPlan` disables AQE for some plans regardless of configuration, and a hook that
only covers AQE silently does nothing for those.

---

## Patch 9 — write-stage commit-message anchoring (default off)

`sql/catalyst/.../connector/write/WriterCommitMessage.java:36`:
```java
public interface WriterCommitMessage extends Serializable {}
```

`V2TableWriteExec` collects one message per partition at the driver, then commits the set. Persist
each as it arrives; on restore submit only the missing indices and commit the union.

Three preconditions, checked rather than assumed: the writer is a DSv2 `BatchWrite` with an atomic
commit; file naming is collision-free across attempts; the old driver cannot also commit (fence
**plus** the connector's optimistic concurrency — the fence alone is insufficient because the
zombie may have begun committing before it was taken).

---

## Patch 10 — Celeborn (separate repository, hard prerequisite)

`LifecycleManager` is driver-embedded (`SparkShuffleManager.java:158`) and holds no persistence
(`grep -c 'recover|persist'` → 0). The shuffle *data* survives; the *catalog naming it* does not.

The hook already exists — `LifecycleManager.scala:109-115` maintains `shuffleIdMapping`,
`celebornShuffleIdToAppShuffleIdMap` and `appShuffleDeterminateMap` for stage rerun, with
identifiers `appShuffleId + "-" + stageId + "-" + stageAttemptNumber`
(`SparkCommonUtils.java:65-67`). It needs to be **seedable** and **durable**.

```scala
def adoptShuffle(appShuffleId: Int, appShuffleIdentifier: String,
                 celebornShuffleId: Int, determinate: Boolean): Boolean
def exportShuffleCatalog(): PbShuffleCatalog
def confirmAlive(celebornShuffleIds: Seq[Int]): Set[Int]
def isSkewOrChildShuffle(appShuffleId: Int): Boolean
def unregisterSuperseded(celebornShuffleIds: Seq[Int]): Unit
```

`getShuffleIdMapping` already exists at `:2104`, so the read half is partly in tree.

**Hard precondition.** `SparkUtils.java:143-165`:
```java
if (handle.stageRerunEnabled()) { ... client.getShuffleId(...) ... }
else                            { return handle.shuffleId(); }
```
With `celeborn.client.spark.stageRerun.enabled=false` there is no indirection at all, and adoption
would require reissuing identical Spark shuffle IDs in identical order.

**Also note** `CelebornShuffleHandle.scala:24-33` carries `lifecycleManagerHost` and
`lifecycleManagerPort`. Any design that deserialises a `ShuffleDependency` hands every task a handle
pointing at the dead driver's endpoint — one of several reasons this design constructs handles
fresh instead.
