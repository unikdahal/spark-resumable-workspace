package org.apache.spark.resume

import scala.collection.mutable

import org.apache.spark.MapOutputStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/** LLD §8 -- phase ordering, the eight-rung acceptance ladder, transitive and closure rejection.
  *
  * Three orderings are load-bearing rather than cosmetic:
  *   P2 (stabilise executors) before P8 (run main), because defaultParallelism feeds
  *      CoalesceShufflePartitions, which changes the plan, which changes the digest. Planning
  *      before executors settle guarantees a miss that looks like a broken feature rather than
  *      a misconfigured one.
  *   P3 (epoch) before everything, because epoch governs whether executors honour invalidations.
  *   P6 (constants) before P8, because constants are folded during ANALYSIS, which happens the
  *      instant user code builds a DataFrame -- a lazily installed rule already lost. */
class ResumeCoordinator(
    store: ResumeAnchorStore,
    celeborn: CelebornAdoptionClient,
    conf: SQLConf,
    schemaVersion: String,
    mode: ResumeConf.Mode) extends Logging {

  private var generation = -1L
  private val loaded    = mutable.Map.empty[String, Anchor]
  private val rejected  = mutable.Map.empty[String, RejectReason]
  private val adopted   = mutable.Set.empty[String]
  private val byClosure = mutable.Map.empty[String, mutable.Set[String]]
  private val pinnedDeps = mutable.ListBuffer.empty[AnyRef]   // LLD §10.S1
  var constants: QueryConstants = QueryConstants.empty

  // ---- P1, P4, P5, P6 --------------------------------------------------
  def prepare(queryId: String): Unit = {
    generation = store.acquireGeneration(queryId)
    val all = store.loadAnchors(queryId)

    val (okSchema, badSchema) = all.partition(_.schemaVersion == schemaVersion)   // rung 1
    badSchema.foreach(a => reject(a.planDigest, RejectReason.SchemaMismatch))

    val live = celeborn.confirmAlive(okSchema.map(_.celebornShuffleId))           // rung 7
    okSchema.foreach { a =>
      if (a.determinism != "DETERMINATE" && a.determinism != "CONDITIONALLY_DETERMINATE")
        reject(a.planDigest, RejectReason.NotAdoptable)                           // rung 2
      else if (a.kind == "SHUFFLE" && !live.contains(a.celebornShuffleId))
        reject(a.planDigest, RejectReason.CelebornExpired)
      else if (a.kind == "SHUFFLE" && celeborn.isSkewOrChildShuffle(a.sparkShuffleId))
        reject(a.planDigest, RejectReason.SkewShuffle)                            // rung 8
      else {
        loaded(a.planDigest) = a
        byClosure.getOrElseUpdate(a.closureId, mutable.Set.empty) += a.planDigest
      }
    }

    // Constants are replayed from any surviving anchor; they are query-scoped, so all agree.
    loaded.values.headOption.foreach(a => constants = a.queryConstants)

    logInfo(s"resume: generation=$generation loaded=${loaded.size} " +
      s"rejected={${rejected.values.groupBy(_.code).map { case (k, v) => s"$k:${v.size}" }.mkString(",")}}")
  }

  // ---- P8: the per-exchange decision -----------------------------------
  def tryAdopt(
      exchangeSubtree: SparkPlan,
      leafAnchors: Map[String, InputAnchor],
      effectiveDefaultParallelism: Int,
      currentLeafAnchors: Map[String, InputAnchor],
      newSparkShuffleId: => Int,
      seedMapOutputs: (Int, Anchor) => Unit,
      installSpecs: Array[Byte] => Unit,
      deserializeStats: Array[Byte] => MapOutputStatistics): Option[MapOutputStatistics] = {

    val digest = PlanDigest.compute(
      exchangeSubtree, leafAnchors, conf, effectiveDefaultParallelism, constants, schemaVersion)

    val anchor = loaded.get(digest) match {
      case None    => reject(digest, RejectReason.DigestMiss); return None        // rung 5
      case Some(a) => a
    }

    // rung 3 -- transitive AND closure. The closure clause (Rule D-2) is the non-obvious one:
    // a self-join over a repartitioned relation where one side is adopted and the other
    // recomputed yields two mutually inconsistent views of one relation, with rows duplicated
    // and dropped and no exception anywhere.
    if (anchor.parentDigests.exists(rejected.contains)) {
      reject(digest, RejectReason.AncestorRejected); return None
    }
    if (byClosure.get(anchor.closureId).exists(_.exists(rejected.contains))) {
      reject(digest, RejectReason.ClosureRejected); return None
    }

    // rung 2b -- atomic adoption for conditionally determinate stages: a single missing map
    // output rejects the whole anchor rather than triggering a partial recompute.
    if (anchor.determinism == "CONDITIONALLY_DETERMINATE" &&
        anchor.mapStatuses.exists(_ == null)) {
      reject(digest, RejectReason.NotAdoptable); return None
    }

    // rung 6 -- THE INPUT BOUNDARY. This is what makes DPP safe.
    val recorded = anchor.inputAnchors.map(a => a.leafId -> a).toMap
    val drift = currentLeafAnchors.iterator.map { case (leafId, cur) =>
      recorded.get(leafId) match {
        case None      => Some(s"$leafId absent from anchor")
        case Some(rec) => InputAnchor.verify(rec, cur).left.toOption.map(m => s"$leafId: $m")
      }
    }.flatten.toSeq.headOption
    if (drift.isDefined) { reject(digest, RejectReason.inputDrift(drift.get)); return None }

    if (mode == ResumeConf.Shadow) {
      logInfo(s"resume[shadow]: WOULD adopt $digest (celebornShuffleId=${anchor.celebornShuffleId})")
      return None
    }

    val sid = newSparkShuffleId
    if (!celeborn.adoptShuffle(sid, anchor.celebornShuffleId, determinate = true)) {
      reject(digest, RejectReason.CelebornExpired); return None
    }
    seedMapOutputs(sid, anchor)
    installSpecs(anchor.partitionSpecs)
    adopted += digest
    Some(deserializeStats(anchor.mapOutputStatistics))
  }

  def pinDependency(dep: AnyRef): Unit = pinnedDeps += dep
  def releasePins(): Unit = pinnedDeps.clear()

  private def reject(digest: String, r: RejectReason): Unit = {
    rejected(digest) = r
    logDebug(s"resume: anchor $digest rejected (${r.code} ${r.detail})")
  }

  def metrics: Map[String, Long] =
    Map("anchors.loaded" -> loaded.size.toLong, "anchors.adopted" -> adopted.size.toLong) ++
    rejected.values.groupBy(_.code).map { case (k, v) => s"anchors.rejected.$k" -> v.size.toLong }
}

/** The Celeborn workstream. Separate repository, separate mailing list, separate review culture:
  * it is not "one more patch". Open that conversation BEFORE the Spark-side work, because a
  * LifecycleManager persistence layer is plausibly something the community already wants -- and
  * if the answer is no, the Spark side has no consumer. */
trait CelebornAdoptionClient {
  /** Seeds LifecycleManager.shuffleIdMapping so handleGetShuffleIdForApp (LifecycleManager.scala
    * :998-1022) answers with the surviving celebornShuffleId instead of UNKNOWN_APP_SHUFFLE_ID. */
  def adoptShuffle(appShuffleId: Int, celebornShuffleId: Int, determinate: Boolean): Boolean
  def confirmAlive(celebornShuffleIds: Seq[Int]): Set[Int]
  /** LifecycleManager.scala:1041-1045 refuses to reuse a skew shuffle or a child of one even in
    * its OWN stage-rerun path. If Celeborn will not reuse it for a stage rerun, this design must
    * not reuse it for a driver restart. */
  def isSkewOrChildShuffle(appShuffleId: Int): Boolean
  def exportCatalog(): Seq[(Int, Int)]
  def unregisterSuperseded(celebornShuffleIds: Seq[Int]): Unit
}
