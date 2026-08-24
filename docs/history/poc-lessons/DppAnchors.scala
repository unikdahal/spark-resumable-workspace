package org.apache.spark.resume

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution.{InSubqueryExec, SparkPlan}

/** LLD §4 -- DPP capture and injection.
  *
  * PlanDynamicPruningFilters.scala:52-87 rewrites every DynamicPruningSubquery into one of:
  *   D1  reusable broadcast -> InSubqueryExec(value, SubqueryBroadcastExec(...))   anchorable
  *   D2  onlyInBroadcast    -> Literal.TrueLiteral                                 inert
  *   D3  otherwise          -> InSubqueryExec over SubqueryExec(Aggregate(...))    anchorable
  * D1 and D3 both terminate in an InSubqueryExec: one injection point for all of DPP.
  *
  * subquery.scala:112-171 gives three favourable properties:
  *   - `result` is a plain var, so injection is ONE field write -- exactly analogous to AQE's
  *     single resultOption.set at AdaptiveSparkPlanExec.scala:327;
  *   - `canonicalized` already nulls result and zeroes exprId, so Spark itself defines a
  *     value-independent identity for a DPP subquery. That is the anchor key, free;
  *   - the payload is the distinct values of one join key -- kilobytes, not megabytes.
  *
  * INVARIANT DPP-1: every injection completes BEFORE any node in the subtree is prepared or
  * executed. selectedPartitions / dynamicallySelectedPartitions / filteredPartitions / inputRDD /
  * inSet are all `lazy val`; once forced they are frozen. Violating DPP-1 does not corrupt data --
  * the injected value is ignored and the subquery simply runs -- it silently converts every DPP
  * query into a full recompute. That is the class of bug that survives review and lives in
  * production for a year, which is why injectResult THROWS rather than logs. */
case class DppAnchor(subqueryDigest: String, pruningDigest: String, keys: Array[Byte])

object DppAnchors extends Logging {

  def subqueryDigest(in: InSubqueryExec, schemaVersion: String): String =
    PlanDigest.sha(Seq(schemaVersion, in.canonicalized.toString).mkString("\u0003"))

  def collect(plan: SparkPlan): Seq[InSubqueryExec] =
    plan.collect { case p: SparkPlan =>
      p.expressions.flatMap(_.collect {
        case DynamicPruningExpression(e) => e.collect { case in: InSubqueryExec => in }
      }.flatten)
    }.flatten

  def capture(plan: SparkPlan, schemaVersion: String,
              serialize: Array[Any] => Array[Byte]): Seq[DppAnchor] =
    collect(plan).flatMap { in =>
      in.values().map { vs =>
        DppAnchor(subqueryDigest(in, schemaVersion),
          InputAnchor.pruningDigest(Seq(DynamicPruningExpression(in))).getOrElse("-"),
          serialize(vs))
      }
    }

  /** Partial injection is safe: any subquery without an anchor simply executes, and the input
    * anchor ladder still gates whether the downstream shuffle is adopted. */
  def inject(plan: SparkPlan, anchors: Map[String, DppAnchor], schemaVersion: String,
             deserialize: Array[Byte] => Array[Any], enabled: Boolean): Int = {
    if (!enabled) return 0
    var n = 0
    collect(plan).foreach { in =>
      anchors.get(subqueryDigest(in, schemaVersion)).foreach { a =>
        InSubqueryExecAccess.injectResult(in, deserialize(a.keys)); n += 1
      }
    }
    logInfo(s"resume: injected $n DPP subquery result(s) before any scan lazy val was forced")
    n
  }
}

/** Isolates the one reflective write so the upstream patch can replace it in a single place.
  * Note that `result` is a VAR, so no final-field write is involved even on stock 3.5.8. */
private[resume] object InSubqueryExecAccess {
  def injectResult(in: InSubqueryExec, values: Array[Any]): Unit = {
    val f = classOf[InSubqueryExec].getDeclaredField("result")
    f.setAccessible(true)
    require(f.get(in) == null,
      "DPP-1 violated: InSubqueryExec.result is already populated, so a consumer lazy val may " +
      "already have been forced. Move the injection hook earlier.")
    f.set(in, values)
  }
}
