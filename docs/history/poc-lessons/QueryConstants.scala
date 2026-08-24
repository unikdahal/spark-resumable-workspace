package org.apache.spark.resume

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{ExpressionWithRandomSeed, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}

/** LLD §3.3 -- query-scoped constant capture and replay.
  *
  * A large fraction of what looks like nondeterminism is a constant chosen ONCE per planning
  * pass and folded into the plan. Two rules do this:
  *
  *   Analyzer.scala:3243-3255   ResolveRandomSeed
  *     case e: ExpressionWithRandomSeed if e.seedExpression == UnresolvedSeed =>
  *       e.withNewSeed(random.nextLong())
  *
  *   finishAnalysis.scala:110-132   ComputeCurrentTime
  *     val instant = Instant.now()
  *     case CurrentTimestamp() | Now() => Literal.create(instantToMicros(instant), TimestampType)
  *     case cd: CurrentDate            => Literal.create(microsToDays(...), DateType)
  *     case CurrentTimeZone()          => Literal.create(conf.sessionLocalTimeZone, StringType)
  *
  * So rand(), randn(), shuffle(), uuid(), current_timestamp(), now(), current_date() and
  * current_timezone() are NOT re-evaluated per row or per task. Within one driver's lifetime
  * they are perfectly deterministic. They break across a restart only because the SECOND
  * planning pass picks different constants.
  *
  * Capturing and replaying them is therefore a correctness improvement independent of resume:
  * a restarted query that stamps rows with current_timestamp() produces the SAME stamp, not a
  * new one. And it moves an entire class of everyday ELT queries out of the indeterminate
  * lattice and into the adoptable set -- the single highest-leverage item in this package. */
case class QueryConstants(
    currentTimestampMicros: Option[Long],
    sessionLocalTimeZone: Option[String],
    randomSeeds: Map[String, Long]) {          // structural expression path -> seed

  def digest: String = PlanDigest.sha(Seq(
    currentTimestampMicros.map(_.toString).getOrElse("-"),
    sessionLocalTimeZone.getOrElse("-"),
    randomSeeds.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(",")
  ).mkString("\u0001"))

  def isEmpty: Boolean =
    currentTimestampMicros.isEmpty && sessionLocalTimeZone.isEmpty && randomSeeds.isEmpty
}

object QueryConstants {
  val empty: QueryConstants = QueryConstants(None, None, Map.empty)

  /** Capture side. Runs after ComputeCurrentTime and ResolveRandomSeed have folded their
    * literals, and reads them back out of the optimized plan. Path-keyed, not exprId-keyed:
    * exprIds are allocation-order dependent and will not match across a restart, which is the
    * same reason canonicalization zeroes them. */
  def capture(plan: LogicalPlan): QueryConstants = {
    var ts: Option[Long] = None
    var tz: Option[String] = None
    val seeds = Map.newBuilder[String, Long]

    def walk(p: LogicalPlan, path: String): Unit = {
      p.expressions.zipWithIndex.foreach { case (e, i) =>
        e.foreach {
          case l: Literal if l.dataType == TimestampType && ts.isEmpty =>
            ts = Option(l.value).map(_.asInstanceOf[Long])
          case l: Literal if l.dataType == StringType && tz.isEmpty && looksLikeZone(l) =>
            tz = Option(l.value).map(_.toString)
          case r: ExpressionWithRandomSeed =>
            r.seedExpression match {
              case Literal(v: Long, _) => seeds += (s"$path#$i#${r.getClass.getSimpleName}" -> v)
              case _ =>
            }
          case _ =>
        }
      }
      p.children.zipWithIndex.foreach { case (c, j) => walk(c, s"$path/$j") }
    }
    walk(plan, "")
    QueryConstants(ts, tz, seeds.result())
  }

  private def looksLikeZone(l: Literal): Boolean = {
    val s = String.valueOf(l.value)
    s.contains("/") || s.startsWith("UTC") || s.startsWith("GMT") || s.startsWith("Etc")
  }

  /** Replay side. Installed BEFORE user code builds any DataFrame -- constants are folded during
    * analysis, which happens the instant a DataFrame is constructed, so a lazily installed rule
    * is a rule that already lost (LLD §8, phase P6). */
  class ReplayRule(constants: QueryConstants, enabled: Boolean) extends Rule[LogicalPlan] with Logging {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!enabled || constants.isEmpty) return plan
      var n = 0
      val out = plan.transformAllExpressions {
        case l: Literal if l.dataType == TimestampType && constants.currentTimestampMicros.isDefined =>
          n += 1; Literal.create(constants.currentTimestampMicros.get, TimestampType)
        case l: Literal if l.dataType == DateType && constants.currentTimestampMicros.isDefined =>
          l   // recomputed from the pinned micros by ComputeCurrentTime ordering; left as-is
        case e => e
      }
      logInfo(s"resume: replayed $n query-scoped constant(s)")
      out
    }
  }
}
