package org.apache.spark.resume

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/** LLD §3.5 -- the determinism lattice.
  *
  *   DETERMINATE                every leaf pinnable; every expression deterministic or a
  *                              replayable query-scoped constant; no order-sensitive
  *                              repartitioning
  *   CONDITIONALLY_DETERMINATE  reproducible IF nothing beneath it recomputes: round-robin
  *                              repartition, replayed-seed rand(), shuffle(), first()/last()
  *   INDETERMINATE              nondeterministic UDF, external mutable state, streaming source
  *   UNKNOWN                    unproven -> treated as INDETERMINATE
  *
  * The middle class is what unlocks real workloads. Spark classifies repartition(n) as
  * indeterminate because RECOMPUTATION can reassign rows; adoption is precisely the act of not
  * recomputing, so a wholly adopted round-robin stage emits the same bytes it emitted before.
  *
  * The safety condition is a CLOSURE property, not a node property (Rule D-2):
  *   - the stage must be adopted ATOMICALLY -- every map output present, zero partial recompute;
  *   - the adoption decision must be UNIFORM across every anchor that transitively depends on it.
  *
  * The second clause is the non-obvious one. A self-join over a repartitioned relation where one
  * side is adopted and the other recomputed yields two mutually inconsistent views of a single
  * relation, with rows duplicated and dropped and no exception anywhere. */
sealed trait Determinism { def adoptable: Boolean }
case object Determinate extends Determinism { val adoptable = true }
case object ConditionallyDeterminate extends Determinism { val adoptable = true }
case object Indeterminate extends Determinism { val adoptable = false }
case object Unknown extends Determinism { val adoptable = false }

case class Classification(level: Determinism, reasons: Seq[String], closureId: String)

object DeterminismClassifier {

  /** Expressions Spark marks nondeterministic that ARE reproducible once their query-scoped
    * constant is replayed (LLD §3.3). Everything else nondeterministic is fatal. */
  private val REPLAYABLE = Set(
    "Rand", "Randn", "Uuid", "Shuffle",
    "CurrentTimestamp", "Now", "CurrentDate", "CurrentTimeZone", "LocalTimestamp")

  def classify(
      plan: SparkPlan,
      leafAnchors: Map[String, InputAnchor],
      constantsReplayEnabled: Boolean,
      allowConditional: Boolean,
      strict: Boolean): Classification = {

    val reasons = Seq.newBuilder[String]
    var level: Determinism = Determinate

    def worsen(l: Determinism, why: String): Unit = {
      reasons += why
      level = (level, l) match {
        case (_, Indeterminate) | (Indeterminate, _) => Indeterminate
        case (_, Unknown) | (Unknown, _)             => Unknown
        case (_, ConditionallyDeterminate) | (ConditionallyDeterminate, _) =>
          ConditionallyDeterminate
        case _ => Determinate
      }
    }

    // 1. leaves must all be pinnable
    leafAnchors.foreach { case (leafId, a) =>
      if (a.pinning == Unpinnable) worsen(Indeterminate, s"unpinnable leaf $leafId")
    }
    if (leafAnchors.isEmpty) worsen(if (strict) Unknown else Determinate, "no leaf anchors extracted")

    // 2. expressions
    plan.foreach { p =>
      p.expressions.foreach { e =>
        e.foreach { sub: Expression =>
          if (!sub.deterministic) {
            val n = sub.getClass.getSimpleName
            if (REPLAYABLE.contains(n) && constantsReplayEnabled) {
              worsen(ConditionallyDeterminate, s"replayable constant $n")
            } else {
              worsen(Indeterminate, s"nondeterministic expression $n")
            }
          }
        }
      }
      // 3. order-sensitive repartitioning
      p match {
        case s: ShuffleExchangeExec if s.outputPartitioning.isInstanceOf[RoundRobinPartitioning] =>
          worsen(ConditionallyDeterminate, "round-robin repartition")
        case _ =>
      }
    }

    if (level == ConditionallyDeterminate && !allowConditional) {
      worsen(Indeterminate, "conditional determinism disabled by config")
    }
    if (level == Unknown && strict) level = Indeterminate

    // closureId groups every anchor that must share one adoption decision (Rule D-2). It is the
    // digest of the lowest conditionally-determinate ancestor; anchors sharing it are accepted or
    // rejected together, never individually.
    Classification(level, reasons.result().distinct,
      PlanDigest.sha(plan.canonicalized.getClass.getSimpleName + reasons.result().mkString))
  }
}
