package org.apache.spark.resume

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/** LLD §3.1. Replaces plan.canonicalized.hashCode() as the resume key, for two disqualifying
  * reasons.
  *
  * STABILITY. BatchScanExec.scala:57 -- `Objects.hashCode(batch, runtimeFilters)` where `batch`
  * is connector-supplied. Identity hashing there makes the key JVM-instance-dependent, so every
  * anchor misses on every restart: safe, and completely useless. Invisible in a test that uses
  * spark.range(), whose leaves are pure case classes.
  *
  * WIDTH. 32 bits collides at ~77k distinct anchors by the birthday bound. */
object PlanDigest {

  private val PREFIXES = Seq(
    "spark.sql.adaptive.", "spark.sql.optimizer.dynamicPartitionPruning.",
    "spark.sql.sources.v2.bucketing.", "spark.sql.cbo.", "spark.sql.legacy.",
    "spark.sql.parquet.", "spark.comet.")

  private val EXACT = Seq(
    "spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.files.maxPartitionBytes", "spark.sql.files.openCostInBytes",
    "spark.sql.files.minPartitionNum", "spark.sql.join.preferSortMergeJoin",
    "spark.sql.codegen.wholeStage", "spark.sql.session.timeZone",
    "spark.shuffle.minNumPartitionsToHighlyCompress", "spark.default.parallelism")

  def configDigest(conf: SQLConf, effectiveDefaultParallelism: Int): String = {
    val all = conf.getAllConfs
      .filter { case (k, _) => EXACT.contains(k) || PREFIXES.exists(k.startsWith) }
      .toSeq.sorted
    // The EFFECTIVE value, not the configured one. CoalesceShufflePartitions.scala:57-61 falls
    // back to sparkContext.defaultParallelism, which tracks CURRENTLY REGISTERED executors and
    // therefore races a restart (LLD §10.C7).
    sha(all.map { case (k, v) => s"$k=$v" }.mkString("\u0001") +
        s"\u0001__effectiveDefaultParallelism=$effectiveDefaultParallelism")
  }

  /** Deterministic pre-order walk.
    *
    * MUST NOT call toString on connector Scan/Batch/Table objects, Broadcast, RDD, Future, or
    * anything transitively holding a SparkContext. Leaves are replaced wholesale by their input
    * anchor digest: the connector's own opinion of its identity is never trusted.
    *
    * Fields added by this feature (pinned splits, injected results) must be excluded here, and
    * from equals and hashCode -- otherwise the act of adopting changes the digest that
    * authorised the adoption. */
  def structuralString(plan: SparkPlan, leafDigests: Map[String, String]): String = {
    val sb = new StringBuilder
    def walk(p: SparkPlan, path: String): Unit = leafDigests.get(path) match {
      case Some(d) =>
        sb.append("LEAF@").append(path).append('[').append(d).append("]\n")
      case None =>
        sb.append(path).append(' ').append(p.nodeName).append(' ')
          .append(p.getClass.getSimpleName).append(' ')
          .append(p.expressions.map(_.canonicalized.toString).mkString(",")).append('\n')
        p.children.zipWithIndex.foreach { case (c, i) => walk(c, s"$path/$i") }
    }
    walk(plan, "")
    sb.toString
  }

  def compute(
      plan: SparkPlan,
      leafAnchors: Map[String, InputAnchor],
      conf: SQLConf,
      effectiveDefaultParallelism: Int,
      constants: QueryConstants,
      schemaVersion: String): String = {
    val canonical = plan.canonicalized.asInstanceOf[SparkPlan]
    sha(Seq(
      schemaVersion,
      structuralString(canonical, leafAnchors.map { case (k, v) => k -> v.digest }),
      leafAnchors.toSeq.sortBy(_._1).map { case (k, v) => s"$k=${v.digest}" }.mkString(","),
      configDigest(conf, effectiveDefaultParallelism),
      constants.digest
    ).mkString("\u0003"))
  }

  def sha(s: String): String =
    MessageDigest.getInstance("SHA-256").digest(s.getBytes(UTF_8)).take(16)
      .map(b => f"${b & 0xff}%02x").mkString
}
