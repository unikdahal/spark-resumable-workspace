package org.apache.spark.resume

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.types.StructType

/** LLD §5.1 -- broadcast payload anchoring.
  *
  * Broadcast blocks die with the driver, so the naive conclusion is that broadcast exchanges
  * always recompute. But BroadcastExchangeExec.scala:131-199 shows the broadcast is a pure
  * function of three things:
  *
  *     val (numRows, input) = child.executeCollectIterator()
  *     val relation = mode.transform(input, Some(numRows))     // HashedRelation | Array[InternalRow]
  *     val broadcasted = sparkContext.broadcastInternal(relation, serializedOnly = true)
  *
  * The mode is part of the plan (hence the digest). The rows are UnsafeRows -- flat byte arrays.
  * And the size is bounded BY CONSTRUCTION: maxBroadcastRows and MAX_BROADCAST_TABLE_BYTES are
  * hard ceilings, with autoBroadcastJoinThreshold the operative bound in practice.
  *
  * So persist the ROWS (not the built relation) and rebuild. Rows are mode-agnostic, so a plan
  * that legitimately rebuilds with a different BroadcastMode -- D1 vs D3 in LLD §4.1 -- still
  * hits, and you avoid depending on HashedRelation's internal serialised format across versions.
  *
  * The win is disproportionate: a broadcast build side is often an entire dimension subtree
  * (scan, filters, aggregate, sometimes a join). This replaces "recompute a subtree" with
  * "read ten megabytes and re-transform". */
case class BroadcastPayload(
    planDigest: String,
    schemaDigest: String,
    numRows: Long,
    rowBytes: Array[Byte])

object BroadcastAnchor extends Logging {

  /** Hook at the head of BroadcastExchangeExec.relationFuture -- a `lazy val`, so one guard
    * covers AQE, non-AQE, and SubqueryBroadcastExec in a single place. Hooking only
    * BroadcastQueryStageExec would silently leave the other two uncovered. */
  def tryRebuild(
      spark: SparkSession,
      payload: Option[BroadcastPayload],
      expectedSchemaDigest: String,
      mode: BroadcastMode,
      maxPayloadBytes: Long,
      deserializeRows: (Array[Byte], StructType) => Iterator[InternalRow],
      schema: StructType): Option[Broadcast[Any]] = payload.flatMap { p =>

    // HARD reject, never a warning. UnsafeRow deserialisation is schema-dependent: a mismatched
    // schema does not throw, it REINTERPRETS BYTES. That is a memory-safety problem wearing a
    // wrong-answer costume (LLD §10.C8).
    if (p.schemaDigest != expectedSchemaDigest) {
      logWarning(s"resume: broadcast payload schema mismatch for ${p.planDigest}; recomputing")
      None
    } else if (p.rowBytes.length > maxPayloadBytes) {
      logInfo(s"resume: broadcast payload ${p.rowBytes.length}B over cap; recomputing")
      None
    } else {
      val rows = deserializeRows(p.rowBytes, schema)
      val relation = mode.transform(rows, Some(p.numRows))
      Some(spark.sparkContext.broadcastInternal(relation, serializedOnly = true)
        .asInstanceOf[Broadcast[Any]])
    }
  }

  /** A SubqueryBroadcastExec used purely for DPP needs no payload at all -- DppAnchors already
    * persists the keys it would have extracted from the broadcast. */
  def isDppOnly(name: String): Boolean = name.startsWith("dynamicpruning#")
}
