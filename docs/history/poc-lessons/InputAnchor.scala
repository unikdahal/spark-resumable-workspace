package org.apache.spark.resume

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/** LLD §3.2 -- THE CORRECTNESS BOUNDARY, and the reason DPP is solvable.
  *
  * Pinning DPP subquery results is necessary and NOT sufficient. For DSv2:
  *
  *   BatchScanExec.scala:60-112
  *     val filterableScan = scan.asInstanceOf[SupportsRuntimeV2Filtering]
  *     filterableScan.filter(dataSourceFilters.toArray)   // MUTATES the connector's Scan
  *     val newPartitions = scan.toBatch.planInputPartitions()
  *
  * The pruned set is produced INSIDE the connector from its own snapshot resolution. Spark can
  * neither observe, reconstruct, nor constrain it. Pinning the keys constrains the connector's
  * INPUT; it says nothing about its OUTPUT. For v1 sources the same hole exists in a different
  * shape: DataSourceScanExec.scala:280 filters a LIVE catalog listing, so an identical predicate
  * over a mutated table is still a different scan.
  *
  * So the boundary sits on what the leaf RESOLVED TO. */
sealed trait PinStrength { def rank: Int }
/** Force the recorded read version; divergence becomes impossible rather than merely detected. */
case object PinnedSnapshot extends PinStrength { val rank = 3 }
/** Replay the captured splits, bypassing the connector's planner entirely (LLD §5.2).
  * InputPartition.java:38 -- `public interface InputPartition extends Serializable` -- makes this
  * legal for EVERY DSv2 source, by contract, not by luck. */
case object PinnedSplits   extends PinStrength { val rank = 2 }
/** Re-list and compare. Detects drift; cannot prevent it. */
case object PinnedFileList extends PinStrength { val rank = 1 }
/** Never anchored. Fail closed. */
case object Unpinnable     extends PinStrength { val rank = 0 }

case class InputAnchor(
    leafId: String,                    // structural path, NEVER an object identity
    kind: String,                      // V1_FILE | V2_VERSIONED | V2_GENERIC | INMEMORY | RANGE
    identity: String,                  // resolved relation identity, not the user-written name
    pinning: PinStrength,
    snapshotToken: Option[String],
    splitManifest: Option[Array[Byte]],
    fileDigest: Option[String],
    schemaDigest: String,
    pruningDigest: Option[String],
    numFiles: Long,
    numSplits: Long,
    totalBytes: Long) {

  def digest: String = InputAnchor.sha(Seq(
    leafId, kind, identity, pinning.toString,
    snapshotToken.getOrElse("-"), fileDigest.getOrElse("-"),
    schemaDigest, pruningDigest.getOrElse("-"),
    numFiles.toString, numSplits.toString, totalBytes.toString).mkString("\u0001"))
}

object InputAnchor {

  def sha(s: String): String =
    MessageDigest.getInstance("SHA-256").digest(s.getBytes(UTF_8)).take(16)
      .map(b => f"${b & 0xff}%02x").mkString

  /** Acceptance-ladder rung 6. Cheap tripwires first: on a 200k-file table the listing digest
    * costs more than the recompute it is protecting. */
  def verify(recorded: InputAnchor, current: InputAnchor): Either[String, Unit] = {
    if (recorded.identity != current.identity)
      Left(s"relation identity ${recorded.identity} -> ${current.identity}")
    else if (recorded.schemaDigest != current.schemaDigest)
      Left("read schema / partition spec changed")
    else if (recorded.numFiles != current.numFiles)
      Left(s"numFiles ${recorded.numFiles} -> ${current.numFiles}")
    else if (recorded.totalBytes != current.totalBytes)
      Left(s"totalBytes ${recorded.totalBytes} -> ${current.totalBytes}")
    else if (recorded.pruningDigest != current.pruningDigest)
      Left("DPP pruning keys drifted")
    else recorded.pinning match {
      case Unpinnable => Left("leaf is unpinnable")
      case PinnedSnapshot =>
        if (recorded.snapshotToken != current.snapshotToken) Left("snapshot drifted despite pin")
        else if (recorded.fileDigest != current.fileDigest) Left("files drifted under a pinned snapshot")
        else Right(())
      case PinnedSplits =>
        // Splits are replayed, so the current listing is irrelevant BY DESIGN -- that is the
        // whole point. What remains is (a) semantic drift in the dimension side, covered by
        // pruningDigest above, and (b) files deleted out from under the manifest, which fails
        // loudly at read time and is pre-screened by validateSplits.
        if (recorded.splitManifest.isEmpty) Left("split manifest missing") else Right(())
      case PinnedFileList =>
        if (recorded.fileDigest != current.fileDigest) Left("file listing drifted") else Right(())
    }
  }

  def extract(
      plan: SparkPlan,
      leafId: String,
      readVersion: AnyRef => Option[String],          // connector SPI: currentReadVersion()
      captureSplits: SparkPlan => Option[Array[Byte]],
      listing: SparkPlan => Option[(Long, Long, String)],   // (numFiles, bytes, fileDigest)
      schemaDigestOf: SparkPlan => String,
      maxFilesForDigest: Int,
      maxSplitBytes: Long,
      allowSplitCapture: Boolean): InputAnchor = plan match {

    case f: FileSourceScanExec =>
      val (n, bytes, fd) = listing(f).getOrElse((-1L, -1L, ""))
      val splits = if (allowSplitCapture) captureSplits(f).filter(_.length <= maxSplitBytes) else None
      InputAnchor(leafId, "V1_FILE",
        f.tableIdentifier.map(_.unquotedString)
          .getOrElse(f.relation.location.rootPaths.sorted.mkString(",")),
        strength(None, splits, n, maxFilesForDigest),
        None, splits, if (n >= 0) Some(fd) else None, schemaDigestOf(f),
        pruningDigest(f.partitionFilters), n, splits.size.toLong, bytes)

    case b: BatchScanExec =>
      // IMPORTANT: identity must NOT derive from scan.hashCode or batch.hashCode.
      // BatchScanExec.scala:57 overrides hashCode to Objects.hashCode(batch, runtimeFilters) and
      // `batch` is a connector object whose hashCode may be identity-based -- which would make
      // the key JVM-instance-dependent and every anchor miss on every restart (LLD §3.1).
      val snap = readVersion(b.scan)
      val (n, bytes, fd) = listing(b).getOrElse((-1L, -1L, ""))
      val splits = if (allowSplitCapture) captureSplits(b).filter(_.length <= maxSplitBytes) else None
      InputAnchor(leafId, if (snap.isDefined) "V2_VERSIONED" else "V2_GENERIC",
        Option(b.table).map(_.name()).getOrElse("<unnamed-v2>"),
        strength(snap, splits, n, maxFilesForDigest),
        snap, splits, if (n >= 0) Some(fd) else None, schemaDigestOf(b),
        pruningDigest(b.runtimeFilters), n, splits.size.toLong, bytes)

    case other =>
      InputAnchor(leafId, other.nodeName, other.nodeName, Unpinnable,
        None, None, None, schemaDigestOf(other), None, -1L, 0L, -1L)
  }

  private def strength(snap: Option[String], splits: Option[Array[Byte]],
                       numFiles: Long, maxFiles: Int): PinStrength =
    if (snap.isDefined) PinnedSnapshot
    else if (splits.isDefined) PinnedSplits
    else if (numFiles >= 0 && numFiles <= maxFiles) PinnedFileList
    else Unpinnable

  /** Type-tagged and sorted, so ordering never matters and Long 1 never collides with String "1".
    * Reads InSubqueryExec.values() -- the accessor Spark documents as "used only by DPP". */
  def pruningDigest(filters: Seq[Expression]): Option[String] = {
    val subs = filters.collect { case DynamicPruningExpression(e) => e }
      .flatMap(_.collect { case in: InSubqueryExec => in })
    if (subs.isEmpty) return None
    val parts = subs.flatMap(_.values().map(_.toSeq.map(v =>
      s"${if (v == null) "null" else v.getClass.getSimpleName}:${String.valueOf(v)}").sorted))
    if (parts.isEmpty) None else Some(sha(parts.map(_.mkString(",")).sorted.mkString("\u0002")))
  }
}
