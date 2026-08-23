package org.apache.spark.sql.execution.datasources.v2

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Base64

import org.apache.spark.recovery.e2e.{TestCommitCodec, TestCommitMessage}
import org.apache.spark.sql.connector.write.{RecoveryTaskCommitStore, WriterCommitMessage}

/**
 * Format-compatibility gate for the Spark-owned task commit envelope.
 *
 * `record` writes two base64 fixtures produced by the current code. `verify` decodes those fixtures
 * with the current code and re-encodes them, so a change to the envelope layout, the digest scope,
 * the field order, or the manifest's determinism fails here instead of failing silently against a
 * durable store written by an earlier build.
 *
 * The envelope is `private[sql]`, which is why this file lives in Spark's package.
 */
object EnvelopeCompatibilityCheck {

  private val RecoveryId = "compat-execution-1"
  private val PartitionId = 3
  private val NumRows = 4242L
  private val NumPartitions = 8
  private val Metadata = "sink=compat;schema=id:bigint".getBytes(StandardCharsets.UTF_8)
  private val Message: WriterCommitMessage = TestCommitMessage("part-00003.data", NumRows)

  private object UnusedStore extends RecoveryTaskCommitStore {
    override def resolveWriteManifest(recoveryId: String, proposedValue: Array[Byte]): Array[Byte] =
      proposedValue
    override def load(recoveryId: String, partitionIds: Array[Int]): Array[Array[Byte]] =
      Array.fill(partitionIds.length)(null)
    override def publish(
        recoveryId: String,
        partitionId: Int,
        taskAttemptId: Long,
        attemptNumber: Int,
        value: Array[Byte]): Array[Byte] = value
  }

  private def context = RecoveryTaskCommitContext(UnusedStore, RecoveryId, new TestCommitCodec)

  def main(args: Array[String]): Unit = {
    val Array(mode, fixturePath) = args
    val envelope = RecoveryTaskCommitEnvelope.encode(context, PartitionId, Message, NumRows)
    val manifest = RecoveryTaskCommitEnvelope.writeManifest(context, NumPartitions, Metadata)

    mode match {
      case "record" =>
        val text = Seq(
          "envelope=" + Base64.getEncoder.encodeToString(envelope),
          "manifest=" + Base64.getEncoder.encodeToString(manifest)).mkString("\n")
        Files.write(Paths.get(fixturePath), text.getBytes(StandardCharsets.UTF_8))
        println(s"COMPAT-RECORDED $fixturePath")

      case "verify" =>
        val fields = new String(Files.readAllBytes(Paths.get(fixturePath)), StandardCharsets.UTF_8)
          .linesIterator.map(_.split("=", 2)).map(parts => parts(0) -> parts(1)).toMap
        val storedEnvelope = Base64.getDecoder.decode(fields("envelope"))
        val storedManifest = Base64.getDecoder.decode(fields("manifest"))

        val decoded = RecoveryTaskCommitEnvelope.decode(context, PartitionId, storedEnvelope)
        require(decoded.message == Message, s"decoded message changed: ${decoded.message}")
        require(decoded.numRows == NumRows, s"decoded row count changed: ${decoded.numRows}")
        println("COMPAT-DECODE-OK stored envelope still decodes to the same commit message")

        require(java.util.Arrays.equals(storedEnvelope, envelope),
          "the envelope produced by the current code differs byte-for-byte from the fixture; " +
            "the format changed and every durable record written by an earlier build is now " +
            "unreadable unless a readable-version set is introduced")
        require(java.util.Arrays.equals(storedManifest, manifest),
          "the write manifest produced by the current code differs from the fixture; a " +
            "replacement driver would fail its manifest comparison against records written by " +
            "an earlier build")
        println("COMPAT-BYTES-OK envelope and manifest are byte-identical to the fixture")

      case other =>
        throw new IllegalArgumentException(s"Unknown mode: $other")
    }
  }
}
