package org.apache.spark.sql.execution.datasources.v2

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.apache.spark.recovery.e2e.{FaultPlan, FileTaskCommitStore, TestCommitMessage}
import org.apache.spark.sql.connector.write.{RecoveryCommitMessageCodec, WriterCommitMessage}

/**
 * Measures what a recoverable write actually costs at scale: durable bytes per partition, envelope
 * encode/decode throughput, and the cost of the driver-side batched load that every replacement
 * driver pays before it can schedule anything.
 *
 * The numbers matter because the current Celeborn backend stores these envelopes inline in
 * replicated master state, so "bytes per partition x partitions" is a hard admission bound, not an
 * efficiency concern.
 *
 * Arguments: workDir partitionCounts payloadBytes [storeMaxPartitions]
 *   e.g. /tmp/bench 1000,10000,100000 512 20000
 * `storeMaxPartitions` caps how many records are actually written to the filesystem store, so the
 * encode/decode measurements can run at a scale the local disk should not be asked to reproduce.
 */
object RecoveryScaleBenchmark {

  private class FixedPayloadCodec(payloadBytes: Int) extends RecoveryCommitMessageCodec {
    private val filler = "f" * math.max(0, payloadBytes - 32)
    override def codecId(): String = "scale-benchmark"
    override def version(): Int = 1
    override def encode(message: WriterCommitMessage): Array[Byte] = {
      val commit = message.asInstanceOf[TestCommitMessage]
      s"1\t${commit.fileName}\t${commit.numRows}\t$filler".getBytes(StandardCharsets.UTF_8)
    }
    override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = {
      val parts = new String(payload, StandardCharsets.UTF_8).split("\t")
      TestCommitMessage(parts(1), parts(2).toLong)
    }
  }

  private def timed[T](body: => T): (T, Long) = {
    val start = System.nanoTime()
    val result = body
    (result, System.nanoTime() - start)
  }

  def main(args: Array[String]): Unit = {
    val workDir = args(0)
    val partitionCounts = args(1).split(",").map(_.trim.toInt)
    val payloadBytes = args(2).toInt
    val storeMaxPartitions = if (args.length > 3) args(3).toInt else 20000

    println("partitions\tpayloadBytes\tenvelopeBytes\ttotalDurableMiB\tencodeMsPerK\t" +
      "decodeMsPerK\tstoreWriteMsPerK\tbatchLoadMs\tmanifestBytes")

    partitionCounts.foreach { partitions =>
      val root: Path = Paths.get(workDir).resolve(s"scale-$partitions-$payloadBytes")
      if (Files.exists(root)) {
        Files.walk(root).sorted(java.util.Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      }
      Files.createDirectories(root)

      val codec = new FixedPayloadCodec(payloadBytes)
      val store = new FileTaskCommitStore(root.toString, FaultPlan.of(FaultPlan.NoFault, root.toString))
      val recoveryId = s"scale-execution-$partitions"
      val context = RecoveryTaskCommitContext(store, recoveryId, codec)

      val metadata = ("sink=benchmark;" + ("m" * 256)).getBytes(StandardCharsets.UTF_8)
      val manifest = RecoveryTaskCommitEnvelope.writeManifest(context, partitions, metadata)

      val message = TestCommitMessage("part-00000-benchmark.data", 1000L)
      val sample = RecoveryTaskCommitEnvelope.encode(context, 0, message, 1000L)
      val envelopeBytes = sample.length
      val totalDurableMiB = (envelopeBytes.toLong * partitions) / (1024.0 * 1024.0)

      val (_, encodeNanos) = timed {
        var partition = 0
        while (partition < partitions) {
          RecoveryTaskCommitEnvelope.encode(context, partition, message, 1000L)
          partition += 1
        }
      }
      val (_, decodeNanos) = timed {
        var partition = 0
        while (partition < partitions) {
          RecoveryTaskCommitEnvelope.decode(
            context, partition, RecoveryTaskCommitEnvelope.encode(context, partition, message, 1000L))
          partition += 1
        }
      }

      val stored = math.min(partitions, storeMaxPartitions)
      val (_, storeNanos) = timed {
        var partition = 0
        while (partition < stored) {
          store.publish(recoveryId, partition, partition.toLong, 0,
            RecoveryTaskCommitEnvelope.encode(context, partition, message, 1000L))
          partition += 1
        }
      }

      // The driver-side load Spark performs before scheduling: bounded batches of 1024.
      val (_, loadNanos) = timed {
        (0 until stored).grouped(1024).foreach { batch =>
          val ids = batch.toArray
          val values = store.load(recoveryId, ids)
          var index = 0
          while (index < ids.length) {
            if (values(index) != null) {
              RecoveryTaskCommitEnvelope.decode(context, ids(index), values(index))
            }
            index += 1
          }
        }
      }

      def msPerThousand(nanos: Long, count: Int): String =
        f"${nanos / 1e6 / math.max(1, count) * 1000}%.1f"

      println(f"$partitions\t$payloadBytes\t$envelopeBytes\t$totalDurableMiB%.1f\t" +
        s"${msPerThousand(encodeNanos, partitions)}\t${msPerThousand(decodeNanos, partitions)}\t" +
        s"${msPerThousand(storeNanos, stored)}\t${loadNanos / 1000000}\t${manifest.length}")

      Files.walk(root).sorted(java.util.Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
    }
  }
}
