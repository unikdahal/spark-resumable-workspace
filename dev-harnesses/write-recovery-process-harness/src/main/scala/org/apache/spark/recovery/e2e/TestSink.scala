package org.apache.spark.recovery.e2e

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRecoveryAnchor, SupportsRecoveryWrite, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private object SinkPaths {
  def dataDir(root: String): Path = Paths.get(root)
  def committedMarker(root: String): Path = Paths.get(root).resolve("_COMMITTED")
  def dataFile(root: String, name: String): Path = Paths.get(root).resolve(name)
}

/** Commit message: the single file this partition wrote, and how many rows are in it. */
case class TestCommitMessage(fileName: String, numRows: Long) extends WriterCommitMessage

/**
 * Stable, non-executable codec. Version 1 is `"1\tfileName\tnumRows"` in UTF-8. No Java or Kryo
 * serialization anywhere: a durable recovery record must outlive the class that wrote it.
 */
class TestCommitCodec extends RecoveryCommitMessageCodec {
  override def codecId(): String = "recovery-test-sink"
  override def version(): Int = 1

  override def encode(message: WriterCommitMessage): Array[Byte] = message match {
    case TestCommitMessage(fileName, numRows) =>
      s"1\t$fileName\t$numRows".getBytes(StandardCharsets.UTF_8)
    case other =>
      throw new IllegalArgumentException(s"Unsupported commit message ${other.getClass.getName}")
  }

  override def decode(version: Int, payload: Array[Byte]): WriterCommitMessage = {
    require(version == 1, s"Unsupported recovery-test-sink codec version: $version")
    val parts = new String(payload, StandardCharsets.UTF_8).split("\t")
    require(parts.length == 3 && parts(0) == "1", "Malformed recovery-test-sink payload")
    TestCommitMessage(parts(1), parts(2).toLong)
  }
}

class TestDataWriter(root: String, partitionId: Int, taskId: Long) extends RecoveryDataWriter {
  private val fileName = f"part-$partitionId%05d-$taskId-${UUID.randomUUID()}.data"
  private val buffer = new StringBuilder
  private var rows = 0L

  override def write(record: InternalRow): Unit = {
    buffer.append(record.getLong(0)).append('\n')
    rows += 1
  }

  override def commit(): WriterCommitMessage = {
    val target = SinkPaths.dataFile(root, fileName)
    Files.createDirectories(target.getParent)
    Files.write(
      target,
      buffer.toString.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE)
    println(s"E2E-WRITER-COMMIT partition=$partitionId file=$fileName rows=$rows")
    TestCommitMessage(fileName, rows)
  }

  override def abort(): Unit = {
    println(s"E2E-WRITER-ABORT partition=$partitionId")
    Files.deleteIfExists(SinkPaths.dataFile(root, fileName))
  }

  override def close(): Unit = {}

  override def discardCommittedOutput(committedMessage: WriterCommitMessage): Unit = {
    val discarded = committedMessage.asInstanceOf[TestCommitMessage].fileName
    println(s"E2E-WRITER-DISCARD partition=$partitionId file=$discarded")
    Files.deleteIfExists(SinkPaths.dataFile(root, discarded))
  }
}

class TestWriterFactory(root: String) extends RecoveryDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): RecoveryDataWriter = {
    println(s"E2E-WRITER-CREATED partition=$partitionId")
    new TestDataWriter(root, partitionId, taskId)
  }
}

class TestBatchWrite(root: String, writeQueryId: String, sinkId: String)
  extends SupportsBatchWriteRecovery {

  override def recoveryId(): String = writeQueryId

  override def commitMessageCodec(): RecoveryCommitMessageCodec = new TestCommitCodec

  override def recoveryCompatibilityMetadata(info: PhysicalWriteInfo): Array[Byte] =
    s"sink=$sinkId;partitions=${info.numPartitions()};schema=id:bigint"
      .getBytes(StandardCharsets.UTF_8)

  override def recover(info: PhysicalWriteInfo): BatchWriteRecoveryState = {
    val marker = SinkPaths.committedMarker(root)
    val committed = Files.exists(marker)
    val totalRows =
      if (committed) new String(Files.readAllBytes(marker), StandardCharsets.UTF_8)
        .linesIterator.filter(_.startsWith("rows=")).map(_.drop(5).toLong).sum
      else -1L
    println(s"E2E-RECOVER committed=$committed totalRows=$totalRows " +
      s"partitions=${info.numPartitions()}")
    new BatchWriteRecoveryState {
      // Deliberately empty: this connector keeps no task ledger of its own, so Spark's durable
      // store is the sole authority. That is the configuration the protocol is strictest about.
      override def isCommitted(): Boolean = committed
      override def commitMessages(): Array[WriterCommitMessage] =
        new Array[WriterCommitMessage](info.numPartitions())
      override def numRows(): Array[Long] = Array.fill(info.numPartitions())(-1L)
      override def totalNumRows(): Long = totalRows
    }
  }

  override def abortAfterRecovery(messages: Array[WriterCommitMessage]): Unit = {
    // Preserve every durable commit: a later driver must be able to resume them. Only the global
    // marker would be unsafe to leave behind, and it is written atomically at commit time.
    println("E2E-ABORT-AFTER-RECOVERY (durable task commits preserved)")
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new TestWriterFactory(root)

  override def useCommitCoordinator(): Boolean = true

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val lines = messages.zipWithIndex.map { case (message, index) =>
      val commit = message.asInstanceOf[TestCommitMessage]
      s"partition=$index file=${commit.fileName} rows=${commit.numRows}"
    }
    val payload = lines.mkString("\n").getBytes(StandardCharsets.UTF_8)
    val marker = SinkPaths.committedMarker(root)
    try {
      Files.write(marker, payload, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      println(s"E2E-GLOBAL-COMMIT files=${messages.length}")
    } catch {
      case _: java.nio.file.FileAlreadyExistsException =>
        // Idempotent: an identical replay is a no-op, a different one is a protocol violation.
        val existing = Files.readAllBytes(marker)
        require(util.Arrays.equals(existing, payload),
          "A different global commit already exists for this sink")
        println("E2E-GLOBAL-COMMIT-IDEMPOTENT")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    println("E2E-ABORT (non-recovery path)")
  }
}

class TestSinkTable(root: String, sinkId: String) extends Table
  with SupportsWrite with SupportsRecoveryWrite with SupportsRecoveryAnchor {

  override def name(): String = s"recovery-test-sink($sinkId)"
  override def schema(): StructType = new StructType().add("id", LongType)
  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_WRITE).asJava

  override def recoverySinkId(): String = sinkId
  override def recoverySourceId(): String = sinkId
  override def currentRecoveryAnchor(): String = "static"
  override def withRecoveryAnchor(anchor: String): Table = this

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder {
    override def build(): Write = new Write {
      override def description(): String = s"recovery-test-sink write ${info.queryId()}"
      override def toBatch: BatchWrite = new TestBatchWrite(root, info.queryId(), sinkId)
    }
  }
}

class TestSinkProvider extends TableProvider with DataSourceRegister {
  override def shortName(): String = "recovery-test-sink"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    new StructType().add("id", LongType)

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val root = Option(options.get("path")).getOrElse(
      throw new IllegalArgumentException("recovery-test-sink requires a path"))
    new TestSinkTable(root, Option(options.get("sinkId")).getOrElse(root))
  }

  override def supportsExternalMetadata(): Boolean = true
}
