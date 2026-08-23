package org.apache.spark.recovery.e2e

import java.io.IOException
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.UUID

import org.apache.spark.sql.connector.write.RecoveryTaskCommitStore

/**
 * A filesystem-backed [[RecoveryTaskCommitStore]] whose only job is to be a *correct* immutable CAS
 * so that the Spark-side protocol can be exercised without Celeborn.
 *
 * The CAS primitive is `Files.move(tmp, target)` **without** `REPLACE_EXISTING`: on a POSIX
 * filesystem that is `rename(2)` with `RENAME_NOREPLACE` semantics via `link`/`rename` — it either
 * publishes the whole record or fails with `FileAlreadyExistsException` because someone else won.
 * A partially written record can never be observed, because the temp file is fully written and
 * fsynced before the rename.
 *
 * Faults are injected through [[FaultPlan]] so the harness can reproduce the failure modes that
 * motivated the design: an accepted publish whose reply is lost, a fenced writer, and a corrupt
 * durable record.
 */
class FileTaskCommitStore(rootPath: String, fault: FaultPlan) extends RecoveryTaskCommitStore {

  @transient private lazy val root: Path = {
    val path = Paths.get(rootPath)
    Files.createDirectories(path)
    path
  }

  private def recordPath(recoveryId: String, partitionId: Int): Path =
    root.resolve(s"${sanitize(recoveryId)}__$partitionId.record")

  private def manifestPath(recoveryId: String): Path =
    root.resolve(s"${sanitize(recoveryId)}__manifest.record")

  private def sanitize(id: String): String = id.replaceAll("[^A-Za-z0-9_.-]", "_")

  /** Publishes bytes under `target` if and only if nothing is there yet; returns the winner. */
  private def compareAndSet(target: Path, value: Array[Byte]): Array[Byte] = {
    val existing = readIfPresent(target)
    if (existing.isDefined) {
      return existing.get
    }
    val tmp = root.resolve(s".tmp-${UUID.randomUUID()}")
    val channel = java.nio.channels.FileChannel.open(
      tmp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    try {
      channel.write(java.nio.ByteBuffer.wrap(value))
      channel.force(true)
    } finally {
      channel.close()
    }
    try {
      // link(2) is atomic and fails with EEXIST if the target already exists, which is exactly
      // first-writer-wins. Files.move(ATOMIC_MOVE) is NOT usable here: on Linux it lowers to
      // rename(2), which silently replaces the target, and the JDK's "target exists" check when
      // REPLACE_EXISTING is absent is a separate stat() -- a TOCTOU race, not a CAS.
      Files.createLink(target, tmp)
      Files.deleteIfExists(tmp)
      value
    } catch {
      case _: java.nio.file.FileAlreadyExistsException =>
        Files.deleteIfExists(tmp)
        readIfPresent(target).getOrElse(
          throw new IOException(s"Lost the CAS for $target but no record is readable"))
      case e: UnsupportedOperationException =>
        Files.deleteIfExists(tmp)
        throw new IOException(
          s"Filesystem under $root does not support hard links, so it cannot provide an " +
            "immutable compare-and-set. Point the harness at a normal local filesystem.", e)
      case e: Throwable =>
        Files.deleteIfExists(tmp)
        throw e
    }
  }

  private def readIfPresent(target: Path): Option[Array[Byte]] =
    if (Files.exists(target)) Some(Files.readAllBytes(target)) else None

  override def resolveWriteManifest(recoveryId: String, proposedValue: Array[Byte]): Array[Byte] = {
    fault.beforeManifest(recoveryId)
    compareAndSet(manifestPath(recoveryId), proposedValue)
  }

  override def load(recoveryId: String, partitionIds: Array[Int]): Array[Array[Byte]] = {
    fault.beforeLoad(recoveryId, partitionIds)
    val loaded = partitionIds.map { partitionId =>
      readIfPresent(recordPath(recoveryId, partitionId))
        .map(bytes => fault.onRead(partitionId, bytes))
        .orNull
    }
    if (partitionIds.length == 1) {
      // Single-key loads only happen in the executor preflight, so this marker distinguishes
      // "the retry adopted a canonical commit" from "the retry wrote the partition again".
      val outcome = if (loaded.head == null) "MISS" else "HIT"
      println(s"E2E-STORE-PREFLIGHT-$outcome partition=${partitionIds.head}")
    }
    loaded
  }

  override def publish(
      recoveryId: String,
      partitionId: Int,
      taskAttemptId: Long,
      attemptNumber: Int,
      value: Array[Byte]): Array[Byte] = {
    fault.beforePublish(partitionId, attemptNumber)
    val canonical = compareAndSet(recordPath(recoveryId, partitionId), value)
    fault.afterPublish(partitionId, attemptNumber)
    canonical
  }
}
