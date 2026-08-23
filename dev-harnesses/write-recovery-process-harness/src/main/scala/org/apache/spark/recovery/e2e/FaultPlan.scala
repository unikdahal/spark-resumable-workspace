package org.apache.spark.recovery.e2e

import java.io.IOException
import java.nio.file.{Files, Path, Paths}

/**
 * Deterministic fault injection for [[FileTaskCommitStore]].
 *
 * Every fault is expressed as `name:argument` so a scenario is one environment variable and the
 * harness never depends on a sleep or a race to reach a failure point. Faults that must fire
 * exactly once across task attempts (and across driver processes) record that they fired in a
 * marker file, because a JVM-local flag would not survive the crash the scenario is testing.
 */
case class FaultPlan(spec: String, markerDir: String) extends Serializable {

  private val (name, argument) = spec.split(":", 2) match {
    case Array(n) => (n, "")
    case Array(n, a) => (n, a)
  }

  private def markerPath(tag: String): Path = Paths.get(markerDir).resolve(s".fault-$tag")

  /** True the first time it is called for `tag` in the lifetime of this store directory. */
  private def fireOnce(tag: String): Boolean = {
    val marker = markerPath(tag)
    try {
      Files.createDirectories(marker.getParent)
      Files.createFile(marker)
      true
    } catch {
      case _: java.nio.file.FileAlreadyExistsException => false
    }
  }

  private def partitionArgument: Int =
    if (argument.isEmpty) -1 else argument.toInt

  def beforeManifest(recoveryId: String): Unit = name match {
    case "fail-manifest" => throw new IOException("injected manifest failure")
    case _ =>
  }

  def beforeLoad(recoveryId: String, partitionIds: Array[Int]): Unit = name match {
    case "fail-load" => throw new IOException("injected load failure")
    case _ =>
  }

  def onRead(partitionId: Int, bytes: Array[Byte]): Array[Byte] = name match {
    case "corrupt-on-read" if partitionId == partitionArgument =>
      val corrupted = bytes.clone()
      // Flip a byte inside the connector payload: the envelope's SHA-256 covers exactly this
      // region, so the decoder must reject it with a checksum error rather than an identity error.
      val index = math.max(0, corrupted.length - 33)
      corrupted(index) = (corrupted(index) ^ 0x01).toByte
      corrupted
    case _ => bytes
  }

  def beforePublish(partitionId: Int, attemptNumber: Int): Unit = name match {
    case "fail-before-publish" if partitionId == partitionArgument && fireOnce(s"pre-$partitionId") =>
      throw new IOException(s"injected pre-publish failure for partition $partitionId")
    case _ =>
  }

  def afterPublish(partitionId: Int, attemptNumber: Int): Unit = name match {
    case "drop-reply-after-accept"
        if partitionId == partitionArgument && fireOnce(s"post-$partitionId") =>
      // The record is durable; only the answer is lost. The task must fail, and its retry must
      // discover the canonical value in preflight instead of writing the partition again.
      throw new IOException(s"injected lost reply after accepting partition $partitionId")
    case "halt-after-commits" =>
      val committed = Option(Paths.get(markerDir).toFile.listFiles())
        .map(_.count(f => f.getName.endsWith(".record") && !f.getName.contains("manifest")))
        .getOrElse(0)
      if (committed >= argument.toInt) {
        // Local mode: driver and executor share a JVM, so halting here is a driver SIGKILL with
        // exactly `argument` partitions durably committed. halt(), not exit(): no shutdown hooks,
        // no graceful abort, nothing that a real crash would not do.
        System.err.println(s"E2E-HALT: $committed durable commits, halting the driver")
        Runtime.getRuntime.halt(137)
      }
    case _ =>
  }
}

object FaultPlan {
  val NoFault: String = "none"
  def of(spec: String, markerDir: String): FaultPlan =
    new FaultPlan(if (spec == null || spec.isEmpty) NoFault else spec, markerDir)
}
