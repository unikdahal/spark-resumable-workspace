package org.apache.spark.resume

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigBuilder

/** LLD §9. Four levels: arm, capture, restore, per-capability.
  *
  * INVARIANT M-1: turning any flag OFF can only reduce the set of adopted anchors, never widen
  * it. Assert this as a property test, not in review. The trap is real -- disabling dpp.enabled
  * must REJECT DPP-bearing anchors, not silently accept them on the grounds that DPP is "not in
  * play". */
object ResumeConf {

  sealed trait Mode
  case object Off     extends Mode
  case object Capture extends Mode
  case object Restore extends Mode
  /** Capture normally; on restart compute every digest and run every rung of the acceptance
    * ladder WHILE ADOPTING NOTHING, then report what would have hit and why the rest failed.
    * Hit rate and drift characteristics with zero correctness exposure. Run this for a month
    * before anyone enables Restore -- the rejection histogram it produces is the only honest
    * evidence the feature would ever pay for itself. */
  case object Shadow  extends Mode

  val ENABLED   = ConfigBuilder("spark.resume.enabled").booleanConf.createWithDefault(false)
  val MODE      = ConfigBuilder("spark.resume.mode").stringConf
    .checkValues(Set("off", "capture", "restore", "shadow")).createWithDefault("off")
  val QUERY_ID  = ConfigBuilder("spark.resume.queryId").stringConf.createOptional
  val FAIL_FAST = ConfigBuilder("spark.resume.failFast").booleanConf.createWithDefault(true)

  // capability gates
  val AQE_ENABLED     = ConfigBuilder("spark.resume.aqe.enabled").booleanConf.createWithDefault(true)
  val AQE_PIN_SPECS   = ConfigBuilder("spark.resume.aqe.pinPartitionSpecs").booleanConf.createWithDefault(true)
  val NON_AQE         = ConfigBuilder("spark.resume.nonAqe.enabled").booleanConf.createWithDefault(true)
  val DPP_ENABLED     = ConfigBuilder("spark.resume.dpp.enabled").booleanConf.createWithDefault(true)
  val DPP_INJECT      = ConfigBuilder("spark.resume.dpp.injectSubqueryResults").booleanConf.createWithDefault(true)
  val REQ_SNAPSHOT    = ConfigBuilder("spark.resume.scan.requirePinnedSnapshot").booleanConf.createWithDefault(true)
  val SPLIT_CAPTURE   = ConfigBuilder("spark.resume.scan.allowSplitCapture").booleanConf.createWithDefault(true)
  val MAX_SPLIT_BYTES = ConfigBuilder("spark.resume.scan.maxSplitBytes").longConf.createWithDefault(64L << 20)
  val VALIDATE_SPLITS = ConfigBuilder("spark.resume.scan.validateSplits").stringConf
    .checkValues(Set("exists", "sample", "none")).createWithDefault("sample")
  val MAX_FILES       = ConfigBuilder("spark.resume.scan.fileDigest.maxFiles").intConf.createWithDefault(200000)
  val BCAST_ANCHOR    = ConfigBuilder("spark.resume.broadcast.payloadAnchoring").booleanConf.createWithDefault(true)
  val BCAST_MAX_BYTES = ConfigBuilder("spark.resume.broadcast.maxPayloadBytes").longConf.createWithDefault(64L << 20)
  val STRICT_DETERM   = ConfigBuilder("spark.resume.determinism.strict").booleanConf.createWithDefault(true)
  val ALLOW_COND      = ConfigBuilder("spark.resume.determinism.allowConditional").booleanConf.createWithDefault(true)
  val REPLAY_CONSTS   = ConfigBuilder("spark.resume.constants.replay").booleanConf.createWithDefault(true)
  /** Highest value and highest risk in the package: the only thing that makes the FINAL stage of
    * a long ETL job resumable, and the only place a mistake writes wrong data to a table rather
    * than returning a wrong result. Off until everything else is boring. */
  val WRITE_ANCHOR    = ConfigBuilder("spark.resume.write.commitMessageAnchoring").booleanConf.createWithDefault(false)
  val RESULT_MAX      = ConfigBuilder("spark.resume.result.maxBytes").longConf.createWithDefault(64L << 20)
  val MIN_STAGE_MS    = ConfigBuilder("spark.resume.anchorMinStageDurationMs").longConf.createWithDefault(60000L)
  val MAX_ANCHORS     = ConfigBuilder("spark.resume.maxAnchorsPerQuery").intConf.createWithDefault(2000)
  val COMET_NATIVE    = ConfigBuilder("spark.resume.comet.allowNativeShuffleAdoption").booleanConf.createWithDefault(false)

  // store and timing
  val STORE           = ConfigBuilder("spark.resume.store").stringConf.createOptional
  val BATCH_SIZE      = ConfigBuilder("spark.resume.store.batchSize").intConf.createWithDefault(200)
  val QUEUE_CAPACITY  = ConfigBuilder("spark.resume.store.queueCapacity").intConf.createWithDefault(100000)
  val ON_UNAVAILABLE  = ConfigBuilder("spark.resume.store.onUnavailable").stringConf
    .checkValues(Set("degrade", "fail")).createWithDefault("degrade")
  val TTL_SECONDS     = ConfigBuilder("spark.resume.store.ttlSeconds").longConf.createWithDefault(43200L)
  val MIN_RES_RATIO   = ConfigBuilder("spark.resume.restore.minRegisteredResourcesRatio").doubleConf.createWithDefault(0.8)
  val MAX_WAIT_MS     = ConfigBuilder("spark.resume.restore.maxWaitMs").longConf.createWithDefault(120000L)

  /** Includes CONNECTOR versions, not just the Spark version: PinnedSplits persists
    * connector-internal serialised objects, and a connector upgrade can break deserialisation
    * (LLD §5.2). A mismatch is a hard refusal, never a best-effort attempt. */
  def schemaVersion(sparkVersion: String, connectorVersions: Map[String, String]): String =
    s"resume-1/$sparkVersion/" + connectorVersions.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(";")
}

/** LLD §9.2 -- evaluated once, before any job is submitted. Under failFast a violation aborts
  * context init rather than degrading silently, because silent degradation is how a resume
  * feature becomes a resume feature that never fires and nobody notices for six months. */
class ResumePreconditions(conf: SparkConf) extends Logging {
  case class Violation(id: String, message: String)

  def check(
      supportsReliableStorage: Boolean,
      appIdentity: String,
      celebornAppUniqueId: Option[String],
      celebornUuidSuffixEnabled: Boolean,
      celebornStageRerunEnabled: Boolean,
      driverIdentityStable: Boolean,
      unpinnableLeaves: Seq[String],
      pushBasedShuffleEnabled: Boolean,
      localShuffleReaderEnabled: Boolean,
      storeSchemaVersion: Option[String],
      runtimeSchemaVersion: String): Seq[Violation] = {
    val v = Seq.newBuilder[Violation]

    if (!supportsReliableStorage) v += Violation("P1",
      "ShuffleDriverComponents.supportsReliableStorage()==false: restored MapStatus.loc would " +
      "address dead executors. Under Celeborn this means shuffleForceFallbackEnabled is on " +
      "(CelebornShuffleDataIO.java:61).")

    val qid = conf.get(ResumeConf.QUERY_ID).orNull
    if (qid == null || qid != appIdentity) v += Violation("P2",
      s"spark.resume.queryId ($qid) must equal the application identity ($appIdentity).")
    celebornAppUniqueId.filter(_ != appIdentity).foreach { c => v += Violation("P2",
      s"celeborn appUniqueId ($c) != application identity ($appIdentity). Note SparkUtils.java:136-141 " +
      "appends an attempt id when one exists, which makes adoption structurally impossible on " +
      "cluster managers that increment it across driver restarts.") }

    if (celebornUuidSuffixEnabled) v += Violation("P3",
      "celeborn.client.application.uuidSuffix.enabled must be false (CelebornConf.scala:978-984).")

    if (!celebornStageRerunEnabled) v += Violation("P4",
      "celeborn.client.spark.stageRerun.enabled must be true. Without it SparkUtils.java:143-165 " +
      "returns handle.shuffleId() directly: no shuffleIdMapping indirection exists to seed, and " +
      "adoption would require reissuing identical Spark shuffle IDs in identical order.")

    if (!driverIdentityStable) v += Violation("P5",
      "driver identity must survive restart: spark.driver.port pinned, driver Service not owned " +
      "by the driver pod, executor pods not cascade-deleted via ownerReference.")

    if (conf.get(ResumeConf.REQ_SNAPSHOT) && unpinnableLeaves.nonEmpty) v += Violation("P6",
      s"unpinnable leaves: ${unpinnableLeaves.mkString(", ")}")

    storeSchemaVersion.filter(_ != runtimeSchemaVersion).foreach { s => v += Violation("P7",
      s"anchor store schema $s != runtime $runtimeSchemaVersion; refusing cross-version restore.") }

    if (pushBasedShuffleEnabled) v += Violation("P8",
      "push-based shuffle maintains its own merge state (shuffleMergeId, mergerLocs, " +
      "finalisation) that adoption does not reconstruct.")

    if (localShuffleReaderEnabled) v += Violation("P9",
      "local shuffle reader assumes co-location with the mapper host, which is meaningless with " +
      "a remote shuffle service.")

    v.result()
  }

  def enforce(violations: Seq[Violation]): Unit = if (violations.nonEmpty) {
    val msg = violations.map(x => s"  [${x.id}] ${x.message}").mkString("\n")
    if (conf.get(ResumeConf.FAIL_FAST)) throw new IllegalStateException(s"spark.resume preconditions failed:\n$msg")
    else logWarning(s"spark.resume disabled; preconditions failed:\n$msg")
  }
}
