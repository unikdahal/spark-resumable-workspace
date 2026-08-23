package org.apache.spark.recovery.e2e

import org.apache.spark.sql.catalyst.analysis.{SourceRecoveryInfo, WriteRecoveryInfo}
import org.apache.spark.sql.connector.write.RecoveryTaskCommitStore
import org.apache.spark.sql.execution.adaptive.{RecoveredShuffleStage, ShuffleStageRecovery, ShuffleStageRecoveryInfo}

/**
 * The session-scoped recovery provider for this harness.
 *
 * Shuffle recovery is deliberately not implemented: `tryRecover` returns `None`, which the SPI
 * defines as authoritative permission to recompute the stage. That isolates the write-recovery
 * protocol, which is what this harness exists to prove, from any shuffle-service dependency.
 *
 * Source anchors and write IDs are resolved through an immutable durable binding so that a
 * replacement driver derives the same write identity from the same logical execution.
 */
class HarnessRecoveryProvider(bindingDir: String, executionId: String, store: RecoveryTaskCommitStore)
  extends ShuffleStageRecovery {

  override def tryRecover(info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage] = None

  override def resolveSourceAnchor(info: SourceRecoveryInfo): String =
    DurableBinding.resolve(bindingDir, s"source-$executionId", info.sourceId, info.currentAnchor)

  override def resolveWriteId(info: WriteRecoveryInfo): String =
    DurableBinding.resolve(bindingDir, s"write-$executionId", info.sinkId, info.currentWriteId)

  override def taskCommitStore: Option[RecoveryTaskCommitStore] = Some(store)
}
