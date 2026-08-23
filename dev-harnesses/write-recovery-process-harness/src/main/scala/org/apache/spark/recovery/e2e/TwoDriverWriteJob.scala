package org.apache.spark.recovery.e2e

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

/**
 * One driver process of the two-driver write-recovery scenario. Both drivers run this program with
 * identical arguments; only the injected fault differs.
 *
 * Arguments: storeDir sinkDir executionId numPartitions numRows faultSpec
 */
object TwoDriverWriteJob {

  def main(args: Array[String]): Unit = {
    val Array(storeDir, sinkDir, executionId, numPartitions, numRows, faultSpec) = args

    val fault = FaultPlan.of(faultSpec, storeDir)
    val store = new FileTaskCommitStore(storeDir, fault)
    val provider = new HarnessRecoveryProvider(storeDir, executionId, store)

    val spark = SparkSession.builder()
      .master("local[2, 4]")
      .appName(s"write-recovery-$executionId")
      .config("spark.sql.shuffle.partitions", numPartitions)
      .config("spark.ui.enabled", "false")
      .withExtensions(_.injectShuffleStageRecovery(_ => provider))
      .getOrCreate()

    val tasks = new java.util.concurrent.atomic.AtomicInteger(0)
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = tasks.incrementAndGet()
    })

    try {
      spark.range(0, numRows.toLong)
        .repartition(numPartitions.toInt)
        .write
        .format("recovery-test-sink")
        .option("path", sinkDir)
        .option("sinkId", "harness-sink")
        .mode("append")
        .save()
      println(s"E2E-RESULT SUCCESS tasks=${tasks.get()}")
    } catch {
      case throwable: Throwable =>
        println(s"E2E-RESULT FAILURE tasks=${tasks.get()} " +
          s"error=${throwable.getClass.getName}: ${throwable.getMessage}")
        throw throwable
    } finally {
      spark.stop()
    }
  }
}
