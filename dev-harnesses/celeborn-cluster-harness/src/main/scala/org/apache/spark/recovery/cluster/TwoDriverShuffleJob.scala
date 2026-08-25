package org.apache.spark.recovery.cluster

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

/**
 * One driver of the two-driver shuffle-recovery scenario. Both drivers run this program with
 * identical arguments and the same recovery identity; only KILL_AFTER_STAGES differs.
 *
 * Arguments: celebornMaster recoveryId appUniqueId leaseDuration killAfterStages outputPath
 *
 * The measurement is task counts per stage, printed as machine-greppable markers. Wall time is not
 * evidence: a stage that is adopted submits zero map tasks, and that is the only thing worth
 * asserting.
 */
object TwoDriverShuffleJob {

  def main(args: Array[String]): Unit = {
    val Array(master, recoveryId, appUniqueId, leaseDuration, killAfterStages, outputPath) = args

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName(s"two-driver-shuffle-$recoveryId")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .config("spark.celeborn.master.endpoints", master)
      .config("spark.celeborn.client.application.uniqueId", appUniqueId)
      .config("spark.celeborn.driverRecovery.enabled", "true")
      .config("spark.celeborn.auth.enabled", "true")
      .config("spark.celeborn.driverRecovery.id", recoveryId)
      .config("spark.celeborn.driverRecovery.leaseDuration", leaseDuration)
      .config("spark.celeborn.driverRecovery.probeTimeout", "5s")
      .config("spark.sql.extensions",
        "org.apache.spark.shuffle.celeborn.CelebornShuffleStageRecoveryExtension")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    val tasksByStage = TrieMap.empty[Int, AtomicInteger]
    val completedStages = new AtomicInteger(0)
    val killAfter = killAfterStages.toInt

    spark.sparkContext.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
        tasksByStage.getOrElseUpdate(taskEnd.stageId, new AtomicInteger(0)).incrementAndGet()

      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val stageId = stageCompleted.stageInfo.stageId
        val tasks = tasksByStage.get(stageId).map(_.get()).getOrElse(0)
        println(s"E2E-STAGE-COMPLETED stage=$stageId tasks=$tasks " +
          s"name=${stageCompleted.stageInfo.name}")
        val finished = completedStages.incrementAndGet()
        if (killAfter > 0 && finished >= killAfter) {
          // The stage's shuffle output is committed and its catalog published before this callback
          // returns, so halting here is a driver crash at a deterministic protocol point.
          println(s"E2E-HALT after $finished completed stages")
          Runtime.getRuntime.halt(137)
        }
      }
    })

    try {
      import spark.implicits._
      val facts = spark.range(0, 200000).select(($"id" % 1000).as("key"), $"id".as("value"))
      val aggregated = facts.groupBy($"key").sum("value")
      val joined = aggregated.join(
        spark.range(0, 1000).select($"id".as("key"), ($"id" * 2).as("weight")), "key")
      val rows = joined.selectExpr("key", "`sum(value)` as total", "weight").orderBy("key")

      rows.write.mode("overwrite").json(outputPath)
      val total = spark.read.json(outputPath).count()
      println(s"E2E-RESULT SUCCESS rows=$total " +
        s"stageTasks=${tasksByStage.toSeq.sortBy(_._1).map { case (s, c) => s"$s:${c.get()}" }
          .mkString(",")}")
    } catch {
      case throwable: Throwable =>
        println(s"E2E-RESULT FAILURE error=${throwable.getClass.getName}: ${throwable.getMessage}")
        throw throwable
    } finally {
      spark.stop()
    }
  }
}
