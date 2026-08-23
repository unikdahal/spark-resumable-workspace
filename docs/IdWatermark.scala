package org.apache.spark.resume

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.spark.internal.Logging

/** LLD §8.1 -- monotonic watermarks, not absolute restoration.
  *
  * Adoption needs no exact-ID restoration, because no restored object carries an old identifier
  * into the regenerated plan. Exactly two counters want a watermark:
  *
  *   MapOutputTracker.epoch   executors compare received epochs against their highest-seen and
  *                            clear caches on increase (MapOutputTracker.scala:533-534,
  *                            1497-1505). A fresh driver starting at 0 while surviving
  *                            infrastructure holds a higher epoch means invalidations are ignored.
  *   SparkContext.nextShuffleId  only in the degraded configuration where Celeborn's mapping
  *                            layer is off, which precondition P4 forbids.
  *
  * nextRddId, nextJobId, nextStageId, AccumulatorContext.nextId, nextBroadcastId and
  * SQLExecution._nextExecutionId need NOTHING.
  *
  * ON MECHANISM. These are declared `private val ... = new AtomicInteger(0)`
  * (SparkContext.scala:2713, 2717), i.e. `private final` in bytecode. That does NOT block
  * restoration, for two independent reasons, both verified by execution on JDK 21.0.11:
  *   1. the field is a final REFERENCE to a MUTABLE object, so mutating it involves no
  *      final-field write at all;
  *   2. Field.set on a NON-STATIC final field of a non-hidden, non-record class is legal after
  *      setAccessible(true) on every JDK through 21. Only STATIC final is blocked.
  * A common false negative here comes from py4j, which unboxes AtomicInteger (it extends Number)
  * and re-boxes it as Integer, producing IllegalArgumentException ("... to java.lang.Integer") --
  * a VALUE TYPE mismatch, not an access denial. Note the exception class: it is never
  * IllegalAccessException.
  *
  * A watermark cannot move a live counter backwards, so a mis-sequenced restore can only waste
  * identifiers, never reissue one. That property is also what makes it defensible upstream: an
  * absolute setter on a live SparkContext is a footgun; a monotone advance is not. */
object IdWatermark extends Logging {

  def advanceAtLeast(ai: AtomicInteger, floor: Int): Int = {
    var cur = ai.get()
    while (cur < floor && !ai.compareAndSet(cur, floor)) cur = ai.get()
    ai.get()
  }

  def advanceAtLeast(al: AtomicLong, floor: Long): Long = {
    var cur = al.get()
    while (cur < floor && !al.compareAndSet(cur, floor)) cur = al.get()
    al.get()
  }

  /** Used when running against stock Spark. Prefer the private[spark] helpers on a patched build;
    * reflecting into internals is hygiene worth fixing, not a blocker. */
  def advanceReflectively(owner: AnyRef, fieldName: String, floor: Long): Long = {
    val f = owner.getClass.getDeclaredField(fieldName)
    f.setAccessible(true)
    f.get(owner) match {
      case ai: AtomicInteger => advanceAtLeast(ai, floor.toInt).toLong
      case al: AtomicLong    => advanceAtLeast(al, floor)
      case other => throw new IllegalStateException(
        s"$fieldName is ${if (other == null) "null" else other.getClass.getName}, expected an atomic counter")
    }
  }

  def applyRestoreWatermarks(
      sc: AnyRef, mapOutputTracker: AnyRef,
      capturedEpoch: Long, maxAdoptedShuffleId: Option[Int]): Unit = {
    advanceReflectively(mapOutputTracker, "epoch", capturedEpoch + 1)
    logInfo(s"resume: epoch watermarked to >= ${capturedEpoch + 1}")
    maxAdoptedShuffleId.foreach { m =>
      advanceReflectively(sc, "nextShuffleId", (m + 1).toLong)
      logInfo(s"resume: nextShuffleId watermarked to >= ${m + 1}")
    }
  }
}
