/* Establishes that Spark's ID counters are restorable on stock 3.5.8 (LLD section 8.1).
 *
 * The counters are declared `private val ... = new AtomicInteger(0)`
 * (SparkContext.scala:2713, 2717), i.e. `private final` in bytecode. A common but wrong
 * conclusion is that Java 17+ therefore blocks restoration. Two independent reasons it does not:
 *
 *   1. the field is a final REFERENCE to a MUTABLE object, so mutating it involves no
 *      final-field write at all;
 *   2. Field.set on a NON-STATIC final field of a non-hidden, non-record class is legal after
 *      setAccessible(true) on every JDK through 21. Only STATIC final is blocked.
 *
 * Case [D] reproduces the misleading failure: py4j unboxes AtomicInteger (it extends Number) and
 * re-boxes it as Integer, so the JVM rejects the write on VALUE TYPE. Note the exception class --
 * IllegalArgumentException, never IllegalAccessException. Read to the end of the message: the
 * "... to java.lang.Integer" suffix is the tell, and it is exactly what gets truncated in logs.
 *
 * Case [B] is the semantics this design actually uses: a monotonic CAS watermark, which cannot
 * move a live counter backwards, so a mis-sequenced restore can only waste IDs, never reissue one.
 *
 * Run:  javac IdCounterReflectionProof.java && java IdCounterReflectionProof
 * Expected on JDK 21:
 *   [A] PASS mutate-through-final-ref     [B] PASS CAS watermark
 *   [C] PASS replace-final-instance-field [D] reproduced IllegalArgumentException
 *   [E] PASS scala-object singleton       [F] static-final write blocked (IllegalAccessException)
 */
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

public class IdCounterReflectionProof {
  // exactly what Scala `private val nextShuffleId = new AtomicInteger(0)` emits
  static class SparkContextLike {
    private final AtomicInteger nextShuffleId = new AtomicInteger(0);
    private final AtomicInteger nextRddId = new AtomicInteger(0);
    int newShuffleId() { return nextShuffleId.getAndIncrement(); }
    int newRddId() { return nextRddId.getAndIncrement(); }
  }
  // Scala `object Foo { private[this] val nextId = new AtomicLong(0) }`
  static class AccumulatorContextLike {
    static final AccumulatorContextLike MODULE$ = new AccumulatorContextLike();
    private final java.util.concurrent.atomic.AtomicLong nextId =
        new java.util.concurrent.atomic.AtomicLong(0L);
    long newId() { return nextId.getAndIncrement(); }
  }

  public static void main(String[] a) throws Exception {
    System.out.println("java.version = " + System.getProperty("java.version"));
    SparkContextLike sc = new SparkContextLike();
    for (int i = 0; i < 7; i++) sc.newShuffleId();
    System.out.println("baseline newShuffleId() -> " + sc.newShuffleId());

    // ---- A: mutate the object the final field points at (the CORRECT approach)
    try {
      Field f = SparkContextLike.class.getDeclaredField("nextShuffleId");
      f.setAccessible(true);
      AtomicInteger ai = (AtomicInteger) f.get(sc);
      ai.set(4242);
      System.out.println("[A] PASS mutate-through-final-ref: newShuffleId() -> " + sc.newShuffleId());
    } catch (Throwable t) { System.out.println("[A] FAIL " + t); }

    // ---- B: monotonic watermark advance (what a restore actually needs)
    try {
      Field f = SparkContextLike.class.getDeclaredField("nextRddId");
      f.setAccessible(true);
      AtomicInteger ai = (AtomicInteger) f.get(sc);
      int floor = 9000;
      int cur;
      do { cur = ai.get(); if (cur >= floor) break; } while (!ai.compareAndSet(cur, floor));
      System.out.println("[B] PASS CAS watermark: newRddId() -> " + sc.newRddId());
    } catch (Throwable t) { System.out.println("[B] FAIL " + t); }

    // ---- C: REPLACE the final instance field object entirely
    try {
      Field f = SparkContextLike.class.getDeclaredField("nextShuffleId");
      f.setAccessible(true);
      f.set(sc, new AtomicInteger(777));
      System.out.println("[C] PASS replace-final-instance-field: newShuffleId() -> " + sc.newShuffleId());
    } catch (Throwable t) { System.out.println("[C] FAIL " + t.getClass().getSimpleName() + ": " + t.getMessage()); }

    // ---- D: reproduce the py4j failure -- wrong VALUE TYPE, not an access problem
    try {
      Field f = SparkContextLike.class.getDeclaredField("nextShuffleId");
      f.setAccessible(true);
      f.set(sc, Integer.valueOf(99));   // py4j unboxes AtomicInteger -> int -> Integer
      System.out.println("[D] unexpectedly PASSED");
    } catch (Throwable t) { System.out.println("[D] reproduced: " + t.getClass().getSimpleName() + ": " + t.getMessage()); }

    // ---- E: Scala `object` singleton (static MODULE$, instance field)
    try {
      Field f = AccumulatorContextLike.class.getDeclaredField("nextId");
      f.setAccessible(true);
      ((java.util.concurrent.atomic.AtomicLong) f.get(AccumulatorContextLike.MODULE$)).set(5150L);
      System.out.println("[E] PASS scala-object singleton: newId() -> " + AccumulatorContextLike.MODULE$.newId());
    } catch (Throwable t) { System.out.println("[E] FAIL " + t); }

    // ---- F: contrast -- STATIC final IS blocked
    try {
      Field f = AccumulatorContextLike.class.getDeclaredField("MODULE$");
      f.setAccessible(true);
      f.set(null, new AccumulatorContextLike());
      System.out.println("[F] static-final write PASSED (unexpected)");
    } catch (Throwable t) { System.out.println("[F] static-final write blocked as expected: " + t.getClass().getSimpleName()); }
  }
}
