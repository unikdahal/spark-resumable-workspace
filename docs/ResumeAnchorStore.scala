package org.apache.spark.resume

import java.io.Closeable

/** LLD §7. Anchors only. There is no cold path: the plan is regenerated rather than restored, so
  * there are no task-binary blobs to store. Object storage is therefore not in the design at all
  * -- one fewer single point of failure, one fewer credential surface, one fewer schema. */
case class Anchor(
    schemaVersion: String,
    queryId: String,
    generation: Long,
    kind: String,                        // SHUFFLE | BROADCAST | WRITE | RESULT
    planDigest: String,
    parentDigests: Seq[String],
    closureId: String,                   // indeterminacy closure membership (Rule D-2)
    determinism: String,
    sparkShuffleId: Int,
    celebornShuffleId: Int,
    numMaps: Int,
    numReduces: Int,
    mapStatuses: Array[Array[Byte]],
    mapOutputStatistics: Array[Byte],    // restored verbatim -- pins downstream AQE (LLD §2.3)
    partitionSpecs: Array[Byte],         // mandatory, never optional (LLD §10.C6)
    coalesceParams: Map[String, String],
    inputAnchors: Seq[InputAnchor],
    dppAnchors: Seq[DppAnchor],
    broadcastPayload: Option[BroadcastPayload],
    commitMessages: Map[Int, Array[Byte]],
    queryConstants: QueryConstants,
    epochAtCapture: Long,
    createdAtMs: Long)

/** Reject codes double as the metric dimension (LLD §15). Two histogram shapes matter and both
  * look like success until read carefully: DIGEST_MISS near 100% means the fingerprint is
  * unstable and the feature is a no-op; INPUT_DRIFT near 100% means the tables mutate faster
  * than restarts complete. */
case class RejectReason(code: String, detail: String = "")
object RejectReason {
  val SchemaMismatch   = RejectReason("SCHEMA_MISMATCH")
  val NotAdoptable     = RejectReason("INDETERMINATE")
  val AncestorRejected = RejectReason("ANCESTOR_REJECTED")
  val ClosureRejected  = RejectReason("CLOSURE_REJECTED")
  val DigestMiss       = RejectReason("DIGEST_MISS")
  val CelebornExpired  = RejectReason("CELEBORN_EXPIRED")
  val SkewShuffle      = RejectReason("SKEW_SHUFFLE")
  val PayloadTooLarge  = RejectReason("PAYLOAD_TOO_LARGE")
  def inputDrift(d: String) = RejectReason("INPUT_DRIFT", d)
}

trait ResumeAnchorStore extends Closeable {
  /** Monotonic fence. Every subsequent write is CAS-gated on this value (invariant F-1). */
  def acquireGeneration(queryId: String): Long

  /** Asynchronous by design: losing a record costs a recompute, inventing one costs a wrong
    * answer, so the writer may batch freely -- provided it never runs ahead of the Celeborn
    * commit (invariant W-1) and never blocks DAGScheduler's single eventThread
    * (EventLoop.scala:42). Overflow DROPS. It must never block. */
  def offerMapStatus(gen: Long, planDigest: String, mapIndex: Int, bytes: Array[Byte]): Unit
  def offerInvalidation(gen: Long, planDigest: String, mapIndex: Int): Unit
  def offerCommitMessage(gen: Long, planDigest: String, partitionIndex: Int, bytes: Array[Byte]): Unit
  def putAnchor(gen: Long, anchor: Anchor): Boolean
  def loadAnchors(queryId: String): Seq[Anchor]
  def dropAnchor(gen: Long, planDigest: String): Unit
  def flush(timeoutMs: Long): Boolean
}

/** Hash tags {qid} keep MULTI and Lua same-slot on Redis Cluster. maxmemory-policy must be
  * noeviction, checked at arm time via CONFIG GET.
  *
  * The :done bitmap is a HINT, never authority. Availability is derived from :ms alone at read
  * time. The reason is a specific eviction failure: a partial eviction that removed half the :ms
  * hash while :done survived intact would let a design that trusted :done report a stage complete
  * that is not. Deriving from :ms makes that a recompute instead of a wrong answer. */
object RedisKeys {
  def gen(q: String)                      = s"{$q}:gen"
  def anchor(q: String, d: String)        = s"{$q}:a:$d"
  def mapStatuses(q: String, d: String)   = s"{$q}:a:$d:ms"
  def done(q: String, d: String)          = s"{$q}:a:$d:done"
  def invalidations(q: String, d: String) = s"{$q}:a:$d:inval"
  def commitMessages(q: String, d: String)= s"{$q}:a:$d:cm"
  def index(q: String)                    = s"{$q}:index"

  /** A losing write returns 0 and the writer STOPS for the remainder of the query rather than
    * retrying. A zombie that keeps retrying is a zombie that eventually wins a race. */
  val FENCED_HSET_LUA =
    """
      |if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
      |redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
      |redis.call('EXPIRE', KEYS[2], ARGV[4])
      |return 1
      |""".stripMargin
}
