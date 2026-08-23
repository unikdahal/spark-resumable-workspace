# SPIP: Resumable execution after driver loss

Draft. Written against the implementation in `spark-resumable-upstream` as of 2026-08-24; every API
signature quoted here was read out of that tree.

## Q1. What are you trying to do?

When a Spark driver dies mid-query and a replacement driver runs the same logical query, Spark
recomputes everything — including shuffle stages whose output still exists intact on a disaggregated
shuffle service, and including write partitions the connector has already durably committed. The work
survived; only the driver-local metadata naming it did not.

This SPIP proposes two narrow, opt-in extension points that let an external provider prove that
surviving work is safe to adopt, so a replacement driver runs **only the work that is actually
missing**:

1. a **shuffle-stage recovery SPI**, consulted before map tasks are submitted, and
2. a **recoverable batch write contract**, in which each task's commit is published to a durable,
   fenced, immutable store before the driver learns about it.

## Q2. What problem is this proposal NOT designed to solve?

- Not a general driver-HA framework. Spark's task and stage retry are untouched and remain the first
  line of defence.
- Not streaming. Micro-batch recovery is a different protocol with an adequate existing mechanism.
- Not in-flight task recovery. A task running at the moment of driver death is lost work; this bounds
  the loss, it does not eliminate it.
- Not a shuffle service. It consumes a shuffle service's durability; it never reimplements it. A
  deployment whose shuffle cannot survive its driver cannot use the shuffle half at all, and says so
  loudly rather than degrading silently.
- Not row-level operations (MERGE/UPDATE/DELETE) in the first version — they fail closed.

## Q3. How is it done today, and what are the limits?

Today the only options are (a) recompute the query, or (b) decompose the job into smaller
applications and make the orchestrator idempotent, materialising intermediates at every boundary.
(b) works but costs a rewrite, forces storage round-trips Spark's planner would otherwise avoid, and
still recomputes anything inside a boundary. There is no mechanism for a second driver to adopt a
first driver's committed output, because nothing in Spark's model gives that output an identity that
outlives the driver process.

## Q4. What is new in your approach and why do you think it will be successful?

Three properties, in order of importance:

**Identity is durable, not driver-local.** Recovery keys are derived from canonicalized plans and a
caller-supplied execution identity. Shuffle IDs, stage IDs, RDD IDs and task attempt IDs are
explicitly excluded, because a replacement driver allocates all of them again.

**Every path fails closed.** `None`/`null` means an *authoritative* absence and nothing else. Any
unavailable, ambiguous, indeterminate or corrupt lookup throws, and Spark fails the query attempt
rather than mixing a committed generation with recomputed output. A fetch failure on an adopted
shuffle aborts the stage instead of recomputing it, precisely because recomputation there would mix
generations.

**Spark owns arbitration; the connector owns semantics; the store owns durability.** Spark defines a
versioned, checksummed envelope and never interprets the connector payload inside it. The connector
defines a stable non-executable codec and compatibility metadata. The store provides an immutable
first-writer-wins CAS. The failure mode of the whole design is *wasted work*, never corrupted output.

## Q5. Design sketch

### Shuffle stages

```scala
trait ShuffleStageRecovery extends RecoveryAnchorResolver {
  def tryRecover(info: ShuffleStageRecoveryInfo): Option[RecoveredShuffleStage]
  def resolveSourceAnchor(info: SourceRecoveryInfo): String
  def abortRecovery(info: ShuffleStageRecoveryInfo, cause: Throwable): Unit = {}
  def onStageCompleted(info: ShuffleStageRecoveryInfo, result: RecoveredShuffleStage): Unit = {}
}
```

Installed with `SparkSessionExtensions.injectShuffleStageRecovery`; at most one per session.
`DAGScheduler` consults it while creating a shuffle map stage, before any task is submitted. A
returned recovery must already be readable through the active shuffle manager; Spark then registers
synthetic map statuses and skips every map task. The provider must record its *intent* durably before
returning `None`, so that a driver that dies immediately after external shuffle commit still leaves a
discoverable record — a post-completion callback alone leaves an unsafe crash window.

### Batch writes

```java
interface SupportsBatchWriteRecovery extends BatchWrite {
  String recoveryId();
  RecoveryCommitMessageCodec commitMessageCodec();
  byte[] recoveryCompatibilityMetadata(PhysicalWriteInfo info);
  BatchWriteRecoveryState recover(PhysicalWriteInfo info);
  void abortAfterRecovery(WriterCommitMessage[] messages);
}

interface RecoveryTaskCommitStore extends Serializable {
  byte[] resolveWriteManifest(String recoveryId, byte[] proposedValue);
  byte[][] load(String recoveryId, int[] partitionIds);
  byte[] publish(String recoveryId, int partitionId, long taskAttemptId, int attemptNumber, byte[] value);
}
```

Execution order: build a manifest binding recovery ID, partition count, codec identity and connector
compatibility metadata → CAS it → **require the winner to equal the proposal byte-for-byte** →
`recover()` → batch-`load` the durable store → run only partitions with no durable commit → skip the
global commit entirely if the write already committed.

On the executor: `load` **before** creating a writer (so a retry after a lost publish reply neither
re-reads its input nor writes again), `publish` after `DataWriter.commit()`, and if this attempt lost
the CAS, `discardCommittedOutput` deletes its own output and the canonical winner is returned.
`DataWriter.abort()` is never called after a successful commit.

Two consequences a reviewer should see stated plainly:

- The driver-local `OutputCommitCoordinator` is **disabled** for recovery writes. It cannot arbitrate
  here: if a writer commits and the durable-store reply is lost, a coordinator denial on the retry
  would wedge the write until another driver restart. The durable CAS is the coordinator.
- The durable store is authoritative over the connector. A connector-reported commit with no store
  record is an error, not a shortcut.

## Q6. Impact on existing behaviour

None when no provider is injected: every new path is behind `Option`/interface checks, and the new
public surface is additive. There is **no new SQL configuration key** — enablement is by injection
plus per-connector opt-in interfaces. `LogicalWriteInfo` gains `isRecoveryEnabled()`, which is a
source-compatible addition for implementors outside Spark; a MiMa review is required before merge.

## Q7. Risks

| Risk | Mitigation |
|---|---|
| A provider's recovery key collides across different queries | key material includes the canonicalized *query* plan, not only the stage; the doc mandates it. A provider that derives keys from truncated plan strings is unsafe — see the discrimination test in `RecoveryKeyDiscriminationSuite` |
| Recovery ID reused across independent runs | unfixable by the protocol: the ID *is* the claim of identity. Documented as a caller obligation |
| Envelope format cannot roll | `formatVersion` is exact-match today; decide on a readable-version set before the first format change |
| Adopted shuffle becomes unreadable mid-query | fetch failure aborts the query rather than recomputing; loss is bounded to that attempt |
| Provider misbehaviour corrupts results | every provider contract is fail-closed; Spark validates partition counts, sizes, row counts, identity and digests before installing anything |

## Q8. How will this be tested?

Component tests exist in-tree: 5 envelope tests, 19 recovery tests in `SparkSessionExtensionSuite`,
scheduler and map-output tests. The decisive proof is a **two-driver test with a real SIGKILL**;
`E2E-TWO-DRIVER-TEST-PLAN.md` specifies 17 scenarios and 8 assertions for the full stack, and
`WRITE-RECOVERY-HARNESS.md` describes a store-only harness that proves the write half without a
shuffle service. Task counts, not wall time, are the measurement.
