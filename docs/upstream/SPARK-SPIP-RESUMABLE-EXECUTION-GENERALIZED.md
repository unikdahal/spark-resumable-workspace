# SPIP: Resumable Batch Execution — Adopting Completed Shuffle and Write State After a Driver Restart

**Status:** Design proposal, not yet submitted. This is a proposal for community discussion — no
implementation decision has been made pending its acceptance. It supersedes the narrower internal
draft `SPARK-SPIP-RESUMABLE-EXECUTION.md` in this directory, which described one specific
implementation path and is kept only as an earlier design note.

**Scope of this document:** a shuffle-stage recovery SPI and a batch write recovery contract. Row-level
and transactional write recovery is called out as a natural extension of the same contract but is
explicitly out of scope for this proposal — see Q2 and Appendix A.

---

## Q1. What are you trying to do?

Give Spark an **opt-in SPI** so that when a driver process is lost and a replacement driver starts
a logically-identical execution, it can *adopt* work the previous driver already finished — a
shuffle stage whose map output is still durably readable, or a data-writer commit that was already
recorded — instead of recomputing it from scratch.

The proposal is two independent extension points, both no-ops unless a provider is registered:

1. **Shuffle-stage recovery.** A new `ShuffleStageRecovery` SPI, installed through
   `SparkSessionExtensions`, consulted by `DAGScheduler` before it submits map tasks for a shuffle
   stage. If a registered provider reports that a stage's output already exists and is fetchable,
   the scheduler treats the stage as already computed rather than scheduling it.
2. **Batch write recovery.** A new capability interface on DSv2 `BatchWrite` that lets a connector
   record each successful data-writer commit, through a connector-supplied stable codec, into a
   Spark-owned versioned envelope held by a pluggable, fail-closed compare-and-set store. On
   restart, the connector is asked to recover its state before Spark creates or launches any data
   writer, so a task whose commit is already durable is never re-executed.

Neither extension point would name, assume, or depend on a specific shuffle service, table format,
or deployment substrate. The store is proposed purely as an interface, with the expectation that
different backends satisfy it in different deployments — a metadata service, a distributed KV
store, a table format's own commit log, or a purpose-built shuffle service. Nothing in the proposed
API surface would mention Kubernetes, or any particular shuffle service or table format, by name.

**How enablement reaches both sides.** A single caller-supplied recovery identity, carried on the
session that constructs the query, is the thread tying everything together: it is what the
replacement driver presents to a `ShuffleStageRecovery` provider when asking about a stage, and it
is what flows into `LogicalWriteInfo` and is surfaced through `isRecoveryEnabled()` so a connector
knows a given write is part of a recovery attempt rather than a fresh one. No identity present means
`isRecoveryEnabled()` is false and every execution path is exactly what it is today — nothing about
either SPI activates by default. Because the identity is supplied by the caller and not generated or
verified by Spark, its uniqueness per genuinely-new execution is a caller obligation, not a property
Spark can enforce (see Q6).

**What the write side is arbitrating.** Where the shuffle side answers "is this stage's output still
good," the write side answers a related but distinct question: "does everyone agree what the final
result of this write is." A **manifest**, in this design, is the immutable, agreed-upon description
of a write's outcome — which task attempt's commit is canonical for each output partition. Multiple
task attempts can each believe they succeeded (the original attempt before the driver died, and a
new attempt after recovery); the store's job is to pick exactly one canonical manifest per write and
make every subsequent reader see the same one, which is why every write to it is a compare-and-set
rather than an overwrite.

## Q2. What is this NOT designed to solve?

- **Not a general checkpointing or lineage-replacement mechanism.** RDD lineage and stage
  recomputation on task failure are unchanged. This SPI only lets a *new driver* skip work a *prior*
  driver already durably finished; it does not change single-driver fault tolerance at all.
- **Not automatic.** No behavior would change unless an application registers a shuffle-recovery
  provider and/or a connector implements the write-recovery capability and is handed a store
  instance. At most one shuffle-recovery provider per session.
- **Not a driver-restart orchestrator.** Detecting driver loss, starting a replacement process, and
  supplying it the same recovery identity is entirely the caller's/cluster manager's responsibility.
  Spark's contract would begin once the replacement driver constructs a session with the same
  recovery identity and a store instance backed by the same durable state.
- **Not a guarantee that any given store implementation is correct.** The interface would state a
  fail-closed contract (ambiguity, corruption, unavailability, or fencing must throw; an empty
  result is the only authoritative absence), but Spark cannot verify a third-party implementation
  actually provides linearizable CAS. A store that violates the contract could return wrong data.
  That is a real, accepted risk of the design — see Q6.
- **Not streaming.** Structured Streaming already has its own checkpoint/offset-log recovery model;
  this SPI targets batch execution only.
- **Row-level and transactional write recovery are out of scope for this proposal.** The same store
  and envelope contract naturally extends to row-level operations (MERGE/UPDATE/DELETE) and
  multi-statement transactions, but that extension is proposed as a separate, follow-up design once
  this base contract is accepted — bundling it here would widen scope for no benefit to either.

## Q3. How is it done today, and what are the limits of current practice?

Today, if the Spark driver process is lost — regardless of why (node loss, preemption, planned
maintenance, cluster-manager-initiated restart) — a replacement driver starts the application over
from the beginning, unconditionally. This SPI is only useful in deployments where shuffle output can
outlive the process that produced it — in stock Spark, executors are children of the driver's
application and normally die with it, so this precondition does not hold by default. It holds
wherever shuffle data is served by something with a lifetime independent of the executor process,
which Spark already has one in-tree mechanism for: the external shuffle service. This proposal does
not change or extend that mechanism; it only adds a scheduler-side hook so a *replacement driver* can
ask whatever is serving shuffle data "is this stage's output still good?" instead of unconditionally
recomputing it. The provider behind that hook, not Spark, is responsible for asserting the output is
still fetchable — Spark's role is to trust or reject the claim, not verify it independently.

The same "unconditional recompute regardless of whether the data survived" behavior is also true,
separately, on the write side: every V2 write restarts every data writer from empty on driver
replacement, including ones whose prior attempt already committed successfully at the sink.

**Prior art.** Spark has already recognized, piecemeal, that shuffle output surviving the process
that produced it is valuable: [SPARK-25299](https://issues.apache.org/jira/browse/SPARK-25299)
("Use remote storage for persisting shuffle data", open since 2018) is an umbrella issue for
decoupling shuffle storage from individual executors, and
[SPARK-54327](https://issues.apache.org/jira/browse/SPARK-54327) ("SPIP: Using Remote storage for
shuffle") is a newer, currently open SPIP pursuing the same goal for elasticity and cost.
[SPARK-35593](https://issues.apache.org/jira/browse/SPARK-35593) ("Support shuffle data recovery on
the reused PVCs", merged in 3.2.0) shipped a narrower, Kubernetes-PVC-specific version of "reuse
shuffle output instead of recomputing it" for the case where an executor restarts onto the same
volume. None of the three touch the driver side: they are about *executors* recovering or persisting
shuffle data, not about a *replacement driver process* querying and adopting already-completed work,
and none address write-commit recovery at all. This proposal is scoped to exactly that gap — the
driver-restart / scheduler-adoption problem — and is complementary to, not competing with, the
remote-shuffle-storage line of work: a remote-storage-backed shuffle provider is one natural
implementer of the `ShuffleStageRecovery` SPI proposed here.

Workarounds in practice today:
- Chopping a long job into many smaller jobs with an external orchestrator (Airflow, a custom
  script) that persists intermediate DataFrames to storage between stages, so a driver restart only
  loses the current segment. This trades an explicit, coarse-grained checkpoint for the cost of
  materializing and re-reading intermediate data that Spark would otherwise keep only as shuffle
  files, and it requires the application to be restructured around checkpoint boundaries chosen in
  advance rather than at whatever point a stage or a write actually finished.
- Accepting the recompute cost as-is, which for multi-hour batch jobs (large joins, wide
  aggregations, big writes) means a driver loss late in a job is as expensive as one at the start.

Neither workaround is a Spark-level capability; both push cost and complexity onto the application
or the platform team running Spark.

## Q4. What is new in your approach, and why do you think it will succeed?

**What's new:** two SPIs, at the two places where a driver replacement actually needs authoritative
state — "is this shuffle stage's output still good?" and "did this write partition already commit?"
— each backed by a fail-closed, versioned, CAS-arbitrated contract instead of an ad hoc flag.

**Why this shape should work as a Spark change specifically:**

1. **Zero behavior change when unused.** Both extension points would be pure SPI with no default
   provider shipped in Spark. The write-recovery contract can be satisfied by something as simple as
   a directory on a local filesystem using an atomic link/rename as the CAS primitive — no cluster,
   no external service, nothing beyond the local filesystem needed to exercise the contract. That
   simplicity is deliberate: the mechanism should be sound independent of any specific backing
   store, and testable without one.
2. **The fail-closed invariant would be structural, not conventional.** The store interface could
   not return "maybe" — every non-authoritative outcome (unavailable, fenced, ambiguous, corrupt,
   incompatible, resource-exhausted) would be a typed exception, not a nullable or boolean result a
   caller could accidentally treat as success.
3. **It composes with existing DSv2 extension points rather than inventing new scheduler concepts.**
   The write-recovery capability would be a `Supports*` interface on `BatchWrite`, the same pattern
   every other DSv2 capability follows. The one required addition to `LogicalWriteInfo` would be a
   `default` method, so it stays source-compatible with every existing implementor.
4. **The store contract is deliberately narrow** — four methods, no notion of partitioning scheme,
   file format, or transaction semantics baked in. A narrower interface is more likely to hold up
   unmodified as new write shapes (row-level, transactional) are proposed later, precisely because it
   was never widened to accommodate them in the first place; that is a design goal stated up front,
   not a result claimed here.

## Q5. Who cares, and what difference does it make?

**Audience:** teams running long batch Spark jobs — multi-hour joins, wide aggregations, large
table-format writes — on any platform where the driver can be lost independently of the executors
and their shuffle data. That includes, but is not limited to: cluster managers with driver-level
high availability that currently restart the whole application, preemptible or spot driver
placement, and planned maintenance that needs to relocate a running driver.

**The difference:** today, a driver loss at hour 3 of a 4-hour job costs the full 3 hours of
already-completed work. With this SPI wired to a durable store, only the work that had not reached
a durable checkpoint — an uncompleted shuffle stage, an uncommitted write partition — would need to
be recomputed. The size of the win is workload- and deployment-dependent (it scales with how much of
the job had already durably completed at the point of failure), so this document does not claim a
fixed wall-clock number. What would be measurable and testable is narrower and more honest: which
stages were adopted instead of recomputed, and which write partitions were adopted instead of
re-executed, in a given recovery.

## Q6. What are the risks?

- **The recovery identity would be a caller-supplied claim, not something Spark can verify.**
  Nothing in this protocol could distinguish "this is genuinely the same execution resuming" from
  "the caller reused a recovery ID across two independent runs by mistake." The latter would cause
  Spark to adopt state that does not belong to the current execution. This needs to be stated
  plainly rather than hidden: the recovery ID's uniqueness-per-genuinely-new-execution would be a
  caller obligation.
- **`OutputCommitCoordinator` would need to be disabled for recovery writes.** This looks like
  removing a safety mechanism and needs its own argument, not just an implementation: the
  coordinator's authorization state is driver-local and does not survive a driver restart, so after
  a restart it has no memory of what the previous driver authorized. Keeping it enabled would let it
  reject a retry of a task whose commit already succeeded and whose durable record the coordinator
  cannot see, wedging the write. The durable CAS store would be a strictly stronger arbiter for
  recovery writes — every commit decision it makes is itself durable and survives the driver that
  made it — so it would replace the coordinator for this path rather than leaving a gap.
- **Envelope compatibility across a rolling upgrade is a real open question this proposal does not
  claim to have settled.** Any versioned on-wire envelope needs a compatibility policy — what
  happens when a driver and its executors are on different Spark versions during a rolling upgrade.
  The likely first answer is a documented "do not roll a Spark upgrade across a live recovery
  window" constraint for the first version, but that should be settled as part of the design, not
  assumed.
- **New public surface.** The store interface would need a stability annotation
  (`@DeveloperApi`/`@Experimental` is the leading candidate given how much of its contract depends
  on ecosystem feedback); the connector-facing interfaces would be `@Evolving`. Binary-compatibility
  impact on the full surface needs its own pass before this is considered final.
- **A misbehaving store implementation could return wrong data.** As stated in Q2, Spark would
  enforce the fail-closed contract on its own code paths but cannot verify a third-party store
  actually provides the consistency it promises. Mitigated by documentation and by the interface's
  design (an authoritative-absence result is the *only* way to express "not found," so the easy
  implementation mistakes lean toward over-rejecting rather than over-accepting), not eliminated.

## Q7. How much surface does this actually touch?

The design separates into three pieces of increasing risk, each meaningful on its own:

- **The SPI and connector interfaces themselves** — no scheduler or execution changes. Dead code
  until a provider is registered, understandable purely on API shape.
- **Scheduler integration** for shuffle-stage recovery — depends on the SPI existing, but touches
  `DAGScheduler` and related classes.
- **Write-execution integration** — depends on the SPI existing, independent of the scheduler piece,
  and includes the `OutputCommitCoordinator` change from Q6, which is the part most likely to draw
  scrutiny and deserves its own detailed justification wherever this is discussed.

Ordering matters more than timing here: the SPI can be evaluated in isolation before either
integration is judged, and the two integrations do not depend on each other.

## Q8. What are the mid-term and final "exams" for success?

**Mid-term:** the SPI and connector interfaces exist with zero behavior change to any existing code
path — provable because no in-tree provider is registered by default. This is the cheapest,
lowest-risk checkpoint and should be judged purely on API shape and documentation quality.

**Final:** the following should be demonstrable end-to-end, not just at the unit level:

1. A first driver process runs a multi-stage job with recovery enabled, completes at least one
   shuffle stage and commits at least one write partition, and is then killed.
2. A second driver process, started fresh with the same recovery identity and the same durable
   store, adopts the already-completed shuffle stage (no map tasks re-submitted for it) and the
   already-committed write partition (no data writer re-launched for it), and the job completes with
   correct output.
3. At least one connector implementation of the write-recovery capability and one shuffle-service
   integration exist outside the Spark repository, demonstrating the SPI is usable by real third
   parties and not shaped around a single implementation in mind.

---

## Appendix A: Proposed API shape

The signatures below are illustrative of the proposed shape, not a final API — they are meant to
ground the discussion in something concrete, subject to change as the design is refined.

### A.1 Shuffle-stage recovery

```scala
// proposed: SparkSessionExtensions
def injectShuffleStageRecovery(builder: ShuffleStageRecoveryBuilder): Unit
// at most one registered builder per session
```

`ShuffleStageRecovery` would be consulted by `DAGScheduler` before it submits map tasks for a
shuffle stage; if it reports existing, fetchable output, the stage would be marked recovered instead
of scheduled. A fetch failure against a recovered stage's output should abort that stage rather than
transparently recompute it — a recovery claim, once made, should be trusted, not silently retried
into recompute.

### A.2 Batch write recovery — the store contract

```java
// proposed interface, package to be decided as the design is finalized
public interface RecoveryTaskCommitStore extends Serializable {

  interface Capabilities extends Serializable {
    int semanticsVersion();
    int maxLoadBatchSize();
    int maxManifestBytes();
    int maxTaskCommitBytes();
  }

  enum FailureReason {
    UNAVAILABLE, FENCED, AMBIGUOUS, CORRUPT, INCOMPATIBLE, RESOURCE_EXHAUSTED
  }

  class StoreException extends RuntimeException {
    public FailureReason reason();
  }

  Capabilities capabilities();
  byte[] resolveWriteManifest(String recoveryId, byte[] proposedValue);
  List<Optional<byte[]>> load(String recoveryId, int[] partitionIds);
  byte[] publish(String recoveryId, int partitionId, long taskAttemptId,
                 int attemptNumber, byte[] value);
}
```

Every method would be either an atomic compare-and-set that returns the canonical value (never a
diverged local one) or a batch load where an authoritative absence is the only meaning of an empty
result.

### A.3 Batch write recovery — the connector contract

```java
// proposed: a new capability on BatchWrite
public interface SupportsBatchWriteRecovery extends BatchWrite {
  RecoveryCommitMessageCodec commitMessageCodec();
  byte[] recoveryCompatibilityMetadata(PhysicalWriteInfo info);
  BatchWriteRecoveryState recover(PhysicalWriteInfo info);
  // onFailure(...) would preserve durable commits instead of calling abort() once task
  // execution has started, so a later driver can still resume them
}
```

```java
// proposed: LogicalWriteInfo
default boolean isRecoveryEnabled() { ... }  // source-compatible; existing implementors unaffected
```

### A.4 Design components and their risk

| Component | Contents | Risk profile |
|---|---|---|
| SPI + connector interfaces | No scheduler or execution changes | Dead code until a provider is registered — no existing path changes behavior |
| Scheduler integration | Shuffle-stage adoption in `DAGScheduler` | Scheduler-touching; inert with no provider registered |
| Write-execution integration | Including the `OutputCommitCoordinator` change from Q6 | The most likely to draw scrutiny; needs its own detailed justification |

### A.5 Natural extension, out of scope here

The same store and envelope-versioning approach is expected to extend to row-level and
transactional writes (MERGE/UPDATE/DELETE, multi-statement transactions) by adding connector-facing
interfaces on top, without changing the base store or envelope contract. Proposed as a follow-up
SPIP once this one is accepted, not bundled here.

## Appendix B: Alternatives considered and rejected

- **Application-level checkpointing of intermediate DataFrames.** Already available today without
  any Spark change (Q3). Rejected as *the* answer because it is coarse — an entire DataFrame must be
  materialized and re-read, not just the shuffle output or commit state Spark already produces as a
  byproduct of execution — and because it requires restructuring the application around
  checkpoint boundaries chosen in advance, rather than recovering at whatever granularity the job
  actually reached.
- **Extending `OutputCommitCoordinator` to be recovery-aware instead of adding a separate store.**
  Rejected because the coordinator's state is intentionally driver-local and ephemeral — that is
  what makes it cheap and simple for the non-recovery path — and making it durable across driver
  restarts would change its cost and failure profile for every job, not just ones that opt into
  recovery.
- **Baking a specific shuffle service or table format into the API.** Considered and explicitly
  rejected: the store and connector contracts should be interfaces with no default, production
  implementation shipped in Spark itself, specifically so that no particular downstream project is a
  prerequisite for adopting this SPI.
