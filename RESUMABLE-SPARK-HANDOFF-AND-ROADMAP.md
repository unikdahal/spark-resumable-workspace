# Resumable Spark: Implementation Handoff and Roadmap

Last updated: 2026-08-24 (Asia/Kolkata)

> This file is the durable record of the progress summary, granular backlog, estimates, evidence,
> constraints, and integration roadmap discussed with the user. Section 0 is the authoritative
> current snapshot. Later package estimates describe the broader program and must not override the
> remaining-time estimates in section 0.

## 0. Current execution ledger

This section is the concise source of truth for current progress. The detailed work packages below
remain the specification; a task is marked complete here only when its stated evidence exists.

| Area | State | Evidence or remaining gate | Conservative remaining effort |
|---|---|---|---:|
| Repository and ownership audit | Complete | Spark-only ownership, clean upstream baseline, dirty-file preservation, and CPU/build constraints verified | 0 |
| Initial Spark compile gate | Complete | Capped `catalyst/Test/compile` and `sql/Test/compile` completed successfully on 2026-08-24 | 0 |
| X1 API design audit | Complete | Dual task authority, store placement, typed absence/failure semantics, checksum coverage, and UTF-8 defects identified | 0 |
| X1 implementation/API freeze | Complete | Fresh capped compile and aggregate gates pass; concurrency/cancellation and Java API fixtures pass; the Java-17 artifact reproduced byte-for-byte with SHA-256 `2468aa94912d294081897228b46b05fd5ce1d2a289eeed8aff9bb095364e787e` | 0 |
| X2 row-level/transaction model | In progress | Transaction execution passes six crash-window cases. The v3 task envelope, authoritative nine-counter row summary, type-preserving delta APIs, driver-side exact-once metric restoration, and Spark-owned semantic control-event transport are implemented. Main sources compile; focused execution/conformance and manifest-binding gates remain | 4-7 weeks Spark implementation |
| X2 production proof | Blocked on shared integration after Spark framework | Needs connector transaction recovery/finalization plus Iceberg row-level implementation and crash tests | additional shared 3-6 weeks |
| X3 shuffle fail-closed boundaries | Implemented and aggregate verified | `DAGSchedulerSuite` passed 228/228 in the fresh aggregate gate, including recovery rejection cases | 0.5-1 day final evidence review |
| X3 AQE reuse/coalescing/skew coverage | Execution proof authored, verification pending | A non-empty AQE execution fixture now covers provider lookup, recovered-stage consumption, coalescing/skew-capable metadata, and zero writer construction. Production sources compile; the new test currently has a small Scala test-compilation defect | 1-3 days Spark-only execution proof; shared E2E later |
| X3 shuffle protocol versioning/fingerprints | Implemented and aggregate verified | Spark-owned protocol checks and full canonical-plan SHA-256 fingerprints replace truncated `treeString` identity; discrimination and extension suites pass | 0.5-1 day final evidence review |
| X3 broadcast/table-cache recovery | Architecture only | Requires a durable blob/rebinding protocol; current Spark work cannot truthfully claim completed-broadcast recovery | 3-6 weeks plus provider storage work |
| X3 executor-churn and scale evidence | In progress | Recreated stages retain recovered provenance and late fetch failure remains fail-closed (230/230 scheduler tests). Compact recovered metadata is now O(maps + reducers), with serialization-sharing tests and a 10K/100K/1M benchmark authored; aggregate and benchmark evidence remain | 3-7 days |
| X4 integrated crash/failure audit | Waiting for Claude-owned adapters | Requires Celeborn/Iceberg builds against frozen jar and external two-driver harness | 2-3 weeks after integration |
| X5 upstream decomposition | Pending | Design proposal, reviewable PR series, compatibility/migration notes, disclosure audit | 1-2 weeks |

### Independently executable Codex boundary

Codex can independently finish and validate the Spark API, Spark-owned task arbitration, the
connector-neutral transaction/row-level framework and conformance fixtures, shuffle safety and
version gates, and Spark-only benchmarks. It cannot honestly prove production row-level recovery,
durable broadcast recovery, or end-to-end zero-recomputation until the Claude-owned Celeborn and
Iceberg implementations consume the frozen contracts. Those integration-dependent claims remain
open even if every Spark-only test passes.

At nonstop agent throughput, the conservative critical-path estimate is 7-12 weeks for the
remaining Spark-independent implementation described above. The complete cross-repository project
is more conservatively 11-19 weeks because production connector adaptation, external-process crash
testing, failure injection, scale validation, and review hardening are not perfectly parallel. This
estimate is engineering elapsed time, not CPU time, and assumes defects found by the evidence gates
are fixed rather than waived.

### Status definitions

- **Complete:** implementation, required tests, compatibility/style gates, and recorded evidence exist.
- **Verified:** focused evidence passes, but an aggregate or integration gate remains.
- **In progress:** code or design exists but required evidence is missing.
- **Blocked:** completion requires a contract, implementation, or infrastructure outside Spark.
- **Pending:** implementation has not started. Implemented-but-unverified work is not complete.

### Granular Spark-only remaining-work ledger

These are remaining elapsed-time estimates as of 2026-08-24. They include review and reasonable
defect-fixing contingency and assume one continuous agent, one build at a time, and the mandatory
two-thread cap. Parallel rows overlap, so their durations must not be simply added.

| ID | Deliverable and done criterion | Current state / evidence | Dependency | Remaining | Confidence |
|---|---|---|---|---:|---|
| X1.1 | One public, authoritative task store; no redundant connector task arrays | Complete: store moved to `connector.recovery`, global state reduced, Spark owns the recovery ID | None | 0 | High |
| X1.2 | Envelope rejects header/payload corruption, invalid UTF-8, nulls, and oversize data | Complete: v1/v2 corruption, bounds, checksum, strict UTF-8, metric validation, and deterministic goldens pass | X1.1 | 0 | High |
| X1.3 | Retry/speculation/fencing covers lost replies, losers, canonical responses, and cleanup failure | Complete in connector-neutral execution tests, including deterministic same-partition CAS arbitration and lost-response retry | X1.1 | 0 | High |
| X1.4 | All focused Catalyst/SQL suites pass together under the resource cap | Complete: fresh aggregate passed scheduler 228/228 plus transaction 19/19, metric schema 8/8, task commit 20/20, compatibility 2/2, batch recovery 8/8, discrimination 3/3, AQE 2/2, and extensions 50/50 | X1.1-X1.3 | 0 | High |
| X1.5 | MiMa/style/diff checks plus deterministic API class list, signatures, jar, and SHA-256 are recorded | Complete: Catalyst/SQL MiMa and relevant Checkstyle/Scalastyle gates pass; 32-class Java-17 artifact reproduced twice. One unrelated upstream Catalyst test Checkstyle mismatch remains documented (`StUtilsSuite.java`) | X1.4 | 0 | High |
| X1.6 | Same-partition concurrent CAS-loser and cancellation-window behavior is deterministic | Complete: before-local-commit cancellation aborts; after-local-commit/before-publish does not abort; accepted/lost-response retry preflights without input | X1.1-X1.3 | 0 | High |
| X1.7 | A minimal Java connector/provider fixture compiles against the frozen public API | Complete: `JavaRecoveryApiConformance.java` compiles in the fresh capped SQL test compilation | X1.5 | 0 | High |
| X1.8 | Final X1 evidence index records fresh logs, counts, hashes, HEAD, and worktree provenance | Complete: HEAD `599c5e8cbae8f237a201a61a6d7aa2afe8573491`; jar/class-list/signature hashes recorded below | X1.4-X1.7 | 0 | High |
| X2.1 | Stable transaction ID and typed catalog recovery states are additive and fail closed | Production-wired: OPEN executes, COMMITTED short-circuits, ABORTED/UNKNOWN fail without mutation, and nested accepted/lost commit re-resolves without abort; focused suite 6/6 | X1 decisions | 0.5-1 day aggregate/style review | High |
| X2.2 | Durable task-result metadata v2 restores Spark commit metrics without rerunning writers | Complete: strict additive schema, v1/v2 goldens, full checksum, globally committed totals, exact-once listener restoration, and unavailable-store recovery pass | X1 freeze | 0 | High |
| X2.2a | Compile the v2 metric-envelope and execution changes | Complete in fresh capped Catalyst and SQL test compilation | X2.2 implementation | 0 | High |
| X2.2b | Prove v1 byte compatibility and add a deterministic v2 golden fixture | Complete: both fixtures decode/re-encode deterministically; v1 SHA-256 `46255d4065a825ba5e0684983275a1aa6318a386503f677044da3072b1207230`, v2 `c0ec87b66a2a0f5c2c6acab0857fc8c18e077517d49197290b10cf1a76f1a5ae` | X2.2a | 0 | High |
| X2.2c | Prove recovered custom metrics are applied exactly once and failed jobs apply none | Complete: partial replacement-driver and accepted-but-lost-response cases restore canonical totals exactly once; missing authoritative totals fail closed | X2.2a | 0 | High |
| X2.2d | Freeze the metric API with MiMa, style, signatures, docs, and a regenerated jar checksum | Complete: Java-17 JAR `2468aa94912d294081897228b46b05fd5ce1d2a289eeed8aff9bb095364e787e`; class list `3ce6faf609f1a88f0919b5c24b4c58408b7343d2d692c7ca2e69b79183f97e92`; signatures `a2cfc18310ad946f18f95abb50f0d8811efb8425504cd267ced264c79cfedbd3` | X2.2a-X2.2c | 0 | High |
| X2.3 | `ReplaceData` recovery has stable grouping, deterministic identity, and conformance tests | Execution foundation implemented: v3 summary envelope, action protocol, control-event transport, task aggregation, and exact metric restoration. The blanket row-level recovery guard remains until end-to-end Spark conformance passes | X2.1-X2.2 | 1-2 weeks | Medium |
| X2.3a | Define authoritative per-task row-level counters with nonnegative and overflow-safe aggregation | Implemented: fixed nine-counter schema, nonnegative validation, overflow-safe sum, and per-operation accumulator; focused rerun pending | X2.2 | 0.5-1 day | High |
| X2.3b | Add envelope/schema version carrying row-level summaries while preserving v1/v2 compatibility | Implemented: v3 carries optional schema plus fixed summary, full checksum, action-protocol version, and fail-closed presence rules; fresh tests/API artifact remain | X2.3a | 1-2 days | High |
| X2.3c | Generate summaries from the authoritative row-level writer/scan path, not transient driver metrics | Implemented: Spark-owned semantic control rows survive exchange/AQE, writers skip control-only rows, and the canonical task envelope carries exact counters. Physical writer-call tests remain | X2.3a-X2.3b | 2-4 days | Medium |
| X2.3d | Bind command, mode, input/output schemas, distribution/order, source anchors, transaction ID, and generation into the recovery manifest | Implemented but latest edits are unverified: a Spark-owned versioned/checksummed manifest now binds execution/write/generation/sink/catalog/table IDs, command/mode, canonical operation/condition/conflict-filter digests, explicit source and task schemas, structurally encoded distribution/order, subquery-aware source anchors, and connector bytes in a separate v4 outer field. Transaction remains intentionally absent while combined recovery is rejected | X2.1-X2.3c | 1-3 days verification and adversarial fixtures | Medium |
| X2.3e | Execute `ReplaceData` with committed-task adoption, missing-task execution, exact totals, retry/speculation, and fail-closed tests | Connector-neutral physical command fixtures now prove canonical task adoption with zero replacement writers, exact UPDATE totals, missing/wrong manifest rejection, and manifest drift rejection; newest fixtures await compilation/execution. Full DELETE/MERGE command-plan matrix remains | X2.3b-X2.3d | 3-6 days | Medium |
| X2.4 | `WriteDelta` safely recovers data/delete-file pairs, retry races, and cleanup | Type-preserving public API and wrapper forwarding implemented; production sources compile and focused contract suites pass. Actual paired-output recovery and row-level guard removal remain | X2.1-X2.3 | 1.5-2.5 weeks | Medium-Low |
| X2.4a | Preserve `DeltaWriter`/factory/batch-write type contracts while exposing recovery capabilities | Implemented and compiled; wrappers preserve `DeltaWrite`/`DeltaBatchWrite` and forward recovery ID, store, and metric schema | X2.2 | 0 | High |
| X2.4b | Add Java/Scala conformance fixtures and freeze additive API in allowlist/artifact | Java and Scala conformance fixtures pass; final MiMa/style/signature/class-list/JAR regeneration remains | X2.4a | 1-2 days | High |
| X2.4c | Persist and recover paired data/delete-file task outcomes atomically | Pending | X2.3 envelope/manifest | 4-7 days | Low |
| X2.4d | Prove partial recovery, retry races, cleanup, lost commit replies, and terminal failures | Pending | X2.4c | 4-6 days | Low |
| X2.5 | Transaction begin/recover/commit/abort state machine is wired into execution with crash-window tests | First production slice passes 6/6, including stable identity, all durable states, terminal short-circuit, and accepted/lost commit; fencing, durable abort reason, and batch-stage ordering remain | X2.1-X2.4 | 1-2 weeks | Medium |
| X2.6 | Connector-neutral conformance kit proves idempotence, fencing, corruption rejection, and unsupported paths | Pending | X2.2-X2.5 | 1-2 weeks, overlapping | Medium |
| X3.1 | Barrier, indeterminate, and unsupported push-merge recovery fail before adoption | Focused verified in the 228-test `DAGSchedulerSuite`; aggregate regression remains | None | 0.5-1 day | High |
| X3.2 | AQE recovery preserves map/runtime statistics and coalesced/skew reads | Real non-empty execution/provider/zero-writer fixture authored. Latest compile failed only in this test (`Array[LongType.type]` vs `Array[DataType]` plus unused import); correction and execution pending | X3.1 | 1-3 days | Medium-High |
| X3.3 | Protocol mismatch fails closed and fingerprints cannot collide through plan truncation | Implemented; final discrimination/extension suite rerun pending | X1 freeze | 1-3 days | Medium |
| X3.4 | Executor loss/churn has explicit safe-fallback tests | Implemented, verification pending: tracker-owned provenance survives stage recreation; executor-loss, metadata-fetch, and repeated/late fetch-failure tests assert that producers are not resubmitted. New cases await the capped aggregate run | X3.1-X3.3 | 1-3 days | Medium |
| X3.5 | Fingerprint/catalog benchmarks cover 10K-1M entries with recorded thresholds | Benchmark authored for 10K, 100K, and 1M square cases; execution, threshold selection, and recorded results pending | X3.3 | 1-3 days | Medium |
| X3.6 | Durable broadcast/table-cache recovery is implemented or explicitly excluded by an accepted proposal | Architecture documented; storage/rebinding needs an external provider contract | Provider work | 3-6 weeks plus provider work | Low |
| X5.1 | Spark proposal, compatibility notes, security audit, and reviewable PR decomposition exist | Pending | Stable X1-X3 | 1-2 weeks | Medium |

### Reconciled totals and critical path

| Scope | Included work | Conservative elapsed estimate |
|---|---|---:|
| Immediate API-freeze gate | X1 and v2 metric-envelope freeze are complete; only the newly added X2 delta/summary compile and X3 regression evidence remain in the current build queue | 1-3 days |
| Spark-independent completion boundary | X1, X2, X3 safety/versioning/churn/benchmarks, conformance and Spark docs; no production durable-broadcast claim | 7-12 weeks |
| Integration-dependent completion | Celeborn/Iceberg adaptation, durable storage, two-driver crash/HA/failure matrix | additional 4-8 weeks with overlap |
| Full production-candidate project | Spark-independent work plus all cross-repository proof and hardening | 11-19 weeks |

The Spark-only critical path is dominated by X2.3-X2.6 (4-7 weeks), followed by aggregate,
compatibility, documentation, and hardening gates. X1 is complete and X3
validation can overlap that work. New correctness defects, upstream API-review feedback, or including
durable broadcast recovery can extend the range.

### What is done, what is next, and what “complete” means

Completed and evidenced so far:

- a clean upstream Spark baseline is used; no POC repository is the implementation target;
- Spark owns durable task-result arbitration and fail-closed shuffle adoption;
- the v1/v2 task envelopes, exact-once metric restoration, transaction identity slice, AQE
  structural checks, scheduler rejection boundaries, and full-plan recovery fingerprints have
  focused passing evidence;
- the fresh aggregate gate passed scheduler 230/230 and all recorded transaction, envelope,
  recovery, discrimination, AQE, and extension suites;
- MiMa, relevant Checkstyle/Scalastyle, Java connector conformance, and deterministic Java-17
  API-artifact reproduction passed;
- design/API documents and a deterministic API artifact generator exist in the Spark tree.

The AQE execution test and the focused row-summary/control-event suites pass. The immediate gate is
compiling and running the newest Spark-owned manifest and physical command-adoption fixtures; the
execution service reported a temporary usage-limit rejection on the next build request, so these
latest edits are not yet verified. The dominant work after that is to finish the exhaustive
DELETE/UPDATE/MERGE rewrite matrix, prove physical writer action/control-row behavior,
implement connector-neutral `ReplaceData` and paired-output `WriteDelta` crash conformance, remove
the row-level recovery guard only for proven capabilities, finish transaction/batch ordering and
fencing, and add the complete conformance kit. Shuffle benchmark evidence and upstream
documentation can proceed in parallel, although all builds remain serialized.

For this document, **Spark-independent complete** means all Spark-owned APIs, execution behavior,
negative/fail-closed paths, conformance fixtures, benchmarks, compatibility checks, and docs pass
without editing Celeborn, Iceberg, Comet, or a POC repository. It does **not** mean production-wide
zero recomputation has been proven. That stronger claim requires compatible durable providers and
a real replacement-driver test across Spark, Celeborn, and Iceberg.

### Estimate assumptions and confidence

- Estimates are elapsed engineering time for a continuously running agent, not build CPU time.
- All builds use one core/two hardware threads and only one build runs at once; this makes broad
  Spark suites a real serialized cost even when source analysis is parallelized.
- Parallel agents shorten independent analysis, test authoring, documentation, and review, but do
  not safely collapse the transaction/row-level critical path or the single build queue.
- The 7-12 week Spark-only estimate includes defect fixing and review hardening. A narrow demo could
  appear sooner, but it would not satisfy the fail-closed and no-recomputation standard in this file.
- The 11-19 week production-candidate estimate assumes Celeborn and Iceberg work proceeds in
  parallel after the Spark contracts freeze. Apache review and merge time is separate and remains
  approximately 6-18+ months because it is controlled by upstream review, not nonstop agent time.

## 1. Objective

Build an upstream-quality Spark driver recovery system that resumes completed work after a driver
crash without recomputing successfully committed shuffle stages or output tasks. The overall
architecture covers Spark, Celeborn, and Iceberg, but the currently authorized implementation
scope is Spark-only. DataFusion Comet and edits to Celeborn, Iceberg, and all POC repositories are
explicitly deferred.

Completion requires a real two-driver crash test proving that completed non-empty shuffle stages
run zero replacement map tasks, committed Iceberg output partitions run zero replacement writer
tasks, missing work alone is executed, and exactly one global Iceberg commit is visible.

## 2. Mandatory constraints

- Do not modify any POC repository, including `resume-poc`, `resume-poc-e2e`, or `spark-resume`.
- Spark work is only in `spark-resumable-upstream`.
- Celeborn work is only in `celeborn`.
- Iceberg work is only in `oss-fixes/iceberg`.
- Do not work on Comet until the user re-enables it.
- Every compile or test command must be restricted to CPUs 0 and 1 with `taskset -c 0,1`.
- Maven must use `-T 1` and `MAVEN_OPTS='-XX:ActiveProcessorCount=2'`.
- Gradle must use `--max-workers=2` and `GRADLE_OPTS='-XX:ActiveProcessorCount=2'`.
- SBT must use `-Dsbt.task.cpus=1` and `-XX:ActiveProcessorCount=2`.
- Only one repository build/test process may run at a time.
- Use Java 17 from `/home/unik/.sdkman/candidates/java/17.0.11-tem`.
- Current authorization is Spark-only; Celeborn, Iceberg, Comet, and all POC repositories remain
  out of the active implementation scope unless the user explicitly changes it.

## 3. Authoritative repository state

| Project | Repository | Branch | Baseline commit | Tracking branch |
|---|---|---|---|---|
| Spark | `spark-resumable-upstream` | `resumable-driver-upstream` | `599c5e8cbae` | `myfork/resumable-driver-upstream` |
| Celeborn | `celeborn` | `resume-adoption-patch10` | `ab1e725ba1e` | `myfork/resume-adoption-patch10` |
| Iceberg | `oss-fixes/iceberg` | `supports-snapshot-id` | `acee8051776` | `fork/supports-snapshot-id` |
| POC (read-only) | `resume-poc` | `main` | unchanged | `origin/main` |

The three recovery branches contain WIP commits created at approximately 2026-08-24 02:18 IST.
The commit metadata uses the user's Git identity and does not prove which agent authored individual
lines. Do not rewrite or discard these commits.

### Current uncommitted work

Treat these files as existing work that must be preserved. They appear to be follow-up work made
after the WIP commits, but authorship cannot be established from Git metadata alone.

Spark:

- `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2Writes.scala`
- `sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/RecoveryKeyDiscriminationSuite.scala`
- `sql/core/src/test/resources/recovery/task-commit-envelope-v1.txt`

Celeborn:

- `tests/spark-it/src/test/scala/org/apache/celeborn/tests/spark/CelebornDriverRecoverySuite.scala`

Iceberg:

- `core/src/main/java/org/apache/iceberg/SnapshotProducer.java`
- `core/src/test/java/org/apache/iceberg/TestSnapshotIdempotency.java`

All three worktrees passed `git diff --check` at the handoff point. `resume-poc` was clean.

## 4. Current implementation summary

### Spark

Implemented in the WIP baseline:

- Provider-neutral completed-shuffle recovery SPI.
- DAGScheduler and MapOutputTracker installation of recovered shuffle state.
- AQE and non-AQE recovery paths.
- Fail-closed recovered shuffle fetch behavior.
- Stable source-anchor and sink-write identity resolution.
- Recoverable DataSource V2 write contracts.
- Versioned, checksummed task-commit envelopes.
- Immutable write-manifest CAS.
- Batched durable task lookup.
- Store-authoritative task recovery.
- Canonical first-writer task arbitration.
- Required cleanup of losing speculative output.
- Executor preflight lookup before writer construction or input consumption.
- Durable CAS replacing the driver-local OutputCommitCoordinator for recovery writes.
- Focused scheduler, SQL, compatibility, discrimination, and benchmark suites.

Verification known before handoff:

- Earlier focused scheduler and SQL extension tests passed.
- Catalyst test compilation passed after the new recovery APIs.
- SQL main compilation passed.
- A broad SQL test compile was still running/re-invalidated when work was stopped; do not claim it
  passed without rerunning it.

### Celeborn

Implemented in the WIP baseline:

- Stable application recovery identity and driver ownership leases.
- Lease takeover, renewal, expiry, and stale-driver fencing.
- Worker-side durable lease enforcement.
- HA/Raft replicated committed-shuffle catalog.
- Recovery intent, adoption, worker probing, and rollback.
- Dedicated task-commit CAS protocol with exact canonical winner.
- SHA-256 verification at client, LifecycleManager, master, apply, restore, and read boundaries.
- Ordered bounded batch lookup with authoritative misses.
- Strict UTF-8 identity bounds and length-delimited keys.
- Per-recovery and global byte/record capacity limits.
- Transactional snapshot restore validation and cleanup accounting.
- Spark reflection adapter for the Spark recovery SPI.
- Unit/HA/worker/protocol tests.

Known limitation:

- Task payloads are currently stored inline in bounded master/Raft state. This is safe from
  unbounded heap growth but is not the final scalable storage architecture.

### Iceberg

Implemented in the WIP baseline:

- Snapshot-ID exposure and immutable input anchoring.
- Stable recoverable sink identity.
- Non-executable, versioned task-commit codec using Iceberg content-file JSON.
- Spark recovery-writer factory and losing-file cleanup.
- Removal of object-store-unsafe per-task sidecar arbitration.
- Deterministic compatibility manifests for schema, spec, sort order, format, writer settings,
  branch/WAP state, operation type, base snapshot, and overwrite validation.
- Catalog-atomic snapshot commit idempotency.
- Bounded table-metadata idempotency ledger surviving snapshot expiration.
- Strict ledger corruption, collision, UTF-8, size, version, and duplicate validation.
- Core and Spark focused tests covering races, retention, corruption, and recovery.

Known limitations:

- Source snapshots are selected but not yet protected by a recovery-lifetime Iceberg reference.
- Copy-on-write and position-delta row-level recovery remain fail-closed/unsupported.
- The new Spark API integration and new ledger tests still need a clean capped Gradle run.

## 5. Ownership split

The split is intentionally repository-based to prevent both agents editing the same files.

| Owner | Scope | Approximate remaining effort share |
|---|---|---:|
| Claude | All remaining Celeborn implementation and tests | 35% |
| Claude | All remaining Iceberg implementation and tests | 25% |
| Claude | Operational docs for Celeborn/Iceberg and their E2E harness | 5% |
| Codex | All remaining Spark implementation and tests | 22% |
| Codex | Cross-repository contract review and acceptance audit | 8% |
| Codex | Final failure-matrix orchestration and upstream decomposition | 5% |
| **Total Claude** | | **65%** |
| **Total Codex** | | **35%** |

### Exclusive file ownership rule

- Claude may edit only `celeborn` and `oss-fixes/iceberg`.
- Codex may edit only `spark-resumable-upstream` and this handoff document.
- Claude owns the Celeborn-hosted Spark integration test suite because it resides in `celeborn`.
- Codex may review Celeborn/Iceberg diffs read-only and report contract violations, but must not fix
  them directly.
- Claude may review Spark APIs read-only and request changes, but must not edit Spark.
- Cross-repository API changes are proposed in writing first. Codex changes Spark, freezes the
  contract, and produces a new API jar; Claude then adapts Celeborn/Iceberg.

## 6. Claude work packages (65%)

### C1. Validate and finish the existing Celeborn inline task store

Estimate: 3-6 days

Tasks:

1. Compile all affected Celeborn modules under the required resource cap.
2. Fix protobuf, Java, Scala, and generated-code compatibility issues.
3. Run focused protocol, metadata, HA, worker lease, master, client, and Spark integration tests.
4. Verify first-writer-wins behavior across an actual HA leader change.
5. Verify stale executor publication is rejected after driver takeover.
6. Verify batch lookup ordering, authoritative misses, response limits, and corruption rejection.
7. Verify snapshot restore reconstructs exact counters and leaves live state untouched on failure.
8. Add metrics for records, bytes, rejections, fencing, CAS winners/losers, and corrupt reads.

Acceptance:

- A capped compile succeeds for every affected module.
- All focused tests pass.
- A real HA test proves one canonical commit through leader failover.
- `git diff --check` and relevant format/license checks pass.

### C2. Build the scalable Celeborn recovery-blob backend

Estimate: 2-4 weeks

Tasks:

1. Upload opaque envelopes under a content-addressed SHA-256 key.
2. Select at least two eligible workers or a configured replication factor.
3. Workers write to a temporary file, fsync, verify length/digest, and atomically rename.
4. Require a configured durability quorum before publishing a Raft pointer.
5. Store only digest, length, generation, format version, and replica locations in Raft.
6. CAS the immutable pointer using the current application lease epoch.
7. Return the exact canonical pointer/payload when another attempt wins.
8. Read replicas with length/digest verification and fail over on loss/corruption.
9. Repair missing/corrupt replicas asynchronously.
10. Add remote-storage fallback for complete worker-set loss where configured.
11. Tombstone pointers before physical blob deletion.
12. Garbage-collect unreferenced speculative uploads after a safe grace period.
13. Preserve in-flight uploads across leader changes without accepting unfenced publication.
14. Add quota, backpressure, observability, and recovery-time metrics.

Acceptance:

- Large commit messages do not enter Raft logs or master heap as inline payloads.
- A worker loss leaves the canonical payload readable.
- A corrupt replica is detected, bypassed, and repaired.
- Leader failover between blob quorum and pointer CAS is safe and idempotent.
- Orphan GC never deletes a live canonical payload.

### C3. Finish the Celeborn two-driver integration suite

Estimate: 1-2 weeks

Start from the existing untracked
`CelebornDriverRecoverySuite.scala`, but do not accept a same-process two-session test as the final
proof.

Tasks:

1. Fork driver A as an external process.
2. Complete a non-empty shuffle stage and persist a machine-readable crash checkpoint.
3. Kill driver A with `SIGKILL`, not graceful `SparkSession.stop()`.
4. Start driver B with the same stable recovery/application identity.
5. Count actual submitted map/writer tasks, not elapsed time.
6. Prove completed stages execute zero map tasks.
7. Prove a different recovery identity adopts nothing.
8. Cover AQE enabled/disabled, stage reuse, coalescing, skew, and worker loss.
9. Cover old executor publication during and after lease takeover.
10. Cover Celeborn leader replacement during adoption and task publication.

Acceptance:

- The test uses two OS driver processes and an abrupt first-driver death.
- A completed non-empty stage executes exactly zero replacement map tasks.
- Output equals an uninterrupted control execution.

### C4. Validate and finish Iceberg batch recovery

Estimate: 1-2 weeks

Tasks:

1. Build against the Codex-frozen Spark recovery API jar.
2. Run core idempotency-ledger tests.
3. Run Spark 4.1 codec, SparkTable, and SparkWrite recovery tests.
4. Verify append, dynamic overwrite, and overwrite-by-filter.
5. Verify schema, spec, sort order, branch, WAP, format, property, and base-state drift fail closed.
6. Verify canonical loser files are removed and canonical winner files remain.
7. Verify snapshot expiration does not remove the global completion proof.
8. Verify unknown commit state and catalog-response loss converge on one global result.
9. Test Hadoop/local plus at least one object-store-semantic FileIO.

Acceptance:

- No task recovery sidecar is used for arbitration.
- Concurrent same-write global commits produce exactly one logical commit.
- Expiring the committed snapshot does not permit a duplicate retry.
- Corrupt ledger/codec/manifest data fails closed before mutation.

### C5. Add Iceberg source snapshot pins

Estimate: 1-2 weeks

Tasks:

1. Consume the stable recovery execution/source identity supplied by Spark.
2. Create a deterministic Iceberg tag/ref before the source anchor is accepted.
3. Point it to the exact snapshot and bind the schema identity.
4. Set maximum reference age longer than the Celeborn recovery-store TTL plus safety margin.
5. Validate that the tag still points at the selected snapshot during replacement analysis.
6. Never fall forward to the current snapshot.
7. Remove a losing CAS attempt's tag best-effort.
8. Release successful/abandoned pins or allow bounded TTL cleanup.
9. Test snapshot expiration racing with pin creation and driver death.

Acceptance:

- Normal snapshot expiration cannot remove a live recovery input.
- A missing/moved/corrupt recovery tag fails closed.
- Pins have bounded lifecycle and do not leak permanently.

### C6. Implement Iceberg row-level recovery

Estimate: 2-4 weeks

Copy-on-write manifest must bind:

- scan snapshot ID;
- canonical conflict filter;
- isolation level and validation snapshot;
- sorted overwritten data-file identities;
- sorted dangling delete-vector identities.

Position-delta work must add:

- stable data/delete-file commit codec;
- referenced-data-file semantics;
- durable row-level summaries and metrics;
- idempotent MERGE, UPDATE, and DELETE global commits;
- transaction/catalog atomicity tests;
- crash points before and after data/delete file publication.

Acceptance:

- MERGE, UPDATE, and DELETE rerun no durably committed task partition.
- Exactly one row-level global state transition is visible.
- Unsupported row-level modes remain fail-closed until their full contract is implemented.

### C7. Celeborn/Iceberg documentation

Estimate: 4-7 days, overlapping C1-C6

Deliver:

- protocol and state-machine diagrams;
- lease/fencing invariants;
- blob durability and GC model;
- Iceberg idempotency and source-pin lifecycle;
- sizing/retention configuration;
- security and failure semantics;
- operator recovery runbook;
- upgrade/downgrade compatibility matrix.

## 7. Codex work packages (35%)

### X1. Freeze and validate the Spark recovery API

Original estimate: 4-8 days. Current remaining estimate: 4-8 days, most likely 5-6, because the
new v2 metric envelope and concurrency/cancellation gates were added before final API freeze.

Tasks:

1. Preserve and review the current uncommitted Spark fixes and compatibility fixture.
2. Complete Catalyst and SQL test compilation under the resource cap.
3. Run `RecoveryTaskCommitSuite`, `RecoveryTaskCommitCompatibilitySuite`,
   `BatchWriteRecoverySuite`, `RecoveryKeyDiscriminationSuite`, scheduler suites, and
   `SparkSessionExtensionSuite`.
4. Verify executor preflight returns a late canonical commit without creating a writer or consuming
   upstream input.
5. Verify durable CAS replaces OutputCommitCoordinator for recovery tasks.
6. Test accepted-but-response-lost publication, task retry, speculation, cancellation, and fencing.
7. Review public API size, annotations, binary compatibility, naming, and serialization.
8. Freeze envelope/manifest version 1 and regenerate `/tmp/spark-recovery-api.jar`.
9. Publish the exact API checksum and class list to Claude.

Acceptance:

- All focused Spark tests pass.
- The compatibility fixture decodes and re-encodes deterministically.
- No recovery-enabled path can start a non-recoverable writer.
- No successful durable task is recomputed on same-driver retry or replacement-driver recovery.

### X2. Spark transaction and row-level execution framework

Original estimate: 2-4 weeks. Revised remaining estimate: 6-10 weeks. The original estimate did
not adequately include type-preserving `WriteDelta`, exact-once metric restoration, transaction
crash windows, connector conformance fixtures, and review-hardening contingency.

Tasks:

1. Define recovery behavior inside catalog transactions.
2. Extend the task-store framework to delta and row-level writers without weakening fail-closed
   behavior.
3. Preserve durable custom metrics and row-level summaries.
4. Cover MERGE/UPDATE/DELETE physical execution and abort semantics.
5. Keep streaming/micro-batch recovery explicitly separate unless a complete epoch protocol is
   designed.
6. Add public API and connector conformance tests usable by Iceberg.

Acceptance:

- Transaction and row-level support has an explicit atomicity model.
- Spark never combines messages from incompatible write generations.
- Aborting a resumed execution preserves authoritative committed tasks.

### X3. Remaining Spark intermediate-result recovery

Original estimate: 1.5-3 weeks. Current Spark-only safety, churn, and scale remainder: 1-2 weeks.
Durable broadcast/table-cache recovery is a separate 3-6 week architecture/provider package and
cannot be claimed complete by Spark alone.

Tasks:

1. Complete AQE reuse/coalescing/skew recovery coverage.
2. Test barrier and indeterminate shuffle behavior.
3. Define and implement completed broadcast recovery if “all completed work” includes broadcasts.
4. Test executor churn and recovered shuffle fetch failures.
5. Add version/upgrade checks for recovered shuffle catalogs.
6. Benchmark plan fingerprinting and large stage catalogs.

Acceptance:

- Every supported completed intermediate type either recovers with zero producer tasks or is
  explicitly rejected before recomputation can mix generations.

### X4. Cross-repository acceptance and completion audit

Estimate: 2-3 weeks, after Claude packages are integrated

Codex performs this work read-only in Claude-owned source except for review feedback.

Tasks:

1. Verify Spark API jar/checksum matches Celeborn and Iceberg builds.
2. Review every cross-repository identity, version, checksum, lease, and TTL invariant.
3. Run the serialized build/test matrix.
4. Audit the real two-driver crash evidence.
5. Run a failure matrix covering driver, executor, master, worker, network, catalog, and storage
   failures.
6. Run scale tests at 10K, 100K, and 1M partitions or document verified safe limits.
7. Confirm POC repositories remain untouched.
8. Produce a requirement-by-requirement completion ledger with authoritative evidence.

Acceptance:

- A real two-driver test proves zero map and writer recomputation for completed work.
- All supported workload classes have positive and negative/fail-closed evidence.
- No requirement is marked complete using only narrow unit tests or source inspection.

### X5. Upstream decomposition

Estimate: 1-2 weeks

Deliver:

- Spark design proposal and reviewable PR series;
- Celeborn protocol/lease, task metadata, blob backend, and integration PR series;
- Iceberg source pin, codec, idempotency, and row-level PR series;
- compatibility and migration notes;
- AI-generation disclosures required by project policy;
- no push/PR/external posting without explicit user approval.

## 8. Dependency and execution order

1. **Codex X1 first:** stabilize Spark interfaces, pass tests, produce API jar/checksum.
2. **Claude C1 and C4:** compile current Celeborn and Iceberg source against the frozen contract.
3. **Claude C2 and C5 in parallel:** blob backend and Iceberg source pins have minimal file overlap.
4. **Codex X2 and X3 in parallel:** Spark row-level/transaction and remaining intermediate recovery.
5. **Claude C3 and C6:** external-process crash harness and Iceberg row-level integration.
6. **Codex X4:** final integrated failure, scale, and completion audit.
7. **Both prepare docs; Codex X5 assembles upstream decomposition.**

Builds remain serialized even when source work is parallel.

## 9. Integration contract that neither owner may change unilaterally

- Recovery execution ID is stable across driver incarnations and identifies one immutable logical
  execution.
- Source ID and sink/write ID use stable semantic identities, never driver-local stage IDs.
- The first accepted source anchor, write manifest, task commit, or global commit is immutable.
- Every mutation is fenced by the current Celeborn application lease epoch/owner.
- Store absence is authoritative only when the provider is healthy; ambiguity is an error.
- Spark owns the task envelope and manifest format.
- Iceberg owns deterministic encoding/decoding of its `WriterCommitMessage` payload.
- Celeborn treats Spark envelopes as opaque bytes and preserves them exactly.
- The task store, not connector object-store sidecars, is the task-commit authority.
- Connector global commit must be intrinsically idempotent; a post-commit marker cannot close the
  crash window.
- A recovery task checks the durable store before creating a writer or consuming input.
- A losing speculative commit must clean its local output without deleting the canonical output.
- Fencing, corruption, incompatible versions, retention mismatch, and unavailable state fail
  closed.
- TTL ordering must be: Iceberg source pin and global idempotency proof live longer than Celeborn
  recoverable task state, with a documented safety margin.

## 10. Required build and test commands

Use the Java environment:

```bash
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH=/home/unik/.sdkman/candidates/java/17.0.11-tem/bin:/usr/local/bin:/usr/bin:/bin
```

Spark example:

```bash
taskset -c 0,1 env JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2' \
  ./build/sbt -Dsbt.task.cpus=1 'catalyst/Test/compile' 'sql/Test/compile'
```

Celeborn example:

```bash
taskset -c 0,1 env MAVEN_OPTS='-XX:ActiveProcessorCount=2' \
  ./build/mvn -T 1 -pl <affected-modules> -am test
```

Iceberg example:

```bash
taskset -c 0,1 env GRADLE_OPTS='-XX:ActiveProcessorCount=2' \
  ./gradlew --max-workers=2 <focused-tasks>
```

Do not run these three builds concurrently.

## 11. Cross-repository time estimate

Assumptions: agents work continuously, source work can be parallelized, builds remain serialized on
two CPUs, required infrastructure is available, and estimates exclude Apache maintainer wait time.

| Milestone | Remaining wall time |
|---|---:|
| Freeze Spark API and pass current focused gates | 3-7 days |
| Validate current Celeborn and Iceberg integrations | 1-2 weeks |
| Production worker-backed task payload storage | 2-4 weeks |
| Iceberg source-pin lifecycle | 1-2 weeks |
| Spark framework plus Iceberg row-level and transaction recovery | 7-12 weeks, partly overlapping |
| Real two-driver crash and HA suite | 2-3 weeks |
| Scale, chaos, retention, compatibility, and security | 3-5 weeks |
| Documentation and upstream decomposition | 2-3 weeks |
| **Production candidate with overlapping work** | **12-20 weeks** |
| **Broad supported-workload hardening** | **16-24 weeks** |
| Apache upstream review and merge | **6-18+ months** |

The granular ledger and Spark-only 8-13 week estimate in section 0 are authoritative for current
Codex work; this section estimates the broader multi-repository program. The first useful production
candidate for Iceberg batch append/dynamic overwrite/filter overwrite,
Spark AQE/non-AQE shuffle recovery, fenced Celeborn task commits, and real two-driver validation is
estimated at 6-9 weeks. It must not be labeled generally production-ready before scalable blob
storage, retention coupling, row-level semantics, and chaos testing are complete.

## 12. Copy-paste assignment for Claude

```text
Work only in /home/unik/Coding/spark/celeborn and
/home/unik/Coding/spark/oss-fixes/iceberg. Do not edit spark-resumable-upstream or any POC repo.
Preserve all existing committed and uncommitted work. Read all applicable AGENTS.md/CLAUDE.md files.

You own work packages C1-C7 in
/home/unik/Coding/spark/RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md. Start by inspecting the exact current
state and reporting discrepancies. Do not change the cross-repository contract in section 9. If a
Spark API change is required, describe the exact signature/invariant to Codex and wait for a frozen
Spark API jar.

Priority order: C1 compile/test current Celeborn CAS, C4 compile/test current Iceberg integration,
C2 worker-replicated recovery blob backend, C5 Iceberg source pins, C3 external two-driver crash
suite, C6 row-level recovery, C7 docs.

Every compile/test must use taskset -c 0,1. Maven: -T 1 and
MAVEN_OPTS=-XX:ActiveProcessorCount=2. Gradle: --max-workers=2 and
GRADLE_OPTS=-XX:ActiveProcessorCount=2. Never overlap a build with Codex; coordinate the build slot.
Do not push, force-push, create PRs, or post externally without explicit user approval.
```

## 13. Copy-paste assignment for Codex

```text
Work only in /home/unik/Coding/spark/spark-resumable-upstream and the shared handoff document. Do not
edit Celeborn, Iceberg, Comet, or any POC repo. Preserve existing committed and uncommitted work.

You own work packages X1-X5 in
/home/unik/Coding/spark/RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md. Freeze and validate the Spark API
first, publish the local API jar path/checksum/class list to Claude, then implement Spark
transaction/row-level and remaining intermediate-result recovery. Review Claude-owned diffs
read-only and perform the final cross-repository acceptance audit.

Every compile/test must use taskset -c 0,1, ActiveProcessorCount=2, and sbt.task.cpus=1. Coordinate
the single shared build slot. Do not push, force-push, create PRs, or post externally without
explicit user approval.
```

## 14. Current execution state

Spark packages X1-X5 are active under the user's authorization. Work is restricted to
`spark-resumable-upstream` and this document. C1-C7 are retained as integration planning context
but are not authorized for Codex implementation. Celeborn, Iceberg, Comet, and POC repositories
must not be edited in the current execution phase.
