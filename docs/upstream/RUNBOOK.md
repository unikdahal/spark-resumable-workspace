# Operator runbook

Everything here is keyed to strings that exist in the code, so a grep of a driver log answers the
question. Written 2026-08-24.

## 1. Before enabling it anywhere

| Check | Why |
|---|---|
| `celeborn.auth.enabled=true` | without it, `checkAuth` is a no-op and an unauthenticated peer can win a task-commit CAS. See `THREAT-MODEL.md` §0 |
| All masters and workers upgraded first | an old master silently drops the five new snapshot fields when it re-serialises, destroying live recovery state |
| `spark.celeborn.driverRecovery.id` derived from an orchestrator run ID | reusing it across independent runs is indistinguishable from recovery |
| `spark.celeborn.driverRecovery.leaseDuration` > expected restart gap, and ≥ your restart backoff | too short and state is released before the replacement starts |
| `celeborn.master.applicationLease.maxDuration` ≥ that lease | the master caps the grant silently |
| Iceberg `commit.idempotency.retention-ms` ≥ the recovery window | otherwise commit proof can expire inside the window |
| Snapshot expiry disabled (or far longer than the window) for source tables | a pinned snapshot can be expired while the driver is down; recovery then fails closed |
| Partition count ≤ ~100K and commit messages ≤ 1 MiB with the inline store; ≤ 64 MiB per payload with `celeborn.master.recovery.blob.enabled=true` | sizing bounds differ by backend; see `RETENTION-AND-SIZING.md` §3 |

## 2. Is recovery actually on?

There is **no Spark configuration key** to check. Recovery is on for a session if and only if a
provider was injected. Confirm all three:

1. `spark.sql.extensions` contains `org.apache.spark.shuffle.celeborn.CelebornShuffleStageRecoveryExtension`
2. `spark.celeborn.driverRecovery.enabled=true` — otherwise the extension throws at session
   construction with `"CelebornShuffleStageRecoveryExtension is configured but recovery is disabled"`
3. The Spark build has the SPI — otherwise session construction fails with
   `"Celeborn driver recovery requires a Spark build with ShuffleStageRecovery support"`

A session that constructs successfully with all three has recovery armed.

## 3. Did the replacement driver actually recover anything?

Recovery is only observable through *work not done*. Compare against a control run:

- **Adopted shuffle stage**: the stage completes with **zero map tasks**. In the Spark UI, the stage
  shows 0/0 tasks; in a listener, `SparkListenerStageCompleted` with no `TaskEnd` events.
- **Recovered write partitions**: the driver logs
  `"Data source write support … is committing"` after running fewer writer tasks than the physical
  partition count.
- **Fully recovered write**: `"Data source write support … was already committed; skipping all
  writer tasks and the global commit."`
- **Executor adopted a canonical commit**: `"Using canonical recovery commit for partition N before
  creating a data writer."` — the preflight worked; the partition was neither re-read nor rewritten.

If wall time dropped but task counts did not, nothing was recovered.

## 4. Failure signatures and what each one means

| Log/exception text | Meaning | Action |
|---|---|---|
| `Shuffle recovery failed closed for shuffle N; refusing to recompute because an authoritative committed generation may exist` | the provider threw; Spark will not risk mixing generations | check Celeborn master reachability and the lease; retry the whole execution |
| `Failed to install recovered scheduler state for shuffle N; refusing to recompute` | adoption succeeded externally, Spark could not install it | driver-side bug or partition-count mismatch; capture the stage plan |
| `Recovered shuffle N became unreadable; refusing to recompute its map stage` | a fetch failure on an adopted shuffle | worker/storage loss; rerun with a fresh recovery ID |
| `Recovery write manifest differs from the durable manifest` | the replacement driver's write differs from the first driver's (partition count, schema, spec, mode, codec) | the two runs are not the same logical write — fix the job or use a new recovery ID |
| `Recovery is enabled but write table X does not implement …SupportsRecoveryWrite` | connector does not support recoverable writes | disable recovery for that job |
| `A recoverable batch write has no durable task commit store` | provider returned no `taskCommitStore` | Celeborn client misconfigured |
| `Connector and task store disagree for recovered partition N` | connector's own view contradicts the durable record | stop; do not retry blindly — this is a correctness signal |
| `Connector recovered partition N without an authoritative task store commit` | connector claimed a commit the store does not have | same as above |
| `Recovery task commit payload checksum mismatch` | corrupt durable record | inspect the master's state; the record is unusable |
| `Application X is still leased to <owner>` | the previous lease has not expired | wait out `leaseDuration`, or shorten it for future runs |
| `Stale application lease transition for X: expected epoch A, current epoch B` | two replacements raced | one wins; the loser must not retry with the same epoch |
| `Application lease renewal is fenced for X at epoch N` | this driver lost ownership | it cancels its own jobs; do not restart it in place |
| `Idempotency ledger hash collision for property P` | two different logical commits hashed to the same ledger key | stop; this is a data-integrity signal |

## 5. Diagnosing "it recomputed everything"

In order of likelihood:

1. **The lease expired** before the replacement driver started → all recovery state was released.
   Master log: `Application X timeout, trigger applicationLost event.` If you instead see
   `heartbeat timed out, retaining it until its recovery lease expires`, retention worked.
2. **A different `driverRecovery.id`** — the identity is the whole mechanism.
3. **A different Spark version.** The recovery key includes `session.version()`, so any Spark upgrade
   between the two drivers invalidates every shuffle key by design.
4. **The query changed.** The key includes the canonicalized query plan; a changed plan cannot adopt
   the older execution's stages.
5. **Recovery was never armed** — see §2.

## 6. What to collect before asking for help

- Both drivers' full logs, and their `driverRecovery.id` and `client.application.uniqueId`.
- The Celeborn leader master log for the window, plus which node was leader.
- `spark.sql.shuffle.partitions` and the physical partition count of the write.
- For a write: the sink's committed manifest and the count of durable task records.
- Whether any `expireSnapshots`/table maintenance ran between the two drivers.

## 7. Recovery blob backend operations

Active only when `celeborn.master.recovery.blob.enabled=true`. Large task-commit payloads then live
as content-addressed blobs on workers; Raft holds only the pointer. Design:
`CELEBORN-BLOB-BACKEND-DESIGN.md`; wire details: `PROTOCOL-SPEC.md` §5.

### Metrics

Master-side recovery counters exist (`master` source, label `outcome`):

| Metric | Outcomes | Meaning |
|---|---|---|
| `RecoveryTaskCommitPublishCount` | accepted / duplicate / fenced / rejected | every task-commit **and** blob-pointer CAS. `accepted` = this attempt became canonical; `duplicate` = an earlier value was returned unchanged; `fenced` = lease lost; `rejected` = validation or capacity |
| `RecoveryLookupCount` | hit / miss / corrupt / fenced / rejected | reads of durable records. A `miss` permits recomputation, a `corrupt` read does not — the two must never be summed |
| `RecoveryCatalogPublishCount`, `RecoveryAnchorResolveCount`, `ApplicationLeaseCount` | same publish outcomes | shuffle catalog, source/write anchors, lease transitions |
| Gauges: `RecoveryTaskCommitInlineBytes/Records`, `RecoveryCommittedCatalogCount`, `ApplicationLeaseActiveCount` | — | live-state size |

**Blob-specific metrics from the design (`CELEBORN-BLOB-BACKEND-DESIGN.md` §10) are not
implemented**: there is no upload, fetch, repair or GC counter, and no below-quorum gauge. Until
they exist, repair health is observable only through the leader-master logs below.

Knobs an operator will tune:

| Key | Default | Operational meaning |
|---|---|---|
| `celeborn.master.recovery.blob.replicationFactor` | `3` | replicas targeted per blob |
| `celeborn.master.recovery.blob.quorum` | `2` | durable acknowledgements required before the pointer exists — a publication below quorum never happened, as far as recovery is concerned |
| `celeborn.master.recovery.blob.inlineThreshold` | `4k` | payloads at or under this never become blobs; they ride inline and follow the §1 sizing limits instead |
| `celeborn.master.recovery.blob.orphanGrace` | `1h` | how long an unreferenced blob is kept before a worker may delete it; must exceed the worst upload→pointer-CAS gap or GC can eat a live payload's twin |
| `celeborn.master.recovery.blob.repairInterval` | `5m` | leader scan period for under-replicated pointers |

### Is repair keeping up?

Repair runs on the **leader** master only, every `repairInterval`, at most 64 copies per cycle
(hardcoded `Master.RecoveryBlobRepairTasksPerCycle`), most-degraded-first:

- `Repaired N under-replicated recovery blob(s)` — progress. A steady small `N` after worker loss is
  normal; `N` stuck at the same value across cycles means repair is not keeping up with losses.
- `Failed to repair the recovery blob for partition N` (warning) — that copy failed this cycle; the
  pointer is untouched and the next cycle retries. Recurring warnings for the same partition mean a
  source replica is dying or unreachable.
- **Silence is ambiguous**: no repair lines can mean "nothing degraded" or "the scanner is not
  running". Confirm liveness by stopping nothing and checking that the leader logs the line after a
  known worker loss, or by comparing two cycles' timestamps in the master log.

### When a pointer has no live replica

This is data loss for that partition, by design, and repair refuses to paper over it. The planner
logs once per scan:

> `Recovery blob for partition P of write W has no surviving replica among ...`

and deliberately does *not* plan a repair — there is nothing to copy from. What the operator does:

1. Treat the affected recovery as lost. A replacement driver reading it fails closed (hard failure,
   never recompute) — that is correct behaviour, do not try to route around it.
2. Rerun the job with a **fresh** `spark.celeborn.driverRecovery.id`.
3. If this was a total worker-set loss, restore capacity first; the remote-storage tier that would
   have saved this case is not implemented yet (`CELEBORN-BLOB-BACKEND-DESIGN.md` §8).

Related worker-side GC lines, for completeness: `Collected unreferenced recovery blob <sha256> for
<appId>` (normal garbage collection after the grace period) and `Could not determine whether
recovery blob <hex> is referenced` (the master was unreachable during GC; the blob is kept, which is
the safe direction).

## 8. Known gaps an operator will hit

- **Orphan output files.** A crashed driver leaves data files for partitions that never published.
  Nothing in the protocol cleans them up today; the global commit marker is the only authoritative
  record of what the write produced.
- **No recovery-state inspection tool.** There is no supported way to list what recovery state exists
  for an application; the RPCs exist (`GetCommittedShuffleCatalog`, `BatchGetRecoveryTaskCommits`) but
  no CLI wraps them.
- **No completed-execution cleanup.** State for a successful run is held until the lease lapses.
- **No per-application quota** on the global inline budget: one large write can starve others.
