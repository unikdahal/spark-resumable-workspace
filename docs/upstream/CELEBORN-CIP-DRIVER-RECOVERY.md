# CIP: Driver ownership leases and a committed-work catalog

Draft, written against `celeborn` as of 2026-08-24.

## Motivation

Celeborn already keeps shuffle data alive independently of the Spark driver that produced it. What it
does not provide is (a) a way to know which driver *currently owns* an application, and (b) a durable,
Celeborn-authoritative record of which shuffles are fully committed and which task outputs have been
published. Without (a), a replacement driver cannot safely fence the process it replaces. Without
(b), the surviving data is unaddressable: the only index into it died with the driver.

This proposal adds both, plus the compare-and-set primitives a replacement driver needs to agree with
its predecessor on immutable identities.

## Scope

| In | Out |
|---|---|
| Application lease with monotonic epochs, renewal, takeover, worker-side enforcement | any change to shuffle data paths |
| Immutable committed-shuffle catalog, published only after `StageEnd` commits every worker file | shuffle read/write protocol changes |
| CAS for source anchors and connector write IDs | connector semantics |
| CAS for opaque task-commit envelopes, with hard identity/size bounds | payload interpretation — envelopes are opaque bytes |
| HA/Raft replication, snapshot save/restore, application cleanup | large-payload storage (follow-up, see Limitations) |

## Protocol summary

16 new message types (97–112) and five new replicated maps (`applicationLeases`,
`applicationWorkers`, `committedShuffleCatalogs`, `sourceRecoveryAnchors`, `recoveryTaskCommits`).
Every request carries `applicationLeaseEpoch` + `applicationLeaseOwnerId`, and every recovery RPC is
gated by the existing `checkAuth(appId)`. Full field-level detail is in `PROTOCOL-SPEC.md` §5.

Design points worth review attention:

**Lease epochs advance by exactly one.** `newEpoch == expectedEpoch + 1` is enforced in the state
machine, so a replacement driver must have observed the epoch it replaces. Renewal never shortens
expiry and never changes the epoch; exact replay is idempotent. Every transition is deterministic —
no wall-clock reads inside the state machine — so every replica applies the same result.

**Expiry is derived in the receiver's clock domain.** `leaseDurationMs` is preferred over
`expiresAtMs` on both the master and worker paths; absolute timestamps are accepted only for
compatibility. Cross-clock-domain expiry comparison is the classic way a fencing protocol quietly
stops fencing.

**Lookups distinguish absence from failure.** `PbGetCommittedShuffleCatalogResponse` carries both
`found` and `success`. "The catalog authoritatively does not exist" permits recomputation; "the
lookup failed" must not. Collapsing these into one boolean would be a correctness bug.

**Immutability everywhere.** Catalogs, anchors, write IDs and task commits are first-writer-wins. An
exact byte-for-byte replay is idempotent; a differing replacement is rejected. The publisher receives
the canonical value, so a losing attempt learns it lost rather than assuming it won.

**Keys are prefix-free.** Identities are length-delimited (`len:value` concatenation) and the source
and write namespaces carry disjoint `source:v1:` / `write:v1:` prefixes, so no combination of
identities can alias another. Identity strings are bounded at 1024 UTF-8 bytes and validated with
strict UTF-8 (`CodingErrorAction.REPORT`) at every ingress, including snapshot restore.

**Digests are verified six times.** Executor client, LifecycleManager, master ingress, Raft apply,
snapshot restore, and every read — all through one shared `RecoveryTaskCommitUtils.validatePayload`.

**Retention follows the lease.** `timeoutDeadApplications` normally declares an application lost after
`celeborn.master.heartbeat.application.timeout` (300s) and drops all its recovery state; the new guard
retains the application while its lease is unexpired. This is what makes a driver restart budget
configurable — see `RETENTION-AND-SIZING.md` §1.

## Configuration

Seven new master keys, all bounds rather than tuning knobs, listed with defaults in
`CONFIG-AND-ENABLEMENT.md` §2. No new worker or client keys: workers learn the lease through
`FenceApplication`.

## Limitations to state up front

1. **Task-commit payloads are stored inline in replicated state.** Bounded (1 MiB per record, 256 MiB
   / 200 000 records per recovery, 512 MiB / 1 000 000 cluster-wide, reserved with `Math.addExact` and
   released on failure), so memory is safe — but this is a ≤100 K-partition mechanism. The follow-up
   is worker-replicated content-addressed blobs with a durability quorum, keeping only digest, length,
   generation and locations in Raft.
2. **No per-application fairness** on the global budget: one large write can starve others.
3. **`applicationLeases` entries are not removed on application loss.** Retaining the epoch preserves
   monotonicity for a later incarnation, but the map grows without bound and rides in every snapshot.
   Needs either a documented bound or expiry-with-tombstone.
4. **Recovery makes authentication a correctness dependency.** `checkAuth` is a no-op when
   `celeborn.auth.enabled` is false (the default), and an unauthenticated publisher can win the CAS
   for a partition and make the legitimate writer discard its own output. Either recovery should
   refuse to enable without authentication, or that must be stated as a hard precondition. See
   `THREAT-MODEL.md` §0.

## Compatibility

Masters and workers must be upgraded **before** any job enables recovery: under proto3, a master
build that does not know the five new snapshot fields silently drops them when it re-serialises,
which destroys live recovery state without an error. Rolling back is safe only once no application
has recovery enabled. See `UPGRADE-AND-ROLLBACK.md` §2.

## Testing

48 recovery test cases exist across 8 suites (lease transitions, renewal fencing, snapshot survival,
task-commit integrity and first-writer-wins, identity ambiguity, aggregate bounds failing closed
without corrupting accounting, anchor immutability, catalog validation/replay, application-loss
cleanup, HA state-machine replay, worker lease store, client binding). Still missing: multi-master
failover during publication, lease-expiry chaos, network partition between old driver, replacement
driver, workers and leader, and rolling-version protocol compatibility.

## Completed-execution release (A5, designed 2026-08-25)

Today recovery state is reclaimed only when a dead application's heartbeat times out past its
lease - so a *successful* run holds catalogs, commits, pointers and budget for the full lease
(10m-24h), capping how many resumable jobs a cluster can absorb per hour.

Planned surface, one fenced idempotent RPC:

```
ReleaseRecoveryExecution { appId, recoveryId, leaseEpoch, leaseOwner }
-> releasedRecords, releasedBytes   (0/0 when nothing matched = harmless double-release)
```

Semantics:

1. Master validates `checkAuth` + live lease ownership exactly like pointer CAS; a fenced caller
   learns it is fenced rather than hearing silence.
2. Removal set: task-commit records and blob pointers whose key is `(appId, recoveryId)`, plus
   committed shuffle catalogs whose record names that pair (their semantic-key index entries go
   with them). Capacity accounting is released per removed entry through the same
   bytes/records-split path repairs use.
3. **Source anchors are deliberately retained**: their identity (`source:v1:<sourceId>`) is shared
   across executions of the same source, so releasing one execution's anchors could strand a
   concurrent sibling. Anchor expiry needs its own identity model first.
4. Client trigger: `LifecycleManager.releaseRecoveryExecution(recoveryId)` fired from the driver
   extension on normal application end when recovery was actually used.

Failure mode stays fail-closed: an unreachable master simply leaves state to age out under the
existing timeout, which remains the backstop.
