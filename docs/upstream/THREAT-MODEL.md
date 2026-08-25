# Threat model for the recovery protocol

Read out of the working tree on 2026-08-24. Scope: the new Celeborn recovery RPCs, the Spark
envelope, and the Iceberg ledger. Assumes the existing Celeborn threat model as a baseline and only
discusses what recovery *changes*.

## 0. The headline

**Recovery turns Celeborn's optional authentication into a correctness dependency.**

`celeborn.auth.enabled` defaults to `false`. With it off, `RpcEndpoint.checkAuth` is a no-op:

```scala
private def checkAuth(client: TransportClient, appId: String): Unit = {
  if (client.getClientId != null && client.getClientId != appId)
    throw new IllegalStateException(...)
}
```

`getClientId` is null on an unauthenticated channel, so the check passes. Every new recovery RPC does
call `checkAuth(context, appId)` — correctly, and consistently with `PbApplicationMetaRequest` — but
that check only bites when auth is enabled.

Before recovery, an unauthenticated peer could disrupt a Celeborn application. With recovery, an
unauthenticated peer who knows (or guesses) `appId` + `recoveryId` + `writeId` can **pre-publish a
task-commit record**. Because publication is immutable first-writer-wins, the attacker's record is
canonical: the real task computes its output, loses the CAS, and — per the protocol —
`SupportsRecoveryCommitDiscard.discardCommittedOutput` **deletes its own correct output** and adopts
the attacker's payload. For Iceberg that payload is a list of data files, so the attack is arbitrary
content substitution in a committed table, achieved without touching storage.

Every defence downstream holds: the digest matches (the attacker computed it), the identities match
(the attacker chose them), the codec version matches. None of them is an authentication mechanism.

**Requirement T-1:** recovery must refuse to enable when `celeborn.auth.enabled=false`, or the
documentation must state that auth is mandatory for it. Today neither is true. This is the single
highest-value security change available.

## 1. Assets

| Asset | Compromise means |
|---|---|
| Application lease (epoch/owner) | fence the legitimate driver, or write as a fenced one |
| Committed shuffle catalog | make a driver adopt shuffle bytes it did not produce |
| Source recovery anchor | pin a different input version than the one the query read |
| Task-commit record | substitute a partition's output (see §0) |
| Iceberg ledger entry | suppress a real commit, or forge proof one happened |

## 2. Trust boundaries

| Boundary | Guard |
|---|---|
| Executor → LifecycleManager | lease injected by the LifecycleManager; an executor **cannot** claim an arbitrary epoch |
| Driver → Master | `checkAuth(appId)` + lease epoch/owner on every request |
| Master → Worker | `FenceApplication`; workers persist and enforce the accepted lease |
| Client → Worker (blob upload) | **none beyond checkAuth.** The worker verifies digest and length of what it stores, but never who is storing it; lease fields on blob RPCs are claims until auth is on |
| Master ingress → Raft apply | identity validation + SHA-256 re-verification at both points |
| Snapshot restore → live state | validated into temporary state before installation |
| Spark → connector payload | opaque bytes; Spark never interprets, connector never sees the envelope |

The executor-cannot-forge-a-lease property is worth keeping explicit under review: it is what makes a
compromised executor a *bounded* problem — it can still publish for partitions of its own write,
because it must be able to, but it cannot fence a driver or act for another application.

## 3. Threats

| # | Threat | Status |
|---|---|---|
| T-1 | Unauthenticated task-commit poisoning (§0) | **Open.** Mitigated only by enabling `celeborn.auth.enabled` |
| T-2 | Lease theft while the real driver is alive | **Mitigated.** Non-renewal takeover is rejected while `nowMs < currentLease.expiresAtMs()` and the owner differs |
| T-3 | Epoch rollback / replay of an old lease | **Mitigated.** `newEpoch == expectedEpoch + 1` exactly; `renew` cannot shorten expiry; exact replay is idempotent |
| T-4 | Fenced old driver keeps writing | **Mitigated.** Workers enforce the persisted lease; client-side renewal failure cancels all jobs |
| T-5 | Corrupt record injected via a metadata snapshot | **Mitigated.** Digest verified on restore, into temporary state first |
| T-6 | Memory exhaustion of the master via large/many records | **Mitigated, bounded.** Seven inline limits, `Math.addExact` accounting, released on failure |
| T-7 | Batch-read amplification (one request → huge response) | **Mitigated.** `maxBatchResponseSize` caps the serialized response |
| T-8 | Key aliasing between namespaces or identities | **Mitigated.** Length-delimited prefix-free keys; `source:v1:` / `write:v1:` disjoint |
| T-9 | Malformed-Unicode identity smuggling | **Mitigated.** Strict UTF-8 with `CodingErrorAction.REPORT`, ≤1024 bytes |
| T-10 | Executable payload deserialization | **Mitigated by contract.** Java/Kryo forbidden; Iceberg uses JSON parsers |
| T-11 | Cross-application read of recovery state | **Mitigated only with auth on** — same weakness as T-1, read side |
| T-12 | Ledger-entry forgery in Iceberg | **Out of scope of this protocol** — requires catalog write access, which already implies table write access |
| T-13 | Recovery-ID collision between unrelated executions | **Not mitigated by design.** `driverRecovery.id` is a user-supplied claim of identity; reuse is indistinguishable from recovery |
| T-14 | Denial of service by exhausting the global inline budget | **Mitigated 2026-08-25.** Per-application shares (`maxInlineBytesPerApp` / `maxInlineRecordsPerApp`, default one full recovery each) bound what any single application can hold; rejections name the bound that fired. Validated by `RecoveryInlineCapacitySuite` |
| T-15 | Unauthenticated blob upload and pointer poisoning | **Open — same root cause as T-1, now on the blob path.** Content addressing authenticates *content*, never *authorship*: an attacker who computes `sha256` over their own bytes can `PushRecoveryBlob` it to workers (filling worker disks until orphan GC reaps it after `orphanGrace`) and then win the pointer CAS exactly as in §0, substituting arbitrary payload content for any partition of a known `(appId, recoveryId, writeId)`. Quorum-before-CAS bounds neither: the attacker controls enough workers' worth of uploads from one host. Pointer CAS is lease-fenced (`requireValidApplicationLease`), but with auth off the lease fields are unauthenticated claims |

## 4. Residual risks worth writing into the upstream proposal

1. **Auth dependency (T-1).** State it as a hard precondition, and preferably enforce it at session
   construction: refuse to install the recovery provider when the Celeborn client is not
   authenticated.
2. **Recovery-ID trust (T-13).** The protocol cannot distinguish "same logical execution" from
   "someone reused the ID". Document it as a caller obligation and recommend deriving the ID from an
   orchestrator run ID that is unique by construction.
3. **No per-app quota (T-14).** Consider a per-application share of the global inline budget rather
   than first-come-first-served.
4. **Audit trail.** Publications and takeovers are logged, but there is no structured audit event for
   "driver B fenced driver A and adopted N stages". For a feature whose failure mode is *silently
   using someone else's output*, an audit record is a reasonable review request.
5. **Blob backend inherits all of this (T-15, now implemented).** Payloads have moved to
   worker-replicated blobs: content-addressed storage authenticates *content*, never *authorship*,
   so T-1 extends to the blob upload path — including a disk-filling variant that did not exist
   inline, because worker storage is now writable by whoever can reach the blob RPC. The A3 fix
   (refusing to install the recovery provider without auth) closes the client side of this; the
   services' `checkAuth` remains the only server-side guard.

## 5. What a security review will ask that is not answered yet

- Which of these RPCs are reachable on the non-internal port, and is that intended?
- Is there a rate limit on `PublishRecoveryTaskCommit`? (No.)
- What happens to recovery state when an application's secret is rotated?
- Is `ownerId` (a random UUID) ever used as a security boundary, or only for liveness? (Only for
  liveness — worth stating, so nobody later treats it as a capability.)
