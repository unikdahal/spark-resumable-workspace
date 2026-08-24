# Design: authentication as a precondition for recovery (Celeborn client)

Status: implemented in `celeborn` commit `b80ba5f72`; suite green on this machine. Written
2026-08-25. Companion documents: `THREAT-MODEL.md` §0 and T-1/T-15, `RUNBOOK.md` §1, CIP-5 in
`PATCH-SPLIT-PLAN.md`.

## 1. The requirement

Recovery turns Celeborn's optional authentication into a correctness dependency. With
`celeborn.auth.enabled` at its default of `false`, `RpcEndpoint.checkAuth` is a no-op:

```scala
if (client.getClientId != null && client.getClientId != appId) throw ...
```

An unauthenticated channel carries a null client id and passes. Every recovery RPC calls
`checkAuth` correctly — but the check only bites when auth is on. An attacker who knows (or
guesses) `appId + recoveryId + writeId + partitionId` can pre-publish a task-commit record; the
immutable CAS makes the attacker's record canonical; the real writer then discards its own correct
output per `SupportsRecoveryCommitDiscard`. For a connector like Iceberg that is arbitrary content
substitution without touching storage.

Requirement T-1 therefore states: recovery must refuse to enable when auth is off. This design
covers how the refusal is enforced, where it fires, and why the alternatives were rejected.

## 2. Mechanism

`CelebornShuffleStageRecoveryExtension.requireAuthenticatedClient(SparkConf)` runs inside the
recovery provider's constructor, immediately after the enable-flag check and before any identity
validation or RPC. It reads `celeborn.auth.enabled` through `SparkUtils.fromSparkConf(conf)
.authEnabledOnClient()` — **the same accessor the `LifecycleManager` uses** — so the precondition
and the binding can never disagree about whether the client authenticates.

On failure the provider throws at session construction with a message that:

- names both spellings of the setting (`celeborn.auth.enabled`, set as `spark.celeborn.auth.enabled`
  in Spark), because an operator greps for whichever they know;
- states the consequence it prevents (unauthenticated peers publishing task-commit records for this
  application's recovery identity), because "recovery failed" otherwise invites disabling auth.

## 3. Why here, and why these alternatives were rejected

| Alternative | Rejected because |
|---|---|
| Document auth as required | nothing enforced it; silent misconfiguration returns wrong data downstream |
| Flip the server default to auth-on | breaks every existing deployment on upgrade and is a services-side change outside the recovery patch set |
| Per-RPC capability token independent of SASL | invents a second credential system; SASL already exists and is what `checkAuth` gates |
| Enforce lazily at first RPC | leaves the session alive with half-installed state; construction is the earliest point where all configuration is available |
| Check only on the driver side of the blob upload path (T-15) | same root cause as T-1; one precondition covers both inline and blob publication |

Construction-time enforcement also satisfies fail-fast symmetry with the existing checks in the
same constructor (missing recovery ID, non-positive lease), so there is exactly one place to look
for enablement errors.

## 4. Behavioural contract

| Configuration | Outcome |
|---|---|
| recovery enabled, auth disabled | session construction fails; message names both keys |
| recovery enabled, auth enabled | provider installs; lease acquisition proceeds as before |
| recovery disabled | unchanged — extension throws its existing "configured but recovery is disabled" |

Test coverage drives the real constructor through a deep-stubbed `SparkSession`
(`CelebornShuffleStageRecoveryExtensionSuiteJ`): rejection asserts both keys appear in the message;
the authenticated case constructs successfully.

## 5. Residual risks (explicitly out of scope here)

1. **Server side is still the real guard.** The client precondition protects against accidental
   misconfiguration, not against a hostile client: with services' auth off, `checkAuth` remains a
   no-op for every RPC regardless of what the client checks. Deployments must still set
   `celeborn.auth.enabled=true` on masters and workers (`RUNBOOK.md` §1).
2. **Blob upload disk-filling (T-15)** by an unauthenticated peer is bounded only by orphan GC;
   the remote-storage tier and rate limiting remain open items.
3. A future "recovery requires auth level X" refinement (e.g., encrypted channels) should extend
   this single method rather than adding another call site.
