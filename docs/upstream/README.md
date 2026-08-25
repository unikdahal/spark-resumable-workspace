# Upstream documentation set

Written against the working tree on 2026-08-24. These documents describe the **upstream track**
(local checkouts `spark-resumable-upstream` + `celeborn` + `oss-fixes/iceberg`; pins in
`workspace/repos.yaml`), not the earlier POC track archived under `../history/`. The two tracks
solve the same problem with different mechanisms; do not read one as documentation for the other.

Ownership, per `RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md` §5: Celeborn and Iceberg material here is
maintained by Claude; the Spark-side proposal and the Spark half of the protocol spec describe code
Codex owns and should be treated as a reader's map, not as a specification Claude may change
unilaterally. Cross-repository contract changes go through the written proposal path in §5.

Ground rule for every file here: a statement is either traceable to source that was read, or to a
test run that happened, or it is marked as unverified. Nothing is restated from a status report.

| Document | Read it when |
|---|---|
| `PROTOCOL-SPEC.md` | you need the actual contract: envelope byte layout, CAS semantics, Celeborn wire messages, key namespacing, the eight invariants |
| `CONFIG-AND-ENABLEMENT.md` | you are turning the feature on, or asking "is this job resumable?" |
| `RETENTION-AND-SIZING.md` | you are choosing a lease duration, sizing the master, or wondering how long recovery state lives |
| `UPGRADE-AND-ROLLBACK.md` | you are rolling out or rolling back, or changing a format version |
| `THREAT-MODEL.md` | before enabling this anywhere with a network |
| `E2E-TWO-DRIVER-TEST-PLAN.md` | you are building the decisive full-stack proof |
| `TEST-STRATEGY.md` | you want to know where every test lives, how to run it, and what it does and does not model |
| `RUNBOOK.md` | you are operating a resumable job: preflight checks, how to tell whether anything was recovered, and what each failure string means |
| `VERIFICATION-STATUS.md` | you want to know what has actually been run, as opposed to written |
| `CELEBORN-BLOB-BACKEND-DESIGN.md` | you are implementing or reviewing the scalable replacement for inline task-payload storage |
| `ROW-LEVEL-RECOVERY-DESIGN.md` | you are touching recoverable DELETE/UPDATE/MERGE: the generation fence, manifest, and summary semantics |
| `AUTH-PRECONDITION-DESIGN.md` | you are changing how recovery enablement interacts with authentication (T-1) |
| `PATCH-SPLIT-PLAN.md` | you are preparing pull requests |
| `SPARK-SPIP-RESUMABLE-EXECUTION.md` | the original Spark-side proposal draft — internal audit note now, superseded for external use |
| `SPARK-SPIP-RESUMABLE-EXECUTION-GENERALIZED.md` | the proposal meant for dev@spark.apache.org: same mechanism, motivation and framing rewritten to name no specific shuffle service, table format, or deployment substrate |
| `CELEBORN-CIP-DRIVER-RECOVERY.md` | the Celeborn-side proposal |
| `ICEBERG-PROPOSAL.md` | the two Iceberg-side proposals |

## The five things a newcomer gets wrong

1. **There is no Spark configuration key.** Enablement is `SparkSessionExtensions.injectShuffleStageRecovery`
   plus per-connector opt-in interfaces. The operator-facing switches all live in Celeborn.
2. **The recovery window is the lease**, not a TTL — and takeover is *blocked* until the previous
   lease expires. `leaseDuration` is a two-sided knob.
3. **The durable store outranks the connector.** A connector-reported commit with no store record is
   an error.
4. **`None` means "authoritatively absent", never "unknown".** Every ambiguous lookup throws.
5. **The identity is a claim, not a fact.** Reusing `driverRecovery.id` across two independent runs is
   indistinguishable from recovery, and no mechanism here can catch it.
