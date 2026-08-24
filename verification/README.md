# Verification

Scripts in this directory run focused Spark, Celeborn, and Iceberg suites against local
checkouts. They are not a substitute for the curated record in
[docs/upstream/VERIFICATION-STATUS.md](../docs/upstream/VERIFICATION-STATUS.md).

Canonical gates:

| Script | Runs |
|---|---|
| `run-celeborn-suites.sh` | the 15 Celeborn recovery suites (TODO group A1 gate) |
| `run-spark-recovery-suites.sh` | the 11 Spark recovery suites + scalastyle (TODO C1 gate) |
| `run-verification-queue.sh` | one serialized pass over every gate |
| `run-iceberg-b6.sh`, `run-iceberg-final.sh` | Iceberg core and v4.1 legs |
| `run-spark-followup.sh`, `run-b6-fix.sh`, `run-triage-and-b6.sh`, `run-celeborn-repair-fix.sh` | session follow-ups; delete once their outcomes are recorded in VERIFICATION-STATUS |

Expected checkout paths (see [workspace/repos.yaml](../workspace/repos.yaml)):

| Script variable | Local path |
|---|---|
| Spark | `spark-resumable-upstream` |
| Celeborn | `celeborn` |
| Iceberg | `oss-fixes/iceberg` |

`logs/` is generated output. Do not commit `*.log`, `results*.tsv`, or `harness-cp.txt`.
Those files are machine-local and go stale immediately. After a run, update
`VERIFICATION-STATUS.md` if a gate's outcome changed, then prune logs no doc cites.

Process-boundary tests that cannot live in a project test suite are under `dev-harnesses/`.
