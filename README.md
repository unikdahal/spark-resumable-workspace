# Resumable Spark execution

This repository is the control plane for a multi-generation laboratory, not a monorepo of Spark
source. The same problem was attacked three times. Only the third track is active.

| Generation | Purpose | Status |
|---|---|---|
| Original POCs | Proved individual assumptions | Historical. Live on GitHub; `workspace/repos.yaml` pins their commits; local checkouts removed 2026-08-25 |
| `spark-resume` | Clean standalone / pluggable library | Reference only. Pinned at `31663b1`; local checkout removed 2026-08-25 |
| Spark + Celeborn + Iceberg upstream | Native scheduler and connector contracts | **Canonical. This is the work to upstream** |

The two earlier tracks solve the same problem with different mechanisms. Do not read POC or
`spark-resume` docs as documentation for the upstream forks, or the reverse. To inspect either,
restore it from its pin (`workspace/bootstrap.sh` or a plain clone at the pinned commit).

**False rejection costs compute. False acceptance can return wrong data.** That invariant
survives every generation. Ambiguity fails closed.

## Where to start

1. [docs/architecture/OVERVIEW.md](docs/architecture/OVERVIEW.md) — how the project evolved and what is current
2. [docs/upstream/README.md](docs/upstream/README.md) — protocol, proposals, verification, PR split
3. [docs/architecture/READ-RECOVERY.md](docs/architecture/READ-RECOVERY.md) and [WRITE-RECOVERY.md](docs/architecture/WRITE-RECOVERY.md)
4. [workspace/repos.yaml](workspace/repos.yaml) — which Git repos exist, which commit, which role

Parallel work list for dispatching agents: [TODO.md](TODO.md).

Operational ledger (progress, ownership, estimates): [RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md](RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md).
Evidence of what has actually been run: [docs/upstream/VERIFICATION-STATUS.md](docs/upstream/VERIFICATION-STATUS.md).

## Active codebases

Exactly three forks carry the upstream design. Pins and clone URLs live in `workspace/repos.yaml`.
`workspace/bootstrap.sh` clones `role: active-upstream` only.

| Checkout (local path) | Role |
|---|---|
| `spark-resumable-upstream` | Spark recovery SPI, DAGScheduler adoption, V2 write recovery |
| `celeborn` | Application lease, committed shuffle catalog, recovery bindings |
| `oss-fixes/iceberg` | Snapshot idempotency ledger, Spark write-recovery pins |

`spark-resume` is a separate productization of the idea (plan-level `ExecutionSkipRule` /
`SkippedExchangeExec`). Treat it as a read-only reference, not a second architecture to maintain in
parallel; its checkout lives on GitHub at the `repos.yaml` pin.

Historical POCs (`resume-poc`, `resume-poc-digest`, `resume-poc-sql`, `resume-poc-e2e`) and the
archived research checkouts (`spark-3.5-adoption`, `oss-fixes/celeborn`,
`datafusion-comet-resumable-upstream`) live on GitHub as research evidence. Their local checkouts
were removed on 2026-08-25 to keep the working tree clean; every commit is pinned in
`workspace/repos.yaml` and restorable on demand.

## Layout of this repo

```
spark-resumable-workspace/
├── README.md                          this file
├── workspace/repos.yaml               pins; not git submodules
├── docs/architecture/                 current narrative
├── docs/upstream/                     source of truth for the upstream track
├── docs/history/                      original LLD, POC lessons, spark-resume design
├── verification/                      scripts; logs are generated, not committed
├── dev-harnesses/                     process-boundary tests (not merge material)
└── archive/worklogs/                  AI audit trails, not project status
```

This workspace used to pin every related checkout as a Git gitlink with no `.gitmodules`. That
made the tree look like one source repo and a snapshot of one machine. Independent clones plus
`repos.yaml` are the intended model.

## What is next (after this topology)

The remaining cleanup is in the forks, not here:

1. Split each giant WIP commit per [docs/upstream/PATCH-SPLIT-PLAN.md](docs/upstream/PATCH-SPLIT-PLAN.md)
2. Before Spark PR-1: provider naming (`ShuffleStageRecovery` vs an execution-recovery provider),
   drop Celeborn's Spark-API reflection once PR-1 exists, rewrite the executor-preflight
   non-local `return`
3. Freeze feature work until auth, envelope payload limits, lease retention, and Spark/Iceberg
   version targeting are decided
4. Then resume full-stack E2E

## Historical material

[docs/history/](docs/history/README.md) holds the original LLD, coverage write-up, and prototype
sketches. Those documents describe an earlier mechanism. Read them as how the design was learned,
not as how it ships.
