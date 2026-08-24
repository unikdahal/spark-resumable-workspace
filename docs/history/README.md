# Historical design material

These documents and sketches belong to earlier generations of the project. They are kept
because they record how the architecture was learned. They are not a description of the
current Spark / Celeborn / Iceberg upstream track.

Current design: [../architecture/OVERVIEW.md](../architecture/OVERVIEW.md) and
[../upstream/README.md](../upstream/README.md).

| Directory | Contents |
|---|---|
| [original-lld/](original-lld/) | Spark 3.5 + Celeborn LLD, coverage matrix, AQE/corruption notes |
| [old-project-design/](old-project-design/) | The `spark-resume` library design and patch inventory |
| [poc-lessons/](poc-lessons/) | Prototype Scala/Java/Python sitting beside the LLD |

The POC repositories themselves (`resume-poc`, `resume-poc-digest`, `resume-poc-sql`,
`resume-poc-e2e`) remain independent GitHub repos. Pins are in [../../workspace/repos.yaml](../../workspace/repos.yaml).
Do not treat those trees as maintained product code.
