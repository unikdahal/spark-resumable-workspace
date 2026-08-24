# Workspace pins

`repos.yaml` is the reproducible map of every related repository. This workspace is a
control plane: documentation, verification scripts, and development harnesses. The Spark,
Celeborn, Iceberg, and `spark-resume` source trees are independent Git repositories.

Do not reintroduce gitlinks or `.gitmodules`. Multiple checkouts of the same upstream,
historical POCs, and giant forks do not form one source tree.

```
./bootstrap.sh                 # clone role: active-upstream
./bootstrap.sh reference-implementation
./bootstrap.sh archived-research
```

Existing local checkouts are never overwritten. After clone, `git -C <local_path> status`
is the source of truth for uncommitted work; the commit hashes here are pins, not locks.
