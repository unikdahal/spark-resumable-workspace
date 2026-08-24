"""
Digest stability and drift detection — the experiment that decides whether this design can work
on a given stack (LLD section 13.1).

WHY THIS ONE FIRST
------------------
Everything in the design hangs on one property: the same query, planned in two different JVMs,
must produce the same digest. That is easy to demonstrate on `spark.range(...)`, whose leaves are
pure case classes with no external references. It says nothing about a real scan.

    BatchScanExec.scala:57
        override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)
        @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

The fingerprint delegates to a connector-supplied object. If that object inherits identity
hashing, the key is JVM-instance-dependent, every anchor misses on every restart, and the feature
is a safe no-op. That is a one-day question and it should precede everything else.

WHAT THIS ASSERTS
-----------------
  1. hashCode-based fingerprint stability, per scan shape       (the naive method, as a control)
  2. structural digest stability, per scan shape                (the method the design uses)
  3. DPP pruning-key digest stability                           (InSubqueryExec.values())
  4. drift DETECTION: mutate the dimension table -> digests must differ   (ladder rung 6)

Run:
    python digest_stability.py --phase A
    python digest_stability.py --phase B
    python digest_stability.py --phase B --drift        # must FAIL to match

INTERPRETATION
--------------
  hashCode stable, structural unstable  -> bug in structural_string, not in the design
  structural stable, hashCode unstable  -> exactly the case the design anticipates; proceed
  both unstable on the scan case only   -> connector object identity is leaking into the key
  drift run reports "match"             -> SEV-1. Stop. The correctness boundary is wrong.
"""

import argparse
import hashlib
import json
import os
import shutil

from pyspark.sql import SparkSession

STATE = "/tmp/resume_digest_state"
WAREHOUSE = "/tmp/resume_digest_warehouse"


def sha(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


def structural_string(plan, path=""):
    """Mirror of PlanDigest.structuralString.

    Deliberately never calls toString on connector Scan/Batch/Table objects, Broadcast, RDD, or
    Future. Only nodeName, simple class name, and canonicalized expressions.
    """
    lines = []
    try:
        it = plan.expressions().iterator()
        parts = []
        while it.hasNext():
            parts.append(it.next().canonicalized().toString())
        args = ",".join(parts)
    except Exception as e:
        args = "<expr-err:%s>" % type(e).__name__
    lines.append("%s %s %s %s" % (path, plan.nodeName(), plan.getClass().getSimpleName(), args))
    try:
        ch = plan.children()
        for i in range(ch.size()):
            lines.extend(structural_string(ch.apply(i), "%s/%d" % (path, i)))
    except Exception:
        pass
    return lines


def dpp_pruning_digest(plan):
    """Approximates InputAnchor.pruningDigest by reading the DPP expressions off the plan.

    A full implementation reads InSubqueryExec.values() reflectively -- `result` is a
    @transient private VAR, so no final-field write is involved anywhere.
    """
    found = []

    def visit(p):
        try:
            it = p.expressions().iterator()
            while it.hasNext():
                s = it.next().toString()
                if "dynamicpruning" in s or "InSubquery" in s:
                    found.append(s)
        except Exception:
            pass
        try:
            ch = p.children()
            for i in range(ch.size()):
                visit(ch.apply(i))
        except Exception:
            pass

    visit(plan)
    return sha("\u0002".join(sorted(found))) if found else None


def hashcode_fingerprint(plan):
    try:
        return format(plan.canonicalized().hashCode() & 0xFFFFFFFF, "08x")
    except Exception as e:
        return "ERR:%s" % type(e).__name__


CONF_EXACT = [
    "spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.files.maxPartitionBytes", "spark.sql.files.openCostInBytes",
    "spark.sql.join.preferSortMergeJoin", "spark.sql.codegen.wholeStage",
    "spark.sql.session.timeZone", "spark.shuffle.minNumPartitionsToHighlyCompress",
]
CONF_PREFIX = ["spark.sql.adaptive.", "spark.sql.optimizer.dynamicPartitionPruning."]


def config_digest(spark, effective_default_parallelism):
    conf = spark.sparkContext.getConf().getAll()
    keep = sorted("%s=%s" % (k, v) for k, v in conf
                  if k in CONF_EXACT or any(k.startswith(p) for p in CONF_PREFIX))
    # The EFFECTIVE value, not the configured one: CoalesceShufflePartitions.scala:57-61 falls
    # back to sparkContext.defaultParallelism, which tracks currently registered executors.
    keep.append("__effectiveDefaultParallelism=%d" % effective_default_parallelism)
    return sha("\u0001".join(keep))


def probe(spark, df, label):
    ep = df._jdf.queryExecution().executedPlan()
    try:
        if "AdaptiveSparkPlanExec" in ep.getClass().getName():
            f = ep.getClass().getDeclaredField("currentPhysicalPlan")
            f.setAccessible(True)
            ep = f.get(ep)
    except Exception:
        pass

    struct = "\n".join(structural_string(ep))
    dpp = dpp_pruning_digest(ep)
    cfg = config_digest(spark, spark.sparkContext.defaultParallelism)
    return {
        "label": label,
        "planClass": ep.getClass().getSimpleName(),
        "hashCodeFingerprint": hashcode_fingerprint(ep),
        "structuralDigest": sha(struct),
        "pruningDigest": dpp,
        "configDigest": cfg,
        "fullDigest": sha("\u0003".join([sha(struct), dpp or "-", cfg])),
        "structuralPreview": struct[:1500],
    }


def build_tables(spark, drift):
    """fact is partitioned by region so DPP prunes partitions; dim is the build side whose
    distinct keys become the pruning keys. --drift mutates dim, which must change pruningDigest
    and therefore fullDigest."""
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    os.makedirs(WAREHOUSE, exist_ok=True)

    (spark.range(0, 400000)
        .selectExpr("id",
                    "cast(id % 13 as int) as region",
                    "cast(id % 97 as int) as product",
                    "cast(id * 1.5 as double) as amount")
        .write.mode("overwrite").partitionBy("region").parquet(WAREHOUSE + "/fact"))

    keep = [1, 3, 5, 7] if not drift else [1, 3, 5, 7, 9]        # the drift
    (spark.createDataFrame([(r, "r%d" % r) for r in keep], ["region", "name"])
        .write.mode("overwrite").parquet(WAREHOUSE + "/dim"))

    spark.read.parquet(WAREHOUSE + "/fact").createOrReplaceTempView("fact")
    spark.read.parquet(WAREHOUSE + "/dim").createOrReplaceTempView("dim")


QUERIES = {
    "no_dpp": "SELECT region, sum(amount) AS total FROM fact GROUP BY region",
    "dpp": """SELECT d.name, sum(f.amount) AS total
              FROM fact f JOIN dim d ON f.region = d.region
              GROUP BY d.name""",
    # current_timestamp() is the everyday ELT pattern that query-scoped constant replay
    # (LLD section 3.3) is designed to rescue. Without replay this digest differs every run.
    "constants": """SELECT region, count(*) AS n, current_timestamp() AS loaded_at
                    FROM fact GROUP BY region""",
}


def session(app):
    return (SparkSession.builder.master("local[4]").appName(app)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
            .config("spark.sql.shuffle.partitions", "50")
            .config("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
            .config("spark.local.dir", "/tmp/spark-local-digest-" + app)
            .getOrCreate())


def run(phase, drift):
    spark = session("digest" + phase)
    build_tables(spark, drift)

    results = {}
    for label, q in QUERIES.items():
        df = spark.sql(q)
        df.collect()                       # force AQE and subquery execution
        results[label] = probe(spark, df, label)

    os.makedirs(STATE, exist_ok=True)
    json.dump(results, open(os.path.join(
        STATE, "phase%s%s.json" % (phase, "_drift" if drift else "")), "w"), indent=2)

    print("\n== phase %s%s ==" % (phase, "  (DRIFT)" if drift else ""))
    for k, v in results.items():
        print("  %-11s plan=%-24s hashCode=%-10s structural=%s pruning=%s"
              % (k, v["planClass"], v["hashCodeFingerprint"],
                 v["structuralDigest"][:12], (v["pruningDigest"] or "-")[:12]))

    if phase != "B":
        spark.stop()
        return

    a = json.load(open(os.path.join(STATE, "phaseA.json")))
    print("\n%-8s %-34s %-9s %-9s" % ("", "check", "expected", "actual"))
    for k in results:
        # `constants` is expected to differ across runs until constant replay is implemented --
        # that is the point of including it.
        drifted_input = drift and k == "dpp"
        for field in ("hashCodeFingerprint", "structuralDigest", "fullDigest"):
            same = a[k][field] == results[k][field]
            got = "match" if same else "differ"
            if k == "constants":
                want = "differ"      # documents the gap that constant replay closes
            elif drifted_input and field == "fullDigest":
                want = "differ"
            elif drift and k != "dpp":
                want = "match"
            else:
                want = "match"
            flag = "PASS" if got == want else "FAIL"
            print("  [%s] %-34s %-9s %-9s" % (flag, "%s.%s" % (k, field), want, got))
        if k == "dpp":
            same = a[k]["pruningDigest"] == results[k]["pruningDigest"]
            got = "match" if same else "differ"
            want = "differ" if drift else "match"
            print("  [%s] %-34s %-9s %-9s"
                  % ("PASS" if got == want else "FAIL", "dpp.pruningDigest", want, got))

    print("\nNext: repeat against a real versioned-table connector. The Parquet case above is the")
    print("easy one; the connector case is what actually decides whether anchors ever hit.")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["A", "B"], required=True)
    ap.add_argument("--drift", action="store_true",
                    help="mutate the dimension table; digests MUST then differ")
    args = ap.parse_args()
    run(args.phase, args.drift)
