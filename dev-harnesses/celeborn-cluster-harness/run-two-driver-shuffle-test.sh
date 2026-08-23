#!/usr/bin/env bash
# Two-driver shuffle-stage recovery against a real Celeborn HA ensemble.
# Scenarios implemented here are the shuffle-only subset of E2E-TWO-DRIVER-TEST-PLAN.md:
#   C0  control, no kill
#   S2  crash after the first stage's catalog is published -> the replacement adopts it
#   S3  replacement runs a different query -> no adoption, no mixing
# The write-side scenarios need Iceberg and are blocked on the Spark-version decision.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib/cluster.sh"

WORK="${WORK_DIR:-/tmp/two-driver-shuffle}"
LOGS="$WORK/logs"
CONF="$WORK/conf"
SPARK_MVN="$HERE/../spark-resumable-upstream/build/mvn"
PASS=0; FAIL=0

log()   { echo "E2E-UPSTREAM: $*"; }
check() { if [[ "$3" == "$4" ]]; then log "[$1] PASS $2 ($3)"; PASS=$((PASS+1));
          else log "[$1] FAIL $2 (got '$3', want '$4')"; FAIL=$((FAIL+1)); fi; }

trap 'stop_cluster' EXIT

rm -rf "$WORK"; mkdir -p "$WORK" "$LOGS" "$CONF"
sed "s#__WORKER_DATA_DIR__#$WORK/worker-data#" "$HERE/conf/celeborn-defaults.conf" \
  > "$CONF/celeborn-defaults.conf"

log "probing whether Celeborn's Spark client builds against the fork"
if ! "$HERE/probe-versions.sh" > "$LOGS/probe.log" 2>&1; then
  log "BLOCKED: $(tail -1 "$LOGS/probe.log")"
  log "see $LOGS/probe.log; nothing below can run until that is resolved"
  exit 2
fi

log "building celeborn and the harness (1 core)"
build_celeborn_classpaths "$WORK" || { log "celeborn build failed, see $WORK/celeborn-build.log"; exit 1; }
( cd "$HERE" && taskset -c 0 "$SPARK_MVN" -q -T 1 -DskipTests package ) || exit 1
taskset -c 0 "$SPARK_MVN" -q -f "$HERE/pom.xml" dependency:build-classpath \
  -Dmdep.outputFile="$WORK/harness-cp.txt" >/dev/null || exit 1
CP="$HERE/target/classes:$(cat "$WORK/harness-cp.txt"):$CELEBORN_DIR/client-spark/spark-3/target/classes:$CELEBORN_DIR/client/target/classes:$CELEBORN_DIR/common/target/classes"

log "starting a 3-master HA ensemble and 2 workers"
start_masters "$WORK" "$CONF" "$LOGS" || exit 1
start_workers "$WORK" "$CONF" "$LOGS" 2

run_driver() { # run_driver <scenario> <tag> <recoveryId> <killAfterStages>
  taskset -c 0 java -Xmx2g \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$CP" org.apache.spark.recovery.cluster.TwoDriverShuffleJob \
    "localhost:9097,localhost:9098,localhost:9099" "$3" "app-$1" "30s" "$4" "$WORK/$1/out-$2" \
    > "$LOGS/$1-$2.log" 2>&1
  echo $?
}
adopted_stages() { grep -c 'E2E-STAGE-COMPLETED.*tasks=0' "$LOGS/$1-$2.log" 2>/dev/null || echo 0; }

log "C0 control"
rc=$(run_driver c0 driver run-c0 0)
check C0 "driver exits 0" "$rc" "0"
check C0 "no stage is adopted in a first run" "$(adopted_stages c0 driver)" "0"

log "S2 driver A halts after its first completed stage, driver B resumes"
run_driver s2 driverA run-s2 1 >/dev/null
rc=$(run_driver s2 driverB run-s2 0)
check S2 "driver B exits 0" "$rc" "0"
check S2 "at least one stage runs zero map tasks" \
  "$([[ $(adopted_stages s2 driverB) -ge 1 ]] && echo yes || echo no)" "yes"

log "S3 driver B runs a different recovery identity: nothing may be adopted"
rc=$(run_driver s3 driverB run-s3-different 0)
check S3 "driver B exits 0" "$rc" "0"
check S3 "no adoption under a different identity" "$(adopted_stages s3 driverB)" "0"

log "======== $PASS passed, $FAIL failed ========"
[[ $FAIL -eq 0 ]]
