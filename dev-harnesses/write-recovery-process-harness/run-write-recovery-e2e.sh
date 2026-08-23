#!/usr/bin/env bash
# Two-driver write-recovery scenarios against the forked Spark. Every build and JVM here is pinned
# to one core. See docs/upstream/WRITE-RECOVERY-HARNESS.md for what each scenario proves.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
MVN="${MVN:-$HERE/../spark-resumable-upstream/build/mvn}"
WORK="${WORK_DIR:-/tmp/write-recovery-e2e}"
ROWS="${ROWS:-2000}"
PARTITIONS="${PARTITIONS:-6}"
PASS=0
FAIL=0

log()  { echo "E2E-UPSTREAM: $*"; }
check() { # check <name> <condition-description> <actual> <expected>
  if [[ "$3" == "$4" ]]; then log "[$1] PASS $2 ($3)"; PASS=$((PASS+1));
  else log "[$1] FAIL $2 (got '$3', want '$4')"; FAIL=$((FAIL+1)); fi
}
check_contains() {
  if grep -q -- "$3" "$2"; then log "[$1] PASS log contains '$3'"; PASS=$((PASS+1));
  else log "[$1] FAIL log does not contain '$3'"; FAIL=$((FAIL+1)); fi
}

build() {
  log "building harness (1 core)"
  ( cd "$HERE" && taskset -c 0 "$MVN" -q -T 1 -DskipTests package ) || exit 1
  taskset -c 0 "$MVN" -q -f "$HERE/pom.xml" dependency:build-classpath \
    -Dmdep.outputFile="$WORK/cp.txt" >/dev/null || exit 1
  CP="$HERE/target/classes:$HERE/src/main/resources:$(cat "$WORK/cp.txt")"
}

run_driver() { # run_driver <scenario> <tag> <fault> <partitions>
  local scenario="$1" tag="$2" fault="$3" parts="$4"
  local store="$WORK/$scenario/store" sink="$WORK/$scenario/sink"
  mkdir -p "$store" "$sink"
  taskset -c 0 java -Xmx2g \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$CP" org.apache.spark.recovery.e2e.TwoDriverWriteJob \
    "$store" "$sink" "exec-$scenario" "$parts" "$ROWS" "$fault" \
    > "$WORK/$scenario/$tag.log" 2>&1
  echo $?
}

count_marker() { local n; n=$(grep -c -- "$2" "$WORK/$1" 2>/dev/null); echo "${n:-0}"; }
committed_rows() {
  local marker="$WORK/$1/sink/_COMMITTED"
  [[ -f "$marker" ]] || { echo -1; return; }
  awk '{ for (i=1;i<=NF;i++) if ($i ~ /^rows=/) { split($i,a,"="); s+=a[2] } } END { print s+0 }' "$marker"
}

rm -rf "$WORK"; mkdir -p "$WORK"
build

# ---------------------------------------------------------------- C0: control
log "C0 control run, no fault"
rc=$(run_driver c0 driver none "$PARTITIONS")
check C0 "driver exits 0" "$rc" "0"
check_contains C0 "$WORK/c0/driver.log" "E2E-RESULT SUCCESS"
check C0 "writers created" "$(count_marker c0/driver.log E2E-WRITER-CREATED)" "$PARTITIONS"
check C0 "rows committed" "$(committed_rows c0)" "$ROWS"

# ------------------------------------------- S1: crash with 2 durable commits
log "S1 driver A halts after 2 durable task commits, driver B resumes"
rc=$(run_driver s1 driverA halt-after-commits:2 "$PARTITIONS")
check S1 "driver A dies" "$([[ $rc -ne 0 ]] && echo killed || echo survived)" "killed"
a_writers=$(count_marker s1/driverA.log E2E-WRITER-CREATED)
rc=$(run_driver s1 driverB none "$PARTITIONS")
check S1 "driver B exits 0" "$rc" "0"
b_writers=$(count_marker s1/driverB.log E2E-WRITER-CREATED)
check S1 "driver B skips durably committed partitions" \
  "$(( b_writers + 2 <= PARTITIONS ? 1 : 0 ))" "1"
check S1 "driver B rows committed" "$(committed_rows s1)" "$ROWS"
log "S1 detail: driver A created $a_writers writers, driver B created $b_writers"

# --------------------------------------------- S2: accepted publish, lost reply
log "S2 publish accepted for partition 1 but the reply is lost"
rc=$(run_driver s2 driver drop-reply-after-accept:1 "$PARTITIONS")
check S2 "driver exits 0 after the retry" "$rc" "0"
check S2 "no partition is written twice" \
  "$(count_marker s2/driver.log E2E-WRITER-CREATED)" "$PARTITIONS"
check S2 "canonical commit adopted in preflight" \
  "$(count_marker s2/driver.log 'E2E-STORE-PREFLIGHT-HIT')" "1"
check S2 "rows committed" "$(committed_rows s2)" "$ROWS"

# ------------------------------------------------ S3: already fully committed
log "S3 re-running an execution that already committed globally"
run_driver s3 driverA none "$PARTITIONS" >/dev/null
rc=$(run_driver s3 driverB none "$PARTITIONS")
check S3 "driver B exits 0" "$rc" "0"
check S3 "no writer tasks" "$(count_marker s3/driverB.log E2E-WRITER-CREATED)" "0"
check S3 "no second global commit" "$(count_marker s3/driverB.log E2E-GLOBAL-COMMIT)" "0"
check_contains S3 "$WORK/s3/driverB.log" "was already"

# ------------------------------------------------------- S4: manifest mismatch
log "S4 replacement driver plans a different partition count"
run_driver s4 driverA halt-after-commits:2 "$PARTITIONS" >/dev/null
rc=$(run_driver s4 driverB none "$(( PARTITIONS + 2 ))")
check S4 "driver B fails closed" "$([[ $rc -ne 0 ]] && echo failed || echo succeeded)" "failed"
check_contains S4 "$WORK/s4/driverB.log" "manifest"

# ------------------------------------------------------- S5: corrupt envelope
log "S5 a durable record is corrupted before it is read"
run_driver s5 driverA halt-after-commits:2 "$PARTITIONS" >/dev/null
rc=$(run_driver s5 driverB corrupt-on-read:0 "$PARTITIONS")
check S5 "driver B fails closed" "$([[ $rc -ne 0 ]] && echo failed || echo succeeded)" "failed"
check_contains S5 "$WORK/s5/driverB.log" "checksum"

log "======== $PASS passed, $FAIL failed ========"
[[ $FAIL -eq 0 ]]
