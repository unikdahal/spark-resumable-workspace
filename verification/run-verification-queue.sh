#!/usr/bin/env bash
# Canonical verification pass for the resumable-execution work.
#
# Everything runs one at a time, pinned to one core. A failing gate never stops the pass: the point
# is to learn the state of every gate in a single run. Each gate appends
# "<gate>\t<PASS|FAIL>\t<detail>" to logs/results.tsv and writes its full output to logs/.
#
# Toolchains differ by project: Spark builds on the JDK in PATH (25 here), Celeborn and Iceberg do
# not (Celeborn's SignalUtils needs a JDK where sun.misc.Signal is visible), so those runs pin
# JDK 21 through their own scripts.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SPARK="$HERE/../spark-resumable-upstream"
LOGS="${LOGS_DIR:-$HERE/logs}"
RESULTS="$LOGS/results.tsv"

mkdir -p "$LOGS"
: > "$RESULTS"

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

wait_for_idle() {
  # Match real JVMs only: pgrep -f would also match this session's own shell command lines.
  while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done
}

run_spark_suite() { # run_spark_suite <module> <suite-fqcn> <tag>
  local module="$1" suite="$2" tag="$3" log="$LOGS/spark-$3.log"
  ( cd "$SPARK" && nice -n 10 taskset -c 0 env MAVEN_OPTS="-Xss64m -Xmx3g" \
      ./build/mvn -T 1 -pl "$module" -Dtest=none -DwildcardSuites="$suite" \
      -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
      -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test ) > "$log" 2>&1
  local rc=$?
  local summary
  summary=$(grep -E 'Total number of tests run|TESTS FAILED' "$log" | tail -2 | tr '\n' ' ')
  if [[ $rc -eq 0 ]]; then record "spark:$tag" PASS "${summary:-no summary line}"
  else record "spark:$tag" FAIL "rc=$rc ${summary:-see $log}"; fi
}

wait_for_idle

# ------------------------------------------------------------------ Spark suites
run_spark_suite sql/core \
  org.apache.spark.sql.execution.datasources.v2.RecoveryTaskCommitSuite envelope
run_spark_suite sql/core \
  org.apache.spark.sql.execution.datasources.v2.RecoveryTaskCommitCompatibilitySuite envelope-compat
run_spark_suite sql/core \
  org.apache.spark.sql.execution.datasources.v2.BatchWriteRecoverySuite write-recovery
run_spark_suite sql/core \
  org.apache.spark.sql.execution.adaptive.RecoveryKeyDiscriminationSuite recovery-key
run_spark_suite sql/core org.apache.spark.sql.SparkSessionExtensionSuite extensions
run_spark_suite core org.apache.spark.scheduler.DAGSchedulerSuite dagscheduler
run_spark_suite core org.apache.spark.MapOutputTrackerSuite mapoutput

# ------------------------------------------------------------------ Style gate
( cd "$SPARK" && nice -n 10 taskset -c 0 ./build/mvn -T 1 -pl core,sql/catalyst,sql/core \
    scalastyle:check ) > "$LOGS/spark-scalastyle.log" 2>&1
if [[ $? -eq 0 ]]; then record spark:scalastyle PASS "core, catalyst, sql/core"
else record spark:scalastyle FAIL "$(grep -c '^error file' "$LOGS/spark-scalastyle.log") violations"; fi

# ------------------------------------------------------------------ Celeborn (JDK 21)
"$HERE/run-celeborn-suites.sh" > "$LOGS/celeborn-run.log" 2>&1
cat "$LOGS/results-celeborn.tsv" >> "$RESULTS" 2>/dev/null

# ------------------------------------------------------------------ Iceberg ledger (JDK 21)
"$HERE/run-iceberg-ledger.sh" > "$LOGS/iceberg-run.log" 2>&1
cat "$LOGS/results-iceberg.tsv" >> "$RESULTS" 2>/dev/null

echo
echo "=== results ==="
column -t -s $'\t' "$RESULTS"
