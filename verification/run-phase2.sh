#!/usr/bin/env bash
# Phase 2: everything that needed a correction after the first pass — the in-tree Spark suites added
# here, Celeborn and Iceberg under a JDK they support. Runs strictly after any in-flight build.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SPARK="$HERE/../spark-resumable-upstream"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-phase2.tsv"; : > "$RESULTS"

while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

run_spark_suite() { # <module> <suite> <tag>
  local log="$LOGS/spark-$3.log"
  ( cd "$SPARK" && nice -n 10 taskset -c 0 env MAVEN_OPTS="-Xss64m -Xmx3g" \
      ./build/mvn -T 1 -pl "$1" -Dtest=none -DwildcardSuites="$2" \
      -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
      -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test ) > "$log" 2>&1
  local rc=$?
  local summary
  summary=$(grep -E 'Total number of tests run|TESTS FAILED' "$log" | tail -2 | tr '\n' ' ')
  if [[ $rc -eq 0 ]]; then record "spark:$3" PASS "${summary:-no summary}"
  else record "spark:$3" FAIL "rc=$rc ${summary:-see $log}"; fi
}

run_spark_suite sql/core \
  org.apache.spark.sql.execution.datasources.v2.BatchWriteRecoverySuite write-recovery
run_spark_suite sql/core \
  org.apache.spark.sql.execution.adaptive.RecoveryKeyDiscriminationSuite recovery-key-rerun

( cd "$SPARK" && nice -n 10 taskset -c 0 ./build/mvn -T 1 -pl core,sql/catalyst,sql/core \
    scalastyle:check ) > "$LOGS/spark-scalastyle2.log" 2>&1
if [[ $? -eq 0 ]]; then record spark:scalastyle PASS "core, catalyst, sql/core"
else record spark:scalastyle FAIL "$(grep -c '^error file' "$LOGS/spark-scalastyle2.log") violations"; fi

"$HERE/run-celeborn-suites.sh" >> "$LOGS/phase2-celeborn.log" 2>&1
cat "$LOGS/results-celeborn.tsv" >> "$RESULTS" 2>/dev/null

"$HERE/run-iceberg-ledger.sh" >> "$LOGS/phase2-iceberg.log" 2>&1
cat "$LOGS/results-iceberg.tsv" >> "$RESULTS" 2>/dev/null

echo
column -t -s $'\t' "$RESULTS"
