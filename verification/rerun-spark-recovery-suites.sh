#!/usr/bin/env bash
# Re-runs the Spark suites that were fixed after the first pass. Serialized behind any other
# verification run through a lock file, and behind any in-flight JVM build.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SPARK="$HERE/../spark-resumable-upstream"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-spark-rerun.tsv"; : > "$RESULTS"

exec 9>"$LOGS/.verification.lock"
flock 9

while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

run_suite() { # <module> <suite> <tag>
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

# The golden fixture does not exist yet; record it once, then verify it like any other run.
if [[ ! -f "$SPARK/sql/core/src/test/resources/recovery/task-commit-envelope-v1.txt" ]]; then
  ( cd "$SPARK" && nice -n 10 taskset -c 0 env MAVEN_OPTS="-Xss64m -Xmx3g" \
      SPARK_GENERATE_GOLDEN_FILES=1 ./build/mvn -T 1 -pl sql/core -Dtest=none \
      -DwildcardSuites=org.apache.spark.sql.execution.datasources.v2.RecoveryTaskCommitCompatibilitySuite \
      -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true \
      -Dmaven.scaladoc.skip=true test ) > "$LOGS/spark-golden-record.log" 2>&1
  record spark:golden-fixture RECORDED "generated task-commit-envelope-v1.txt"
fi

run_suite sql/core \
  org.apache.spark.sql.execution.datasources.v2.RecoveryTaskCommitCompatibilitySuite envelope-compat
run_suite sql/core \
  org.apache.spark.sql.execution.datasources.v2.BatchWriteRecoverySuite write-recovery
run_suite sql/core \
  org.apache.spark.sql.execution.adaptive.RecoveryKeyDiscriminationSuite recovery-key

( cd "$SPARK" && nice -n 10 taskset -c 0 ./build/mvn -T 1 -pl core,sql/catalyst,sql/core \
    scalastyle:check ) > "$LOGS/spark-scalastyle3.log" 2>&1
if [[ $? -eq 0 ]]; then record spark:scalastyle PASS "core, catalyst, sql/core"
else record spark:scalastyle FAIL "$(grep -c '^error file' "$LOGS/spark-scalastyle3.log") violations"; fi

column -t -s $'\t' "$RESULTS"
