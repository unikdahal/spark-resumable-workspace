#!/usr/bin/env bash
# C1 triage: refresh the .m2 jars from the working tree, then run every recovery suite the C1 task
# names, including the four uncommitted row-level suites. Produces results-spark-c1.tsv.
# Roadmap §2 constraints: JDK 17, CPUs 0-1, one build at a time, Maven single-threaded.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SPARK="$HERE/../spark-resumable-upstream"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-spark-c1.tsv"; : > "$RESULTS"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9
while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

spark() { ( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    ./build/mvn -T 1 "$@" ); }

run_suite() { # <module> <suite> <tag>
  local log="$LOGS/spark-c1-$3.log"
  spark -pl "$1" -Dtest=none -DwildcardSuites="$2" \
    -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test > "$log" 2>&1
  local rc=$?
  local summary
  summary=$(grep -E 'Total number of tests run|TESTS FAILED' "$log" | tail -2 | tr '\n' ' ')
  if [[ $rc -eq 0 ]]; then record "spark:$3" PASS "${summary:-no summary}"
  else record "spark:$3" FAIL "rc=$rc ${summary:-see $log}"; fi
}

echo "== refreshing installed jars from the working tree =="
if [[ "${SKIP_INSTALL:-0}" != "1" ]]; then
  if ! spark -pl core,sql/api,sql/catalyst -am -DskipTests \
      -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
      -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true \
      install > "$LOGS/spark-c1-install.log" 2>&1; then
    record "spark:install" FAIL "$(grep -E '\[ERROR\]' "$LOGS/spark-c1-install.log" | head -3 | tr '\n' ' ')"
    column -t -s $'\t' "$RESULTS"; exit 1
  fi
fi
record "spark:install" PASS "core, sql/api, sql/catalyst reinstalled"

V2=org.apache.spark.sql.execution.datasources.v2
run_suite sql/core $V2.RecoveryTaskCommitSuite task-commit
run_suite sql/core $V2.RecoveryTaskCommitCompatibilitySuite envelope-compat
run_suite sql/core $V2.BatchWriteRecoverySuite write-recovery
run_suite sql/core org.apache.spark.sql.execution.adaptive.RecoveryKeyDiscriminationSuite recovery-key
run_suite sql/core $V2.RowLevelWriteManifestSuite rowlevel-manifest
run_suite sql/core $V2.RowLevelTaskSummaryAccumulatorSuite rowlevel-accumulator
run_suite sql/core $V2.RowLevelSemanticWritingTaskSuite rowlevel-semantic
run_suite sql/core $V2.RowLevelTaskRecoveryStateSuite rowlevel-state
run_suite sql/core org.apache.spark.sql.SparkSessionExtensionSuite extensions
run_suite core org.apache.spark.scheduler.DAGSchedulerSuite dagscheduler
run_suite core org.apache.spark.MapOutputTrackerSuite mapoutput

( cd "$SPARK" && nice -n 5 taskset -c 0,1 ./build/mvn -T 1 -pl core,sql/catalyst,sql/core \
    scalastyle:check ) > "$LOGS/spark-c1-scalastyle.log" 2>&1
if [[ $? -eq 0 ]]; then record spark:scalastyle PASS "core, catalyst, sql/core"
else record spark:scalastyle FAIL "$(grep -c '^error file' "$LOGS/spark-c1-scalastyle.log") violations"; fi

column -t -s $'\t' "$RESULTS"
