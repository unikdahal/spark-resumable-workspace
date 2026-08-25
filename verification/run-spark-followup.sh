#!/usr/bin/env bash
# Follow-up batch after C1: scalastyle gate after the V2Writes argcount fix, regression suites over
# the refactored row-level manifest construction, and a repro rerun of the failing DAGScheduler
# recovery test. Same constraints as the other runners.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SPARK="$HERE/../spark-resumable-upstream"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-spark-followup.tsv"; : > "$RESULTS"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

spark() { ( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    ./build/mvn -T 1 "$@" ); }

( cd "$SPARK" && nice -n 5 taskset -c 0,1 ./build/mvn -T 1 -pl sql/core \
    scalastyle:check ) > "$LOGS/spark-followup-scalastyle.log" 2>&1
if [[ $? -eq 0 ]]; then record spark:scalastyle PASS "sql/core"
else record spark:scalastyle FAIL "$(grep -E '^error file' "$LOGS/spark-followup-scalastyle.log" | head -2 | tr '\n' ' ')"; fi

V2=org.apache.spark.sql.execution.datasources.v2
for entry in "$V2.RowLevelWriteManifestSuite rowlevel-manifest" \
             "$V2.RowLevelTaskSummaryAccumulatorSuite rowlevel-accumulator" \
             "$V2.RowLevelSemanticWritingTaskSuite rowlevel-semantic" \
             "$V2.RowLevelTaskRecoveryStateSuite rowlevel-state" \
             "org.apache.spark.sql.SparkSessionExtensionSuite extensions"; do
  suite=$(echo "$entry" | awk '{print $1}')
  tag=$(echo "$entry" | awk '{print $2}')
  log="$LOGS/spark-followup-$tag.log"
  spark -pl sql/core -Dtest=none -DwildcardSuites="$suite" \
    -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test > "$log" 2>&1
  rc=$?
  summary=$(grep -E 'Total number of tests run|TESTS FAILED' "$log" | tail -2 | tr '\n' ' ')
  if [[ $rc -eq 0 ]]; then record "spark:$tag" PASS "${summary:-no summary}"
  else record "spark:$tag" FAIL "rc=$rc ${summary:-see $log}"; fi
done

log="$LOGS/spark-followup-dagscheduler.log"
spark -pl core -Dtest=none \
  -DwildcardSuites=org.apache.spark.scheduler.DAGSchedulerSuite \
  -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
  -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test > "$log" 2>&1
rc=$?
summary=$(grep -E 'Tests: succeeded|FAILED' "$log" | tail -3 | tr '\n' ' ')
if [[ $rc -eq 0 ]]; then record spark:dagscheduler-repro PASS "${summary:-no summary}"
else record spark:dagscheduler-repro FAIL "rc=$rc ${summary:-see $log}"; fi

column -t -s $'\t' "$RESULTS"
