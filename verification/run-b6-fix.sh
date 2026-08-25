#!/usr/bin/env bash
# Corrective batch, queued behind whatever holds the verification lock:
#   1. DAGSchedulerSuite after the fixture fix (consumer partitions must match the partitioner).
#   2. Install fork sql/pipelines + sql/hive into mavenLocal (-am; spark-hive depends on
#      spark-pipelines, which was never installed) so Iceberg v4.1 can resolve the fork.
#   3. Iceberg formatting gate + the v4.1 recovery test leg that could not resolve before.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
SPARK="$HERE/../spark-resumable-upstream"
ICEBERG="$HERE/../oss-fixes/iceberg"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }
RESULTS="$LOGS/results-b6-fix.tsv"; : > "$RESULTS"

spark_mvn() { ( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    ./build/mvn -T 1 "$@" ); }

echo "== 1. DAGSchedulerSuite after fixture fix =="
log="$LOGS/spark-dagscheduler-fixed.log"
if spark_mvn -pl core -Dtest=none \
    -DwildcardSuites=org.apache.spark.scheduler.DAGSchedulerSuite \
    -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test > "$log" 2>&1; then
  record "spark:dagscheduler-fixed" PASS "$(grep -E 'Tests: succeeded' "$log" | tail -1)"
else
  record "spark:dagscheduler-fixed" FAIL "$(grep -E '\*\*\* FAILED' "$log" | head -2 | tr '\n' ' ')"
fi

echo "== 2. installing fork sql/pipelines + sql/hive (-am) =="
log="$LOGS/spark-hive-install2.log"
if spark_mvn -pl sql/hive,sql/pipelines -am -DskipTests -Dmaven.source.skip \
    -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true -Dscalastyle.skip=true \
    -Dcheckstyle.skip=true -Dcyclonedx.skip=true install > "$log" 2>&1; then
  record "spark:hive-pipelines-install" PASS "published to mavenLocal"
else
  record "spark:hive-pipelines-install" FAIL "$(grep -E '\[ERROR\]' "$log" | grep -v Help | head -3 | tr '\n' ' ')"
fi

echo "== 3. Iceberg v4.1 against the fork =="
ice_gradle() { ( cd "$ICEBERG" && nice -n 5 taskset -c 0,1 env "JAVA_HOME=$JAVA_HOME" \
    ./gradlew --max-workers=2 --console=plain "$@" ); }
if ice_gradle spotlessApply > "$LOGS/iceberg-spotless2.log" 2>&1 \
    && ice_gradle :iceberg-spark:iceberg-spark-4.1_2.13:test \
        --tests org.apache.iceberg.spark.source.TestSparkWriteRecovery \
        > "$LOGS/iceberg-v41-recovery2.log" 2>&1; then
  record "iceberg:B6+B2+B3-v41" PASS ""
else
  record "iceberg:B6+B2+B3-v41" FAIL "$(grep -E 'FAILED|error:' "$LOGS/iceberg-spotless2.log" "$LOGS/iceberg-v41-recovery2.log" 2>/dev/null | head -5 | tr '\n' ' ')"
fi

column -t -s $'\t' "$RESULTS"
