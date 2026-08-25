#!/usr/bin/env bash
# C6b: regenerate + verify v3 envelope / v4 manifest golden fixtures.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
SPARK="$HERE/../spark-resumable-upstream"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"
exec 9>"$LOGS/.verification.lock"
flock 9

run() { # $1 = extra env
  ( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" $1 \
      ./build/mvn -q -T 1 -pl sql/core -Dtest=none \
      -DwildcardSuites=org.apache.spark.sql.execution.datasources.v2.RecoveryTaskCommitCompatibilitySuite \
      -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
      -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test ) \
    > "$LOGS/spark-c6-golden.log" 2>&1
}

if ! run "SPARK_GENERATE_GOLDEN_FILES=1"; then echo "generate FAILED"; tail -5 "$LOGS/spark-c6-golden.log"; exit 1; fi
ls "$SPARK/sql/core/src/test/resources/recovery/"
echo "--- verify pass ---"
if run ""; then
  grep -E "Tests run|All tests" "$LOGS/spark-c6-golden.log" | tail -1
  printf 'spark:c6-golden-v3\tPASS\n' | tee -a "$LOGS/results-c6.tsv"
else
  echo "verify FAILED"; grep -E "\[Error\]|AssertionFailed" "$LOGS/spark-c6-golden.log" | head -5
  printf 'spark:c6-golden-v3\tFAIL\n' | tee -a "$LOGS/results-c6.tsv"
fi
