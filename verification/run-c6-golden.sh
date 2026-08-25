#!/usr/bin/env bash
# C6: regenerate golden fixtures so every accepted envelope/manifest layout
# (task v1/v2/v3 + write-manifest v4) is pinned against drift.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
SPARK="$HERE/../spark-resumable-upstream"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

log="$LOGS/spark-c6-golden.log"
( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    SPARK_GENERATE_GOLDEN_FILES=1 \
    ./build/mvn -T 1 -pl sql/core -Dtest=none \
    -DwildcardSuites=org.apache.spark.sql.execution.datasources.v2.RecoveryTaskCommitCompatibilitySuite \
    -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test ) > "$log" 2>&1
rc=$?
if [[ $rc -eq 0 ]]; then
  printf 'spark:c6-golden\tPASS\t%s\n' "$(grep -E 'Total number of tests run' "$log" | tail -1)" | tee -a "$LOGS/results-c6.tsv"
  echo "fixtures now present:" | tee -a "$LOGS/results-c6.tsv"
  ls "$SPARK/sql/core/src/test/resources/recovery/" | tee -a "$LOGS/results-c6.tsv"
else
  printf 'spark:c6-golden\tFAIL\trc=%s\n' "$rc" | tee -a "$LOGS/results-c6.tsv"
  grep -E '\[Error\]' "$log" | head -3 | tee -a "$LOGS/results-c6.tsv"
fi
column -t -s $'\t' "$LOGS/results-c6.tsv" 2>/dev/null
