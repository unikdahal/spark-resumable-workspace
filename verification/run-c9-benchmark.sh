#!/usr/bin/env bash
# C9: generate real RecoveryTaskCommitBenchmark results and stash them for the
# RETENTION-AND-SIZING doc update. Uses the workspace tmp dir for outputs.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
SPARK="$HERE/../spark-resumable-upstream"
OUT="/home/unik/Coding/spark/tmp/c9"
mkdir -p "$OUT"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

log="$LOGS/spark-c9-benchmark.log"
( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    SPARK_GENERATE_BENCHMARK_FILES=1 \
    ./build/mvn -T 1 -pl sql/core -Dtest=none \
    -DwildcardSuites=org.apache.spark.sql.execution.datasources.v2.RecoveryTaskCommitBenchmark \
    -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test ) > "$log" 2>&1
rc=$?
summary=$(grep -E 'Total number of tests run|BUILD' "$log" | tail -2 | tr '\n' ' ')
if [[ $rc -eq 0 ]]; then
  printf 'spark:c9-benchmark\tPASS\t%s\n' "$summary" | tee -a "$LOGS/results-c9.tsv"
else
  printf 'spark:c9-benchmark\tFAIL\trc=%s %s\n' "$rc" "$(grep -E '\[Error\]' "$log" | head -2 | tr '\n' ' ')" | tee -a "$LOGS/results-c9.tsv"
fi
# Copy whatever benchmark output was produced into tmp for the doc update.
find "$SPARK/sql/core/benchmarks" -name "*RecoveryTaskCommit*" -newermt "-30 minutes" \
  -exec cp {} "$OUT/" \; 2>/dev/null
ls "$OUT" | head -5
