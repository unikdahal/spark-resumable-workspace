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

log="$LOGS/spark-c9-buildcp.log"
( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    ./build/mvn -q -T 1 -pl sql/core test-compile \
    -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true \
    dependency:build-classpath -Dmdep.outputFile=/home/unik/Coding/spark/tmp/c9/sqlcore-cp.txt ) > "$log" 2>&1

log="$LOGS/spark-c9-benchmark.log"
( cd "$SPARK/sql/core" && nice -n 5 taskset -c 0,1 env JAVA_HOME="$JAVA_HOME"     SPARK_GENERATE_BENCHMARK_FILES=1     "$JAVA_HOME/bin/java" -Xmx4g -Xss8m \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "target/scala-2.13/test-classes:target/scala-2.13/classes:/home/unik/Coding/spark/spark-resumable-upstream/core/target/scala-2.13/test-classes:$(cat /home/unik/Coding/spark/tmp/c9/sqlcore-cp.txt)" \
    org.apache.spark.sql.execution.benchmark.RecoveryTaskCommitBenchmark ) > "$log" 2>&1
rc=$?
grep -E "Benchmark|ms *$|best" "$log" | head -20 >> /home/unik/Coding/spark/tmp/c9/summary.txt 2>/dev/null
if [[ $rc -eq 0 ]]; then
  printf 'spark:c9-benchmark\tPASS\tresults in sql/core/benchmarks + %s\n' "/home/unik/Coding/spark/tmp/c9" | tee -a "$LOGS/results-c9.tsv"
else
  printf 'spark:c9-benchmark\tFAIL\trc=%s %s\n' "$rc" "$(tail -3 "$log" | tr '\n' ' ')" | tee -a "$LOGS/results-c9.tsv"
fi
# Copy whatever benchmark output was produced into tmp for the doc update.
find "$SPARK/sql/core/benchmarks" -name "*RecoveryTaskCommit*" -newermt "-30 minutes" \
  -exec cp {} "$OUT/" \; 2>/dev/null
ls "$OUT" | head -5
