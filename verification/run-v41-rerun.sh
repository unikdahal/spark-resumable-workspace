#!/usr/bin/env bash
# Targeted rerun: Iceberg v4.1 recovery tests after the replaceView throws-clause fix.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
ICEBERG="$HERE/../oss-fixes/iceberg"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

( cd "$ICEBERG" && nice -n 5 taskset -c 0,1 env "JAVA_HOME=$JAVA_HOME" \
    ./gradlew --max-workers=2 --console=plain \
    :iceberg-spark:iceberg-spark-4.1_2.13:test \
    --tests org.apache.iceberg.spark.source.TestSparkWriteRecovery ) \
    > "$LOGS/iceberg-v41-recovery4.log" 2>&1
rc=$?
summary=$(grep -cE "PASSED" "$LOGS/iceberg-v41-recovery4.log")
if [[ $rc -eq 0 ]]; then
  printf 'iceberg:v41-port\tPASS\t%s tests passed\n' "$summary" | tee -a "$LOGS/results-v41-port.tsv"
else
  grep -E 'FAILED|error:' "$LOGS/iceberg-v41-recovery4.log" | head -6 \
    | tee -a "$LOGS/results-v41-port.tsv"
  printf 'iceberg:v41-port\tFAIL\tsee %s\n' "$LOGS/iceberg-v41-recovery4.log" | tee -a "$LOGS/results-v41-port.tsv"
fi
column -t -s $'\t' "$LOGS/results-v41-port.tsv" 2>/dev/null
