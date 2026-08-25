#!/usr/bin/env bash
# Targeted: run the recovery-only sourceset against the forked Spark (B6 verdict + B2 + B3).
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
    :iceberg-spark:iceberg-spark-4.1_2.13:recoveryTest ) \
    > "$LOGS/iceberg-v41-recovery5.log" 2>&1
rc=$?
if [[ $rc -eq 0 ]]; then
  printf 'iceberg:v41-recovery\tPASS\t%s\n' \
    "$(grep -cE 'PASSED' "$LOGS/iceberg-v41-recovery5.log") passed" \
    | tee -a "$LOGS/results-v41-port.tsv"
else
  { grep -E 'error:|FAILED' "$LOGS/iceberg-v41-recovery5.log" | head -6
    printf 'iceberg:v41-recovery\tFAIL\tsee %s\n' "$LOGS/iceberg-v41-recovery5.log"; } \
    | tee -a "$LOGS/results-v41-port.tsv"
fi
column -t -s $'\t' "$LOGS/results-v41-port.tsv" 2>/dev/null | tail -4
