#!/usr/bin/env bash
# Queued: C10 evidence - compile and run ShuffleStageRecoveryAQESuite
# (stage reuse / coalescing / skew over an adopted stage).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
SPARK="$HERE/../spark-resumable-upstream"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

log="$LOGS/spark-c10-aqe.log"
if ( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
      ./build/mvn -T 1 -pl sql/core -Dtest=none \
      -DwildcardSuites=org.apache.spark.sql.execution.adaptive.ShuffleStageRecoveryAQESuite \
      -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
      -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test ) > "$log" 2>&1; then
  printf 'spark:c10-aqe\tPASS\t%s\n' "$(grep -E 'Total number of tests run' "$log" | tail -1)" \
    | tee "$LOGS/results-spark-c10.tsv"
else
  { grep -E '\[Error\]|\*\*\* FAILED' "$log" | head -4
    printf 'spark:c10-aqe\tFAIL\tsee %s\n' "$log"; } | tee "$LOGS/results-spark-c10.tsv"
fi
column -t -s $'\t' "$LOGS/results-spark-c10.tsv" 2>/dev/null
