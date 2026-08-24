#!/usr/bin/env bash
# One locked session: (1) DAGSchedulerSuite with the triage diagnostic,
# (2) the Iceberg B6+B1+B4+B2+B3 verification batch, which re-acquires nothing else.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
exec 9>"$LOGS/.verification.lock"
flock 9

SPARK="$HERE/../spark-resumable-upstream"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    ./build/mvn -T 1 -pl core -Dtest=none \
    -DwildcardSuites=org.apache.spark.scheduler.DAGSchedulerSuite \
    -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
    -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true test ) \
    > "$LOGS/spark-dagscheduler-diag.log" 2>&1

echo "dagscheduler diag done rc=$?"
grep "DIAG" "$LOGS/spark-dagscheduler-diag.log" | head -2

# Release the lock so the iceberg batch owns it exclusively through its own runner.
flock -u 9
"$HERE/run-iceberg-b6.sh"
