#!/usr/bin/env bash
# Queued behind the lock: Celeborn gates for the repair-accounting fix
# (releaseRecoveryTaskCommitCapacity bytes/records split) — spotless, then the pointer suite.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CELEBORN="$HERE/../celeborn"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-celeborn-repair-fix.tsv"; : > "$RESULTS"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

mvn() { ( cd "$CELEBORN" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS='-XX:ActiveProcessorCount=2' \
    ./build/mvn -T 1 -Dmaven.javadoc.skip=true "$@" ); }

if mvn -pl master -am -DskipTests com.diffplug.spotless:spotless-maven-plugin:apply \
    > "$LOGS/celeborn-repair-spotless.log" 2>&1; then
  record "celeborn:repair-spotless" PASS ""
else
  record "celeborn:repair-spotless" FAIL "$(grep -E '\[ERROR\]' "$LOGS/celeborn-repair-spotless.log" | head -2 | tr '\n' ' ')"
fi

log="$LOGS/celeborn-repair-suite.log"
if mvn -pl master -am -Dtest=none \
    -DwildcardSuites=org.apache.celeborn.service.deploy.master.clustermeta.RecoveryBlobPointerSuite \
    test > "$log" 2>&1; then
  record "celeborn:repair-accounting" PASS "$(grep -E 'Total number of tests run' "$log" | tail -1)"
else
  record "celeborn:repair-accounting" FAIL "$(grep -E '\*\*\* FAILED|error:' "$log" | head -3 | tr '\n' ' ')"
fi

column -t -s $'\t' "$RESULTS"
