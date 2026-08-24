#!/usr/bin/env bash
# Queued behind the lock: Celeborn A4 (per-application inline quota) gates —
# spotless, the new capacity suite, the pointer suite it touches, and the
# configuration-docs regeneration gate.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CELEBORN="$HERE/../celeborn"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-celeborn-a4.tsv"; : > "$RESULTS"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9
while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

mvn() { ( cd "$CELEBORN" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS='-XX:ActiveProcessorCount=2' \
    ./build/mvn -T 1 -Dmaven.javadoc.skip=true "$@" ); }

if mvn -pl common,master -am -DskipTests com.diffplug.spotless:spotless-maven-plugin:apply \
    > "$LOGS/celeborn-a4-spotless.log" 2>&1; then
  record "celeborn:a4-spotless" PASS ""
else
  record "celeborn:a4-spotless" FAIL "$(grep -E '\[ERROR\]' "$LOGS/celeborn-a4-spotless.log" | head -2 | tr '\n' ' ')"
fi

for entry in \
  "org.apache.celeborn.service.deploy.master.clustermeta.RecoveryInlineCapacitySuite inlinecapacity" \
  "org.apache.celeborn.service.deploy.master.clustermeta.RecoveryBlobPointerSuite blobpointer" \
  "org.apache.celeborn.ConfigurationSuite configdocs"; do
  suite=$(echo "$entry" | awk '{print $1}')
  tag=$(echo "$entry" | awk '{print $2}')
  log="$LOGS/celeborn-a4-$tag.log"
  if [[ "$tag" == "configdocs" ]]; then
    args=(-pl common -am -Dtest=none -DwildcardSuites="$suite")
    env_args=(UPDATE=1)
  else
    args=(-pl master -am -Dtest=none -DwildcardSuites="$suite")
    env_args=()
  fi
  if mvn "${args[@]}" env "${env_args[@]}" test > "$log" 2>&1; then
    record "celeborn:a4-$tag" PASS "$(grep -E 'Total number of tests run|Tests run:' "$log" | tail -1)"
  else
    record "celeborn:a4-$tag" FAIL "$(grep -E '\*\*\* FAILED|error:|Tests run.*Fail' "$log" | head -3 | tr '\n' ' ')"
  fi
done

column -t -s $'\t' "$RESULTS"
