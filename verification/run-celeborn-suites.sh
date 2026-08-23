#!/usr/bin/env bash
# Celeborn's recovery suites. Celeborn does not compile on JDK 25 (`sun.misc.Signal` in
# common/util/SignalUtils.scala is not visible there), so this pins JDK 21 explicitly rather than
# inheriting whatever the shell has. One core, sequential.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CELEBORN="$HERE/../celeborn"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-celeborn.tsv"; : > "$RESULTS"

JAVA_HOME="${CELEBORN_JAVA_HOME:-$HOME/.sdkman/candidates/java/21.0.3-tem}"
if [[ ! -x "$JAVA_HOME/bin/javac" ]]; then
  echo "no JDK 21 at $JAVA_HOME; set CELEBORN_JAVA_HOME" >&2
  exit 1
fi
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

run_suite() { # run_suite <modules> <suite-fqcn> <tag>
  local modules="$1" suite="$2" tag="$3" log="$LOGS/celeborn-$3.log"
  ( cd "$CELEBORN" && nice -n 10 taskset -c 0 env MAVEN_OPTS="-Xss64m -Xmx2g" \
      ./build/mvn -T 1 -pl "$modules" -am -Dtest=none -DfailIfNoTests=false \
      -DwildcardSuites="$suite" -Dmaven.javadoc.skip=true test ) > "$log" 2>&1
  local rc=$?
  local summary
  summary=$(grep -E 'Tests: succeeded|Total number of tests run|TESTS FAILED' "$log" | tail -2 | tr '\n' ' ')
  if [[ $rc -eq 0 && -n "$summary" ]]; then record "celeborn:$tag" PASS "$summary"
  else record "celeborn:$tag" FAIL "rc=$rc ${summary:-see $log}"; fi
}

run_java_suite() { # run_java_suite <modules> <test-class> <tag>
  local modules="$1" test="$2" tag="$3" log="$LOGS/celeborn-$3.log"
  ( cd "$CELEBORN" && nice -n 10 taskset -c 0 env MAVEN_OPTS="-Xss64m -Xmx2g" \
      ./build/mvn -T 1 -pl "$modules" -am -DwildcardSuites=none -Dtest="$test" \
      -DfailIfNoTests=false -Dmaven.javadoc.skip=true test ) > "$log" 2>&1
  local rc=$?
  local summary
  summary=$(grep -E 'Tests run:' "$log" | tail -1)
  if [[ $rc -eq 0 ]]; then record "celeborn:$tag" PASS "${summary:-see $log}"
  else record "celeborn:$tag" FAIL "rc=$rc ${summary:-see $log}"; fi
}

echo "JDK: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
run_suite master org.apache.celeborn.service.deploy.master.clustermeta.ApplicationLeaseSuite lease
run_suite common org.apache.celeborn.common.protocol.ApplicationLeaseControlSuite protocol
run_suite worker org.apache.celeborn.service.deploy.worker.ApplicationLeaseStoreSuite leasestore
run_suite client org.apache.celeborn.client.LifecycleManagerRecoveryBindingSuite binding
run_suite master org.apache.celeborn.service.deploy.master.MasterSuite mastersuite
run_java_suite master MasterStateMachineSuiteJ statemachine

echo
column -t -s $'\t' "$RESULTS"
