#!/usr/bin/env bash
# Celeborn recovery suites, under the constraints in RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md §2:
# JDK 17, CPUs 0-1 only, one build at a time, Maven single-threaded with two visible processors.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CELEBORN="$HERE/../celeborn"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-celeborn.tsv"; : > "$RESULTS"

export JAVA_HOME="${CELEBORN_JAVA_HOME:-/home/unik/.sdkman/candidates/java/17.0.11-tem}"
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"
[[ -x "$JAVA_HOME/bin/javac" ]] || { echo "no JDK 17 at $JAVA_HOME" >&2; exit 1; }

exec 9>"$LOGS/.verification.lock"
flock 9

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

mvn_test() { # mvn_test <log> <extra args...>
  local log="$1"; shift
  ( cd "$CELEBORN" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS='-XX:ActiveProcessorCount=2' \
      ./build/mvn -T 1 -Dmaven.javadoc.skip=true "$@" test ) > "$log" 2>&1
}

run_scala_suite() { # <modules> <suite-fqcn> <tag>
  local log="$LOGS/celeborn-$3.log"
  mvn_test "$log" -pl "$1" -am -Dtest=none -DfailIfNoTests=false -DwildcardSuites="$2"
  local rc=$?
  local summary
  summary=$(grep -E 'Total number of tests run|TESTS FAILED' "$log" | tail -2 | tr '\n' ' ')
  if [[ $rc -eq 0 && -n "$summary" ]]; then record "celeborn:$3" PASS "$summary"
  else record "celeborn:$3" FAIL "rc=$rc ${summary:-see $log}"; fi
}

run_java_suite() { # <modules> <test-class> <tag>
  local log="$LOGS/celeborn-$3.log"
  mvn_test "$log" -pl "$1" -am -DwildcardSuites=none -Dtest="$2" -DfailIfNoTests=false
  local rc=$?
  local summary
  summary=$(grep -E 'Tests run:' "$log" | tail -1)
  if [[ $rc -eq 0 ]]; then record "celeborn:$3" PASS "${summary:-see $log}"
  else record "celeborn:$3" FAIL "rc=$rc ${summary:-see $log}"; fi
}

echo "JDK: $(java -version 2>&1 | head -1)"

run_scala_suite common org.apache.celeborn.common.protocol.ApplicationLeaseControlSuite protocol
run_scala_suite master \
  org.apache.celeborn.service.deploy.master.clustermeta.ApplicationLeaseSuite lease
run_scala_suite master org.apache.celeborn.service.deploy.master.MasterSuite mastersuite
run_java_suite master MasterStateMachineSuiteJ statemachine
run_scala_suite worker org.apache.celeborn.service.deploy.worker.ApplicationLeaseStoreSuite leasestore
run_scala_suite worker org.apache.celeborn.service.deploy.worker.ControllerSuite controller
run_scala_suite client org.apache.celeborn.client.LifecycleManagerRecoveryBindingSuite binding
run_scala_suite common org.apache.celeborn.common.RecoveryBlobConfSuite blobconf
run_scala_suite common org.apache.celeborn.ConfigurationSuite configdocs
run_scala_suite master \
  org.apache.celeborn.service.deploy.master.clustermeta.RecoveryBlobPointerSuite blobpointer
run_scala_suite worker org.apache.celeborn.service.deploy.worker.RecoveryBlobStoreSuite blobstore
run_scala_suite client \
  org.apache.celeborn.client.recovery.RecoveryBlobReplicationSuite blobreplication
run_scala_suite client \
  org.apache.celeborn.client.recovery.RecoveryTaskCommitBackendSuite blobbackend
run_scala_suite worker \
  org.apache.celeborn.service.deploy.worker.RecoveryBlobCollectorSuite blobcollector
run_scala_suite master org.apache.celeborn.service.deploy.master.RecoveryBlobRepairSuite blobrepair
run_scala_suite master \
  org.apache.celeborn.service.deploy.master.clustermeta.RecoveryInlineCapacitySuite inlinecapacity

echo
column -t -s $'\t' "$RESULTS"
