#!/usr/bin/env bash
# Iceberg recovery tests that do not depend on the Spark API, under the constraints in
# RESUMABLE-SPARK-HANDOFF-AND-ROADMAP.md §2: JDK 17, CPUs 0-1, Gradle limited to two workers.
#
# `core/` has no Spark dependency, so the commit-idempotency ledger is testable now. The Spark 4.1
# module (codec, SparkTable, SparkWrite recovery) waits for the frozen Spark recovery API jar from
# work package X1.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ICEBERG="$HERE/../oss-fixes/iceberg"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-iceberg.tsv"; : > "$RESULTS"

export JAVA_HOME="${ICEBERG_JAVA_HOME:-/home/unik/.sdkman/candidates/java/17.0.11-tem}"
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"
[[ -x "$JAVA_HOME/bin/javac" ]] || { echo "no JDK 17 at $JAVA_HOME" >&2; exit 1; }

exec 9>"$LOGS/.verification.lock"
flock 9

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

run_gradle() { # <tag> <log> <gradle args...>
  local tag="$1" log="$2"; shift 2
  ( cd "$ICEBERG" && nice -n 5 taskset -c 0,1 env GRADLE_OPTS='-XX:ActiveProcessorCount=2' \
      ./gradlew --max-workers=2 --no-daemon "$@" ) > "$log" 2>&1
  local rc=$?
  local summary
  summary=$(grep -E 'BUILD (SUCCESSFUL|FAILED)|tests completed' "$log" | tail -2 | tr '\n' ' ')
  if [[ $rc -eq 0 ]]; then record "iceberg:$tag" PASS "${summary:-BUILD SUCCESSFUL}"
  else record "iceberg:$tag" FAIL "rc=$rc ${summary:-see $log}"; fi
}

echo "JDK: $(java -version 2>&1 | head -1)"

run_gradle ledger "$LOGS/iceberg-ledger.log" \
  :iceberg-core:test --tests 'org.apache.iceberg.TestSnapshotIdempotency'

# Snapshot-producer behaviour the ledger rides on; a regression here would show up as a ledger
# failure with a misleading cause.
run_gradle snapshot-producer "$LOGS/iceberg-snapshot-producer.log" \
  :iceberg-core:test --tests 'org.apache.iceberg.TestSnapshotSummary' \
  --tests 'org.apache.iceberg.TestTableMetadata'

echo
column -t -s $'\t' "$RESULTS"
