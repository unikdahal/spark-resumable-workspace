#!/usr/bin/env bash
# Iceberg's commit-idempotency ledger tests. `core/` has no Spark dependency, so these run without
# any decision about which Spark version the Iceberg Spark module targets. Gradle here needs a JDK
# Iceberg supports; JDK 25 is not one of them. One core.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ICEBERG="$HERE/../oss-fixes/iceberg"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-iceberg.tsv"; : > "$RESULTS"

JAVA_HOME="${ICEBERG_JAVA_HOME:-$HOME/.sdkman/candidates/java/21.0.3-tem}"
[[ -x "$JAVA_HOME/bin/javac" ]] || { echo "no JDK 21 at $JAVA_HOME" >&2; exit 1; }
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

echo "JDK: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
( cd "$ICEBERG" && nice -n 10 taskset -c 0 ./gradlew --max-workers=1 --no-daemon \
    :iceberg-core:test --tests 'org.apache.iceberg.TestSnapshotIdempotency' ) \
  > "$LOGS/iceberg-ledger.log" 2>&1
rc=$?
summary=$(grep -E 'tests completed|BUILD (SUCCESSFUL|FAILED)' "$LOGS/iceberg-ledger.log" | tail -2 | tr '\n' ' ')
if [[ $rc -eq 0 ]]; then printf 'iceberg:ledger\tPASS\t%s\n' "${summary:-BUILD SUCCESSFUL}" | tee -a "$RESULTS"
else printf 'iceberg:ledger\tFAIL\trc=%s %s\n' "$rc" "${summary:-see logs/iceberg-ledger.log}" | tee -a "$RESULTS"; fi
