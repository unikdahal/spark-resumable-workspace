#!/usr/bin/env bash
# Re-run Celeborn and Iceberg after the toolchain and source fixes, from a clean module state so a
# stale generated-source tree cannot masquerade as a compile error.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CELEBORN="$HERE/../celeborn"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
export JAVA_HOME="${CELEBORN_JAVA_HOME:-$HOME/.sdkman/candidates/java/21.0.3-tem}"
export PATH="$JAVA_HOME/bin:$PATH"

while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done

( cd "$CELEBORN" && nice -n 10 taskset -c 0 ./build/mvn -T 1 -q -pl master clean ) \
  > "$LOGS/celeborn-clean.log" 2>&1
"$HERE/run-celeborn-suites.sh" > "$LOGS/celeborn-rerun.log" 2>&1
"$HERE/run-iceberg-ledger.sh" > "$LOGS/iceberg-rerun.log" 2>&1
cat "$LOGS/results-celeborn.tsv" "$LOGS/results-iceberg.tsv" 2>/dev/null
