#!/usr/bin/env bash
# Queued last: E1 version probe — does Celeborn's Spark client build against the fork?
# Expected PASS now that the fork publishes every module incl. sql/hive to mavenLocal.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
exec 9>"$LOGS/.verification.lock"
flock 9
"$HERE/../dev-harnesses/celeborn-cluster-harness/probe-versions.sh"
echo "probe rc=$?" | tee "$LOGS/results-e1-probe.txt"
