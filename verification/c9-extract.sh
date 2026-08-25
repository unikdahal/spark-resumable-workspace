#!/usr/bin/env bash
# Slot real C9 benchmark numbers into RETENTION-AND-SIZING section 3.
set -uo pipefail
R=/home/unik/Coding/spark/spark-resumable-upstream/sql/core/benchmarks/RecoveryTaskCommitBenchmark-results.txt
[ -f "$R" ] || { echo "no results file"; exit 1; }
python3 - "$R" <<'PYEOF'
import re, sys
text = open(sys.argv[1]).read()
sections = re.findall(
    r"Task commit envelope, (\d+) byte payload \((\d+) bytes on the wire.*?\n(.*?)\n\n",
    text, re.S)
print("payload | wire | case | best(ms)")
for size, wire, body in sections:
    for line in body.splitlines():
        m = re.match(r"^(.*?)\s+(\d+)\s+(\d+(?:\.\d+)?)\s+\d", line)
        if m:
            print(f"{size} | {wire} | {m.group(1)} | {m.group(2)}")
PYEOF
