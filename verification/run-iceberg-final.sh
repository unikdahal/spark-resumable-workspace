#!/usr/bin/env bash
# Final queued leg: rerun the Iceberg core verification (B1 idempotency + B4 catalog matrix)
# after the DefaultLocale fix, plus the v4.1 recovery leg now that fork sql/hive is published.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
ICEBERG="$HERE/../oss-fixes/iceberg"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }
RESULTS="$LOGS/results-iceberg-final.tsv"; : > "$RESULTS"

ice_gradle() { ( cd "$ICEBERG" && nice -n 5 taskset -c 0,1 env "JAVA_HOME=$JAVA_HOME" \
    ./gradlew --max-workers=2 --console=plain "$@" ); }

log="$LOGS/iceberg-core-idempotency2.log"
if ice_gradle :iceberg-core:test --tests org.apache.iceberg.TestSnapshotIdempotency \
    > "$log" 2>&1; then
  record "iceberg:B1-idempotency" PASS ""
else
  record "iceberg:B1-idempotency" FAIL "$(grep -E 'FAILED|error:' "$log" | head -3 | tr '\n' ' ')"
fi

for entry in "org.apache.iceberg.hadoop.TestHadoopCatalog hadoop" \
             "org.apache.iceberg.jdbc.TestJdbcCatalog jdbc"; do
  cls=$(echo "$entry" | awk '{print $1}')
  tag=$(echo "$entry" | awk '{print $2}')
  log="$LOGS/iceberg-catalog-$tag.log"
  if ice_gradle :iceberg-core:test --tests "$cls" > "$log" 2>&1; then
    record "iceberg:B4-$tag" PASS ""
  else
    record "iceberg:B4-$tag" FAIL "$(grep -E 'FAILED' "$log" | head -3 | tr '\n' ' ')"
  fi
done

log="$LOGS/iceberg-v41-recovery3.log"
if ice_gradle :iceberg-spark:iceberg-spark-4.1_2.13:test \
    --tests org.apache.iceberg.spark.source.TestSparkWriteRecovery > "$log" 2>&1; then
  record "iceberg:B6+B2+B3-v41" PASS ""
else
  record "iceberg:B6+B2+B3-v41" FAIL "$(grep -E 'FAILED|error:' "$log" | head -5 | tr '\n' ' ')"
fi

column -t -s $'\t' "$RESULTS"
