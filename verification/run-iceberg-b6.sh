#!/usr/bin/env bash
# Post-C1 batch: unblock and verify the Iceberg lane.
#  1. Install the fork's sql/hive into mavenLocal (B6 blocker: iceberg v4.1 resolves spark-hive).
#  2. Rerun the truncated Celeborn MasterStateMachineSuiteJ for D1 evidence.
#  3. Iceberg: TestSnapshotIdempotency (B1), Hadoop+JDBC ledger round-trip legs (B4).
#  4. Iceberg v4.1 module against the fork snapshot: recovery tests incl. new B2/B3 drift tests.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SPARK="$HERE/../spark-resumable-upstream"
CELEBORN="$HERE/../celeborn"
ICEBERG="$HERE/../oss-fixes/iceberg"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
RESULTS="$LOGS/results-iceberg-b6.tsv"; : > "$RESULTS"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9
while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

echo "== 1. installing fork sql/hive for B6 =="
spark_mvn() { ( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
    ./build/mvn -T 1 "$@" ); }
if spark_mvn -pl sql/hive -DskipTests -Dmaven.source.skip -Dmaven.scaladoc.skip=true \
    -Dskip.scaladoc=true -Dscalastyle.skip=true -Dcheckstyle.skip=true \
    -Dcyclonedx.skip=true install > "$LOGS/spark-hive-install.log" 2>&1; then
  record "spark:sql-hive-install" PASS "5.0.0-SNAPSHOT published"
else
  record "spark:sql-hive-install" FAIL "$(grep -E '\[ERROR\]' "$LOGS/spark-hive-install.log" | head -3 | tr '\n' ' ')"
fi

echo "== 2. Celeborn MasterStateMachineSuiteJ (D1 gap) =="
celeborn_mvn() { ( cd "$CELEBORN" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS='-XX:ActiveProcessorCount=2' \
    ./build/mvn -T 1 -Dmaven.javadoc.skip=true "$@" ); }
if celeborn_mvn -pl master -am -DwildcardSuites=none \
    -Dtest=org.apache.celeborn.service.deploy.master.clustermeta.ha.MasterStateMachineSuiteJ \
    -DfailIfNoTests=false test > "$LOGS/celeborn-statemachine-rerun.log" 2>&1; then
  record "celeborn:statemachine" PASS "$(grep -E 'Tests run:' "$LOGS/celeborn-statemachine-rerun.log" | tail -1)"
else
  record "celeborn:statemachine" FAIL "see $LOGS/celeborn-statemachine-rerun.log"
fi

echo "== 3. Iceberg core tests =="
ice_gradle() { ( cd "$ICEBERG" && nice -n 5 taskset -c 0,1 env "JAVA_HOME=$JAVA_HOME" \
    ./gradlew --max-workers=2 --console=plain "$@" ); }
# Mergeability gate: format everything changed vs the ratchet base, fail loudly if it drifts.
if ! ice_gradle spotlessApply > "$LOGS/iceberg-spotless.log" 2>&1; then
  record "iceberg:spotless" FAIL "$(grep -E 'FAILED|error' "$LOGS/iceberg-spotless.log" | head -3 | tr '\n' ' ')"
fi
if ice_gradle spotlessCheck > /dev/null 2>&1; then
  record "iceberg:spotless" PASS "changed files formatted"
else
  record "iceberg:spotless" FAIL "see $LOGS/iceberg-spotless.log"
fi
if ice_gradle :iceberg-core:test --tests org.apache.iceberg.TestSnapshotIdempotency \
    > "$LOGS/iceberg-core-idempotency.log" 2>&1; then
  record "iceberg:B1-idempotency" PASS "$(grep -cE 'PASSED' "$LOGS/iceberg-core-idempotency.log") passed"
else
  record "iceberg:B1-idempotency" FAIL "$(grep -E 'FAILED|error:' "$LOGS/iceberg-core-idempotency.log" | head -3 | tr '\n' ' ')"
fi

if ice_gradle :iceberg-core:test --tests org.apache.iceberg.hadoop.TestHadoopCatalog \
    > "$LOGS/iceberg-hadoop-catalog.log" 2>&1; then
  record "iceberg:B4-hadoop" PASS ""
else
  record "iceberg:B4-hadoop" FAIL "$(grep -E 'FAILED' "$LOGS/iceberg-hadoop-catalog.log" | head -3 | tr '\n' ' ')"
fi

if ice_gradle :iceberg-core:test --tests org.apache.iceberg.jdbc.TestJdbcCatalog \
    > "$LOGS/iceberg-jdbc-catalog.log" 2>&1; then
  record "iceberg:B4-jdbc" PASS ""
else
  record "iceberg:B4-jdbc" FAIL "$(grep -E 'FAILED' "$LOGS/iceberg-jdbc-catalog.log" | head -3 | tr '\n' ' ')"
fi

echo "== 4. Iceberg v4.1 against the fork (B6 verdict + B2/B3) =="
if ice_gradle :iceberg-spark:iceberg-spark-4.1_2.13:test \
    --tests org.apache.iceberg.spark.source.TestSparkWriteRecovery \
    > "$LOGS/iceberg-v41-recovery.log" 2>&1; then
  record "iceberg:B6+B2+B3-v41" PASS "$(grep -cE 'PASSED' "$LOGS/iceberg-v41-recovery.log") passed"
else
  record "iceberg:B6+B2+B3-v41" FAIL "$(grep -E 'FAILED|error:' "$LOGS/iceberg-v41-recovery.log" | head -5 | tr '\n' ' ')"
fi

column -t -s $'\t' "$RESULTS"
