#!/usr/bin/env bash
# Answers the question that blocks the whole cluster-level E2E: can Celeborn's Spark client be
# built against the forked Spark? Prints a verdict and leaves full logs behind. One core.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CELEBORN="$HERE/../celeborn"
SPARK="$HERE/../spark-resumable-upstream"
LOGS="${LOGS_DIR:-$HERE/logs}"
mkdir -p "$LOGS"

SPARK_VERSION="$(grep -m1 -A1 '<artifactId>spark-parent' "$SPARK/pom.xml" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT' | head -1)"
SPARK_VERSION="${SPARK_VERSION:-5.0.0-SNAPSHOT}"
echo "PROBE: forked Spark version = $SPARK_VERSION"

for profile in spark-4.2 spark-4.1; do
  echo "PROBE: trying -P$profile -Dspark.version=$SPARK_VERSION"
  ( cd "$CELEBORN" && nice -n 10 taskset -c 0 ./build/mvn -T 1 -q \
      -P"$profile" -Dspark.version="$SPARK_VERSION" \
      -pl client-spark/common,client-spark/spark-3 -am -DskipTests \
      -Dmaven.javadoc.skip=true compile ) > "$LOGS/probe-$profile.log" 2>&1
  if [[ $? -eq 0 ]]; then
    echo "PROBE: PASS  -P$profile builds Celeborn's Spark client against $SPARK_VERSION"
    echo "PROBE: the cluster E2E is unblocked; use this profile in lib/cluster.sh"
    exit 0
  fi
  echo "PROBE: FAIL  -P$profile (first errors below, full log $LOGS/probe-$profile.log)"
  grep -E '^\[ERROR\]' "$LOGS/probe-$profile.log" | head -5
done

cat <<'MSG'
PROBE: no profile builds the Celeborn Spark client against the forked Spark.
       Options, in the order PATCH-SPLIT-PLAN.md recommends:
         1. publish the fork under a version Celeborn can consume (4.2.0-recovery-SNAPSHOT)
         2. add a Celeborn profile for the fork's version
         3. develop the connector against a released Spark once the API lands
       The write-recovery harness (../spark-write-recovery-e2e) does not need any of this.
MSG
exit 1
