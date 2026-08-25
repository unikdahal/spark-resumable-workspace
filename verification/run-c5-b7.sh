#!/usr/bin/env bash
# Reinstall fork modules carrying the new SupportsRecoveryAnchor hook, then rerun the
# Iceberg recovery suite so B7 wiring compiles against fresh jars. Lock-serialized.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HERE/logs"
SPARK="$HERE/../spark-resumable-upstream"
ICEBERG="$HERE/../oss-fixes/iceberg"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9

log="$LOGS/spark-c5-install.log"
if ( cd "$SPARK" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS="-Xss64m -Xmx3g" \
      ./build/mvn -T 1 -pl core,sql/api,sql/catalyst,sql/core -am -DskipTests \
      -Dmaven.source.skip -Dmaven.scaladoc.skip=true -Dskip.scaladoc=true \
      -Dscalastyle.skip=true -Dcheckstyle.skip=true -Dcyclonedx.skip=true install ) > "$log" 2>&1; then
  printf 'spark:c5-install\tPASS\n' | tee -a "$LOGS/results-c5.tsv"
else
  printf 'spark:c5-install\tFAIL\t%s\n' "$(grep -E '\[ERROR\]' "$log" | head -3 | tr '\n' ' ')" | tee -a "$LOGS/results-c5.tsv"
  exit 1
fi

( cd "$ICEBERG" && nice -n 5 taskset -c 0,1 env "JAVA_HOME=$JAVA_HOME" \
    ./gradlew --max-workers=2 --console=plain \
    :iceberg-spark:iceberg-spark-4.1_2.13:recoveryTest \
    --tests '*TestSparkWriteRecovery*' ) > "$LOGS/iceberg-v41-recovery6.log" 2>&1
rc=$?
python3 - "$rc" <<'EOF'
import sys, glob, xml.etree.ElementTree as ET
rows=[]
for x in glob.glob('oss-fixes/iceberg/spark/v4.1/spark/build/test-results/recoveryTest/TEST-*.xml'):
    t=ET.parse(x).getroot()
    rows.append(f"iceberg:b7-pin-suite\t{'PASS' if int(sys.argv[1])==0 else 'FAIL'}\t{t.get('tests')} tests, {t.get('failures')} failures, {t.get('errors')} errors")
open('verification/logs/results-b7.tsv','a').write("\n".join(rows)+"\n")
EOF
column -t -s $'\t' "$LOGS/results-b7.tsv" 2>/dev/null | tail -3
