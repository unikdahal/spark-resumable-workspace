#!/usr/bin/env bash
# C1: format, build, then run the focused Celeborn recovery suites. Roadmap §2 constraints.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CELEBORN="$HERE/../celeborn"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
export JAVA_HOME=/home/unik/.sdkman/candidates/java/17.0.11-tem
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

exec 9>"$LOGS/.verification.lock"
flock 9
while pgrep -a java 2>/dev/null | grep -q 'classworlds\|GradleDaemon'; do sleep 20; done

mvn() { ( cd "$CELEBORN" && nice -n 5 taskset -c 0,1 env MAVEN_OPTS='-XX:ActiveProcessorCount=2' \
  ./build/mvn -T 1 -Dmaven.javadoc.skip=true "$@" ); }

mvn -pl common,master,worker,client -am spotless:apply > "$LOGS/c1-spotless2.log" 2>&1
mvn -pl common,master,worker,client -am -DskipTests install > "$LOGS/c1-compile5.log" 2>&1
if ! grep -q 'BUILD SUCCESS' "$LOGS/c1-compile5.log"; then
  echo "COMPILE FAILED"; grep -E 'error:|ERROR\] Failed' "$LOGS/c1-compile5.log" | head -5; exit 1
fi
echo "COMPILE OK"
# The suite runner takes the same lock; release it first so the child is not blocked by us.
flock -u 9
"$HERE/run-celeborn-suites.sh"
