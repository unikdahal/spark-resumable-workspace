#!/usr/bin/env bash
# Celeborn HA cluster lifecycle for the two-driver test. Sourced, not executed.
# Every JVM here is pinned to one core.

JAVA_HOME="${JAVA_HOME:-/home/unik/.sdkman/candidates/java/17.0.11-tem}"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin"

CELEBORN_DIR="${CELEBORN_DIR:-$HERE/../../celeborn}"
CELEBORN_PROFILE="${CELEBORN_PROFILE:-spark-4.2}"
CLUSTER_PIDS=()

wait_for_port() { # wait_for_port <port> <label> [tries]
  local port="$1" label="$2" tries="${3:-60}"
  while (( tries-- > 0 )); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then exec 3>&- ; return 0; fi
    sleep 1
  done
  echo "CLUSTER: $label never opened port $port" >&2
  return 1
}

build_celeborn_classpaths() {
  local out="$1"
  # -o keeps the build off the network: every artifact is already in the local repo, and a
  # metadata lookup that stalls (no socket timeout) once hung this step for 10+ minutes.
  ( cd "$CELEBORN_DIR" && nice -n 10 taskset -c 0 ./build/mvn -q -o -T 1 -P"$CELEBORN_PROFILE" \
      -pl master,worker,client-spark/spark-3 -am -DskipTests -Dmaven.javadoc.skip=true install ) \
    > "$out/celeborn-build.log" 2>&1 || return 1
  for module in master worker; do
    ( cd "$CELEBORN_DIR" && taskset -c 0 ./build/mvn -q -o -P"$CELEBORN_PROFILE" -pl "$module" \
        dependency:build-classpath -Dmdep.outputFile="$out/$module-cp.txt" ) >> "$out/celeborn-build.log" 2>&1
  done
}

start_masters() { # start_masters <workdir> <confdir> <logdir>
  local work="$1" conf="$2" logs="$3"
  local cp="$CELEBORN_DIR/master/target/classes:$(cat "$work/master-cp.txt")"
  local index=1
  for port in 9097 9098 9099; do
    CELEBORN_CONF_DIR="$conf" nice -n 10 taskset -c 0 java -Xmx1g \
      -Dceleborn.master.ha.node.id="$index" \
      --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.base/java.nio=ALL-UNNAMED \
      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
      -cp "$cp" org.apache.celeborn.service.deploy.master.Master \
      > "$logs/master-$index.log" 2>&1 &
    CLUSTER_PIDS+=($!)
    index=$((index + 1))
  done
  wait_for_port 9097 "celeborn master 1" || return 1
}

start_workers() { # start_workers <workdir> <confdir> <logdir> <count>
  local work="$1" conf="$2" logs="$3" count="${4:-2}"
  local cp="$CELEBORN_DIR/worker/target/classes:$(cat "$work/worker-cp.txt")"
  for index in $(seq 1 "$count"); do
    mkdir -p "$work/worker-$index"
    CELEBORN_CONF_DIR="$conf" nice -n 10 taskset -c 0 java -Xmx1g \
      -Dceleborn.worker.storage.dirs="$work/worker-$index" \
      --add-opens=java.base/java.lang=ALL-UNNAMED \
      --add-opens=java.base/java.nio=ALL-UNNAMED \
      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
      -cp "$cp" org.apache.celeborn.service.deploy.worker.Worker \
      > "$logs/worker-$index.log" 2>&1 &
    CLUSTER_PIDS+=($!)
  done
  sleep 5
}

stop_cluster() {
  for pid in "${CLUSTER_PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done
  CLUSTER_PIDS=()
}
