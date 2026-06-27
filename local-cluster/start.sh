#!/usr/bin/env bash
# Start a 3-node Apache Ignite cluster on localhost from a local binary.
#
# Export IGNITE_HOME to point at your local Apache Ignite installation (the
# directory containing bin/ignite.sh), then run this script:
#
#   export IGNITE_HOME=/path/to/apache-ignite-x.y.z-bin
#   ./local-cluster/start.sh
#
# Thin-client ports: node-1 → 10800, node-2 → 10801, node-3 → 10802.
# Logs: /tmp/ignite-node{1,2,3}.log
set -euo pipefail

if [ -z "${IGNITE_HOME:-}" ]; then
  echo "ERROR: IGNITE_HOME is not set." >&2
  echo "  export IGNITE_HOME=/path/to/apache-ignite-x.y.z-bin" >&2
  exit 1
fi
if [ ! -x "$IGNITE_HOME/bin/ignite.sh" ]; then
  echo "ERROR: \$IGNITE_HOME/bin/ignite.sh not found or not executable: $IGNITE_HOME" >&2
  exit 1
fi

# Apache Ignite 2.x runs on Java 11; auto-select it if JAVA_HOME is unset.
JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 11 2>/dev/null || true)}"
export JAVA_HOME
CFG="$(cd "$(dirname "$0")" && pwd)/ignite-config.xml"

echo "IGNITE_HOME=$IGNITE_HOME"
echo "JAVA_HOME=$JAVA_HOME"
echo "config=$CFG"

wait_port() { # host port timeout_s
  local p=$1 t=$2 i=0
  while ! nc -z -w1 localhost "$p" 2>/dev/null; do
    i=$((i+1)); [ "$i" -ge "$t" ] && { echo "timeout waiting for port $p"; return 1; }
    sleep 1
  done
  echo "port $p is up"
}

# Per-node data-center id, used only in DC-demo mode.  Nodes 1 & 2 → DC1, node
# 3 → DC2, so for a client with dcId=DC1 every partition has a DC1 owner and the
# partitions whose primary is node 3 route reads to a DC1 backup.
dc_id_for() { # index → echoes the DC id (empty unless IGNITE_DC_DEMO is set)
  [ -n "${IGNITE_DC_DEMO:-}" ] || return 0
  case "$1" in
    1 | 2) echo "DC1" ;;
    *) echo "DC2" ;;
  esac
}

start_node() { # index thin_port
  local idx=$1
  local port=$2
  local log="/tmp/ignite-node${idx}.log"
  local dc
  dc="$(dc_id_for "$idx")"
  local dc_opt=""
  [ -n "$dc" ] && dc_opt="-DIGNITE_DATA_CENTER_ID=$dc"
  echo "starting node-$idx (thin $port${dc:+, dc=$dc}) → $log"
  # IGNITE_WORK_DIR per node avoids any shared-state surprises.
  # JVM options:
  #   - allow DML inside transactions so the thin-client tx test suite works;
  #   - pin the JVM timezone to UTC so SQL DATE values are not normalised to the
  #     host's local timezone (otherwise temporal WHERE-clause tests fail off-UTC);
  #   - IGNITE_DATA_CENTER_ID (DC-demo mode only) tags the node's data center.
  IGNITE_WORK_DIR="/tmp/ignite-work-${idx}" \
  JVM_OPTS="${JVM_OPTS:-} -DIGNITE_ALLOW_DML_INSIDE_TRANSACTION=true -Duser.timezone=UTC $dc_opt" \
    nohup "$IGNITE_HOME/bin/ignite.sh" "$CFG" >"$log" 2>&1 &
  echo $! >"/tmp/ignite-node${idx}.pid"
  wait_port "$port" 90
}

start_node 1 10800
start_node 2 10801
start_node 3 10802

echo "all three thin ports are listening; waiting for 3-server topology..."
for i in $(seq 1 30); do
  if grep -q "servers=3" /tmp/ignite-node1.log 2>/dev/null; then
    grep "Topology snapshot" /tmp/ignite-node1.log | tail -1
    echo "CLUSTER READY (servers=3)"
    exit 0
  fi
  sleep 2
done
echo "WARNING: did not observe servers=3 within timeout; check /tmp/ignite-node*.log"
exit 1
