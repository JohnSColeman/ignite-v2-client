#!/usr/bin/env bash
# Stop the localhost Ignite nodes started by start.sh.
set -uo pipefail

stopped=0
for idx in 1 2 3; do
  pidfile="/tmp/ignite-node${idx}.pid"
  if [ -f "$pidfile" ]; then
    pid="$(cat "$pidfile")"
    if kill "$pid" 2>/dev/null; then
      echo "stopped node-$idx (pid $pid)"
      stopped=$((stopped+1))
    fi
    rm -f "$pidfile"
  fi
done

# Fallback: kill any stragglers by main class.
pkill -f "org.apache.ignite.startup.cmdline.CommandLineStartup" 2>/dev/null && \
  echo "killed remaining Ignite processes" || true

echo "stopped $stopped tracked node(s)"
