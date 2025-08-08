#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

kill_on_port() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    local pids
    pids=$(lsof -ti tcp:"$port" || true)
    if [ -n "$pids" ]; then
      echo "Killing processes on port $port: $pids"
      # shellcheck disable=SC2086
      kill -9 $pids || true
    fi
  fi
}

echo "Stopping any processes bound to Comet ports (26657 RPC, 26656 P2P, 26658 ABCI)..."
kill_on_port 26657
kill_on_port 26656
kill_on_port 26658

echo "Removing temporary test data under $DIR/.tm ..."
rm -rf "$DIR/.tm"
echo "Clean complete."

