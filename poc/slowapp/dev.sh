#!/usr/bin/env bash
set -euo pipefail

# Usage: ./dev.sh [sleep_seconds]
SLEEP="${1:-10}"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
BIN_DIR="$ROOT/bin"
BIN="$BIN_DIR/slowapp"

mkdir -p "$BIN_DIR"

# Clean previous state for a deterministic start
"$DIR/clean.sh" || true

echo "Building slowapp to $BIN ..."
GO111MODULE=on go build -o "$BIN" "$ROOT/poc/slowapp"

# Pre-create Comet WAL directory to avoid cs.wal error on first boot
mkdir -p "$DIR/.tm/data/cs.wal"

echo "Starting slowapp (in-proc Comet), EndBlock sleep=${SLEEP}s"
echo "Press Ctrl+C to stop."
exec "$BIN" -mode inproc -sleep "$SLEEP"

