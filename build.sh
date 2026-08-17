#!/usr/bin/env bash
# Build caddy with the turso module embedded.
# Usage: ./build.sh [output]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${1:-$REPO_ROOT/caddy-turso}"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

export GOCACHE="${GOCACHE:-$TMPDIR/gocache}"

cd "$REPO_ROOT"
xcaddy build \
	--with github.com/mdrv/caddy-turso="$REPO_ROOT" \
	--output "$OUT"

echo "built $OUT"
