#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
MYSQL_URL="${MYSQL_URL:?set MYSQL_URL to a dedicated local benchmark database}"
cd "$ROOT"
cargo run --release -p mysql-bench -- --url "$MYSQL_URL" "$@"
