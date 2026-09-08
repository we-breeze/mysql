#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
IMAGE="${IMAGE:-mysql:5.7.44}"
PORT="${PORT:-13307}"
NAME="brz-mysql-bench-$$"

cleanup() {
  docker stop "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run -d --rm --name "$NAME" -p "$PORT:3306" \
  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=brz_mysql_bench "$IMAGE" >/dev/null
for _ in {1..120}; do
  ready_messages="$(docker logs "$NAME" 2>&1 | grep -c 'ready for connections' || true)"
  if (( ready_messages >= 2 )) \
    && docker exec "$NAME" mysqladmin ping -uroot -proot --silent >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
docker exec "$NAME" mysqladmin ping -uroot -proot --silent >/dev/null

cd "$ROOT"
cargo run --release -p mysql-bench -- \
  --url "mysql://root:root@127.0.0.1:$PORT/brz_mysql_bench?ssl-mode=disabled" "$@"
