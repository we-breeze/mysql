#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-registry.api.example.com/mysql/mysql:5.7.18}"
PORT="${PORT:-13306}"
NAME="brz-mysql-it-$$"

cleanup() {
  docker stop "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run -d --rm --name "$NAME" -p "$PORT:3306" \
  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=brz_mysql_test "$IMAGE" \
  --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci >/dev/null

ready=0
for _ in {1..120}; do
  ready_messages="$(docker logs "$NAME" 2>&1 | grep -c 'ready for connections' || true)"
  if (( ready_messages >= 2 )) \
    && docker exec "$NAME" mysqladmin ping -uroot -proot --silent >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
if (( ready != 1 )); then
  docker logs "$NAME" >&2
  exit 1
fi

cd "$ROOT/../.."
BREEZE_MYSQL_TEST_URL="mysql://root:root@127.0.0.1:$PORT/brz_mysql_test?ssl-mode=disabled" \
  cargo test -p brz-mysql --features integration-tests
