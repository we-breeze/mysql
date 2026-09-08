#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-mysql:5.7.44}"
PORT="${PORT:-13306}"
NAME="brz-mysql-it-$$"

cleanup() {
  docker stop "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker_args=(--rm --name "$NAME")
mysql_port=3306
mysqld_args=(--character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci)
if docker network inspect bridge >/dev/null 2>&1; then
  docker_args+=(-p "$PORT:3306")
else
  # Some local daemons intentionally have no default bridge network.
  docker_args+=(--network host)
  mysql_port="$PORT"
  mysqld_args+=(--port="$PORT")
fi

docker run -d "${docker_args[@]}" \
  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=brz_mysql_test "$IMAGE" \
  "${mysqld_args[@]}" >/dev/null

ready=0
for _ in {1..120}; do
  ready_messages="$(docker logs "$NAME" 2>&1 | grep -c 'ready for connections' || true)"
  if (( ready_messages >= 2 )) \
    && docker exec "$NAME" mysqladmin ping -h127.0.0.1 -P"$mysql_port" \
      -uroot -proot --silent >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
if (( ready != 1 )); then
  docker logs "$NAME" >&2
  exit 1
fi

cd "$ROOT"
BREEZE_MYSQL_TEST_URL="mysql://root:root@127.0.0.1:$PORT/brz_mysql_test?ssl-mode=disabled" \
  cargo test --features integration-tests
