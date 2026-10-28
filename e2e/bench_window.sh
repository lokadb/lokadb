#!/usr/bin/env bash
# Effect of the yamux stream window on reading one organization's rows through the tunnel.
# For each window size the relay and the agent are recreated with LOKADB_MUX_WINDOW_KB,
# then the organization's 21 100 rows (~10.6 MB) are read through:
#   tunnel       cloud -> relay -> agent -> customer Postgres
#   lokadb-proxy  app -> proxy -> cloud Postgres -> postgres_fdw -> relay -> agent -> customer Postgres
set -uo pipefail
export PG_FSYNC=${PG_FSYNC:-on} # measurements need a durable Postgres; e2e and defects run with fsync off
export PG_DATA_TMPFS=${PG_DATA_TMPFS:-/run/lokadb-unused} # and its data on the disk, not in memory
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1
C="docker compose"
OUT=bench_results/window.jsonl
APP="user=app password=app sslmode=disable"
CUST="user=postgres password=pw sslmode=disable dbname=cust_bench"

wait_tunnel() {
  for _ in $(seq 1 60); do
    $C exec -T cloud psql "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}
netem() {
  local relay_ip
  relay_ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "'"${COMPOSE_PROJECT_NAME:-lokadb-e2e}"'_internet").IPAddress}}' "${COMPOSE_PROJECT_NAME:-lokadb-e2e}-relay-1")
  docker run --rm --net "container:${COMPOSE_PROJECT_NAME:-lokadb-e2e}-agent-1" --cap-add NET_ADMIN nicolaka/netshoot:v0.13 sh -c \
    "dev=\$(ip route get $relay_ip | grep -o 'dev [^ ]*' | cut -d' ' -f2); tc qdisc $1 dev \$dev root ${2:-}"
}

[ -n "${SKIP_SETUP:-}" ] || SETUP_ONLY=1 bash bench.sh || exit 1
: >"$OUT"
for delay in 20 0; do
  windows="256 1024 4096 16384"
  [ "$delay" = 0 ] && windows="256 16384"
  for kb in $windows; do
    MUX_WINDOW_KB=$kb $C up -d relay agent >/dev/null 2>&1
    wait_tunnel || { echo "tunnel down"; exit 1; }
    echo "window $kb KiB (relay: $($C exec -T relay printenv LOKADB_MUX_WINDOW_KB), agent: $($C exec -T agent printenv LOKADB_MUX_WINDOW_KB)), delay $delay ms"
    [ "$delay" != 0 ] && netem add "netem delay ${delay}ms"
    for p in tunnel lokadb-proxy; do
      if [ $p = tunnel ]; then dsn="host=relay port=15001 $CUST"; else dsn="host=proxy port=6432 dbname=bench $APP"; fi
      $C run --rm -T -e WINDOW_KB="$kb" bench read -repeat 5 -path "$p, +${delay} ms" -dsn "$dsn" -org 1 2>/dev/null | grep '^{' | tee -a "$OUT"
    done
    [ "$delay" != 0 ] && netem del
  done
done
MUX_WINDOW_KB= $C up -d relay agent >/dev/null 2>&1 && wait_tunnel
