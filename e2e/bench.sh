#!/usr/bin/env bash
# Latency and throughput of the same queries over every connection path, on identical data:
#   plain          app -> cloud Postgres, ordinary table (no LokaDB): the baseline
#   cloud-direct   app -> cloud Postgres, LokaDB schema, organization 2 (cloud partition)
#   cloud-proxy    app -> proxy -> cloud Postgres, organization 2
#   lokadb-direct   app -> cloud Postgres -> postgres_fdw -> relay -> agent -> customer Postgres, organization 1
#   lokadb-proxy    app -> proxy -> (same as lokadb-direct): the full path
#   tunnel         cloud -> relay -> agent -> customer Postgres (no FDW, no proxy)
#   customer-local customer Postgres from its own network
# WAN_MS=20 also repeats the organization-1 paths with that much extra round trip
# between the agent and the relay (tc netem on the agent's internet-side interface).
set -uo pipefail
export PG_FSYNC=${PG_FSYNC:-on} # measurements need a durable Postgres; e2e and defects run with fsync off
export PG_DATA_TMPFS=${PG_DATA_TMPFS:-/run/lokadb-unused} # and its data on the disk, not in memory
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1
C="docker compose"
ROWS=${ROWS:-20000}
N=${N:-1000}
WAN_MS=${WAN_MS:-20}
OUT=bench_results
mkdir -p "$OUT"
CP="host=cloud dbname=controlplane user=postgres password=pw sslmode=disable"
APP="user=app password=app sslmode=disable"
CUST="user=postgres password=pw sslmode=disable dbname=cust_bench"

log() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
wait_tunnel() {
  for _ in $(seq 1 60); do
    $C exec -T cloud psql "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

log "fresh stand"
$C --profile apps --profile tools down -v >/dev/null 2>&1
rm -f secrets/token*
$C --profile tools build bench bench-cust 2>&1 | grep -E "ERROR" && exit 1
$C up -d --wait cloud customer proxy relay >/dev/null 2>&1 || exit 1
$C run --rm -T lokadb token -cp "$CP" -org 1 -ttl 10m >secrets/token 2>/dev/null || exit 1
$C up -d agent >/dev/null 2>&1 && wait_tunnel || { echo "tunnel did not come up"; exit 1; }

log "data: $ROWS documents per organization, in a plain database and in a LokaDB database"
$C exec -T cloud psql -U postgres -qc "CREATE DATABASE bench OWNER app" -c "CREATE DATABASE bench_plain OWNER app"
$C exec -T cloud psql -U postgres -d bench -qc "CREATE EXTENSION postgres_fdw"
$C exec -T customer psql -U postgres -qc "CREATE DATABASE cust_bench"
$C run --rm -T bench setup -dsn "host=cloud dbname=bench_plain $APP" -rows "$ROWS" 2>/dev/null
$C run --rm -T bench setup -dsn "host=cloud dbname=bench $APP" -rows "$ROWS" 2>/dev/null
$C run --rm -T lokadb enable -db "host=cloud user=postgres password=pw dbname=bench sslmode=disable" -spec /specs/bench.json -owner app 2>&1 | grep -E "error|done"
$C run --rm -T lokadb move-org -db "host=cloud user=postgres password=pw dbname=bench sslmode=disable" -spec /specs/bench.json \
  -owner app -org 1 -customer "host=relay port=15001 $CUST" -publisher "host=agent port=5433 dbname=bench user=postgres password=pw sslmode=disable" \
  -cp "$CP" \
  2>&1 | grep -E "error|switched|done" | sed 's/^time=[^ ]* level=[A-Z]* //'
$C exec -T cloud psql -U postgres -d bench -qc "VACUUM ANALYZE"
$C exec -T cloud psql -U postgres -d bench_plain -qc "VACUUM ANALYZE"
$C exec -T customer psql -U postgres -d cust_bench -qc "VACUUM ANALYZE"
[ -n "${SETUP_ONLY:-}" ] && exit 0

bench() { $C run --rm -T bench run -n "$N" "$@" 2>/dev/null | grep '^{'; }
org1_paths() {
  bench -path lokadb-direct -dsn "host=cloud dbname=bench $APP options='-c lokadb.org=1'" -org 1 "$@"
  bench -path lokadb-proxy -dsn "host=proxy port=6432 dbname=bench $APP" -org 1 "$@"
  bench -path tunnel -dsn "host=relay port=15001 $CUST" -org 1 -explicit-ids "$@"
}

log "LAN: every path (all containers on one machine)"
{
  bench -path plain -dsn "host=cloud dbname=bench_plain $APP" -org 1
  bench -path cloud-direct -dsn "host=cloud dbname=bench $APP options='-c lokadb.org=2'" -org 2
  bench -path cloud-proxy -dsn "host=proxy port=6432 dbname=bench $APP" -org 2
  org1_paths
  $C run --rm -T bench-cust run -n "$N" -path customer-local -dsn "host=customer $CUST" -org 1 -explicit-ids 2>/dev/null | grep '^{'
} >"$OUT/lan.jsonl"
$C run --rm -T -v "$(pwd -W 2>/dev/null || pwd)/$OUT:/out" bench report /out/lan.jsonl 2>/dev/null | tee "$OUT/lan.md"

if [ "$WAN_MS" != 0 ]; then
  log "WAN: +${WAN_MS} ms round trip between the agent and the relay"
  relay_ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "'"${COMPOSE_PROJECT_NAME:-lokadb-e2e}"'_internet").IPAddress}}' "${COMPOSE_PROJECT_NAME:-lokadb-e2e}-relay-1")
  netem() {
    docker run --rm --net "container:${COMPOSE_PROJECT_NAME:-lokadb-e2e}-agent-1" --cap-add NET_ADMIN nicolaka/netshoot:v0.13 sh -c \
      "dev=\$(ip route get $relay_ip | grep -o 'dev [^ ]*' | cut -d' ' -f2); tc qdisc $1 dev \$dev root ${2:-} && echo \"netem $1 on \$dev ${2:-}\""
  }
  netem add "netem delay ${WAN_MS}ms" || exit 1
  $C exec -T cloud psql "host=relay port=15001 $CUST" -c "\timing on" -c "SELECT 1" 2>&1 | grep Time | head -1
  {
    bench -path plain -dsn "host=cloud dbname=bench_plain $APP" -org 1
    N=$((N / 4)) org1_paths -dur 10s -batches 2
  } >"$OUT/wan.jsonl"
  netem del
  $C run --rm -T -v "$(pwd -W 2>/dev/null || pwd)/$OUT:/out" bench report /out/wan.jsonl 2>/dev/null | tee "$OUT/wan.md"
fi
