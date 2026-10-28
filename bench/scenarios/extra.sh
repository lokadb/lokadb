#!/usr/bin/env bash
# Two controls the external review of the numbers asked for.
#   1. A connection per request without lokadb, through pgbouncer in front of the cloud (transaction and session mode):
#      what a pool alone gives, apart from lokadb (after.sh measures the proxy's own cache of cloud connections).
#   2. The open model: requests due at a set rate (benchload -rate), not the next after the answer. The cloud class
#      through the proxy at RATES (below, at and above its ceiling of the closed loop): p99 and the share of timeouts
#      show what overload does. The same table without lokadb at the highest rate for contrast.
#   bash bench/scenarios/extra.sh INVENTORY
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init extra "${1:?inventory}"
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
RATES=${RATES:-"15000 25000 35000"}
RATE_WORKERS=${RATE_WORKERS:-256}
RATE_TIMEOUT=${RATE_TIMEOUT:-1s}
PGB_IMAGE=${PGB_IMAGE:-edoburu/pgbouncer:v1.24.1-p1}
PGB_PORT=6433
K=${#ORGS[@]}
SAMPLE=1
note "organizations $TOTAL_ORGS x $ROWS documents; moved: ${ORGS[*]}; rates: $RATES with $RATE_WORKERS workers, timeout $RATE_TIMEOUT; pgbouncer $PGB_IMAGE"

mkdb bench_plain
setup_data bench_plain "$TOTAL_ORGS" "$ROWS"
cloud_sql bench_plain "VACUUM ANALYZE" >/dev/null

reconnect() { # reconnect CONFIG DSN: a connection per operation, 16 clients, the organizations all in the cloud
  run_load extra "config=$1" "class=cloud" "protocol=extended" "keys=cold" "clients=16" "op=read" "reconnect=1" -- \
    sql -dsn "'$2'" -op read -c 16 -warmup "$WARMUP" -d "$DURATION" -first-org 1 -orgs "$TOTAL_ORGS" -rows "$ROWS" -reconnect
}

# 1. pgbouncer on the cloud's host, in front of its Postgres
pgb_start() { # pgb_start MODE
  on cloud "mkdir -p $BENCH_HOME/pgbouncer && cat >$BENCH_HOME/pgbouncer/pgbouncer.ini" <<EOF
[databases]
* = host=127.0.0.1 port=$CLOUD_PORT
[pgbouncer]
listen_addr = 0.0.0.0
listen_port = $PGB_PORT
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt
pool_mode = $1
max_client_conn = 2000
default_pool_size = 32
max_prepared_statements = 200
ignore_startup_parameters = extra_float_digits
EOF
  on cloud "printf '\"app\" \"%s\"\n' '$APP_PASSWORD' >$BENCH_HOME/pgbouncer/userlist.txt; docker rm -f bb-pgbouncer >/dev/null 2>&1
    docker run -d --name bb-pgbouncer --network host -v $BENCH_HOME/pgbouncer:/etc/pgbouncer:ro $PGB_IMAGE >/dev/null"
  local i
  for i in $(seq 1 30); do on load "nc -z ${IP[cloud]} $PGB_PORT" 2>/dev/null && return 0; sleep 1; done
  die "pgbouncer does not listen: docker logs bb-pgbouncer on the cloud's host"
}
[ -n "${SKIP_PGB:-}" ] || reconnect plain "$(dsn_cloud bench_plain app)"
for mode in $([ -n "${SKIP_PGB:-}" ] || echo transaction session); do # SKIP_PGB=1: only the open model
  pgb_start $mode
  note "pgbouncer $mode: $(on cloud 'docker exec bb-pgbouncer pgbouncer --version 2>&1 | head -1')"
  reconnect "plain+pgbouncer-$mode" "host=${IP[cloud]} port=$PGB_PORT dbname=bench_plain user=app password=$APP_PASSWORD sslmode=disable"
done
on cloud "docker rm -f bb-pgbouncer >/dev/null 2>&1; rm -rf $BENCH_HOME/pgbouncer"

# 2. the open model: the cloud class through the proxy, the organizations of the inventory moved
mkdb bench
setup_data bench "$TOTAL_ORGS" "$ROWS"
put_spec docs "$SPEC_DOCS"
note "enable: $(enable_db bench docs | tr '\n' ' ' | cut -c1-120)"
MOVED=()
for o in "${ORGS[@]}"; do note "move-org $o: $(move_db bench docs "$o" | tail -1 | cut -c1-120)"; MOVED+=("$o"); done
proxy_config bench
cloud_sql bench "VACUUM ANALYZE" >/dev/null
verify_begin bench
rated() { # rated CONFIG DSN RATE
  run_load extra "config=$1" "class=cloud" "protocol=extended" "keys=cold" "rate=$3" "clients=$RATE_WORKERS" "op=mix" -- \
    sql -dsn "'$2'" -op read,update -c "$RATE_WORKERS" -rate "$3" -timeout "$RATE_TIMEOUT" -warmup "$WARMUP" -d "$DURATION" \
    -first-org $((K + 1)) -orgs $((TOTAL_ORGS - K)) -rows "$ROWS"
}
for r in $RATES; do rated fdw "$(dsn_proxy bench)" "$r"; done
top=${RATES##* }
rated plain "$(dsn_cloud bench_plain app)" "$top"
verify_end bench "extra (open model)"

dropdb bench; dropdb bench_plain
scenario_done
