#!/usr/bin/env bash
# The proxy's own cost on a LAN, apart from routing (tasks/review/perf.md PF2-PF5, PF7, PF9): every organization stays in
# the cloud, nothing goes through a tunnel. What is compared, with the same statements:
#   plain          Postgres without lokadb
#   lokadb-direct   the lokadb schema, the application connected straight to the cloud and setting lokadb.org itself
#                  (its set_config is an exchange of its own on every operation: the proxy sends it with the statement,
#                  so this baseline is slower than a real application would be, performance review 3, P3-20)
#   proxy          the proxy as it is configured by default
#   proxy-<variant>  the same with the settings in PROXY_VARIANTS ("name=json fragment;..."), e.g. no_route_log, meta_cache, pools
# Statements: pgx prepared (extended protocol) and literals in the text (-simple: Laravel, Django, pgbench -M simple);
# keys never seen before and a few hot keys (-hot 50: PF2 shows in the difference); a connection per operation
# (-reconnect: PHP without persistent connections).
#   bash bench/scenarios/overhead.sh INVENTORY
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init overhead "${1:?inventory}"
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
CLIENTS_LIST=${CLIENTS_LIST:-"1 32"}
# The variants of the proxy's configuration (no_route_log is on in every one but `routelog`, which measures the log;
# the metadata cache is on by default, `meta_off` is the way to measure it off, `verify` lets a new connection trust
# a recent check, `pools` adds the connection pools of cloud and direct path): name=json fragment;...
PROXY_VARIANTS=${PROXY_VARIANTS:-'routelog=;base=;meta_off="meta_cache": {"off": true};verify="meta_cache": {"verify": "1s"};pools="upstream_pool": {}, "direct_pool": {}, "meta_cache": {"verify": "1s"}'}
DURATION=${OVERHEAD_DURATION:-45s}
REPEATS=${OVERHEAD_REPEATS:-2}
SAMPLE=1

mkdb bench_plain
setup_data bench_plain "$TOTAL_ORGS" "$ROWS"
mkdb bench
setup_data bench "$TOTAL_ORGS" "$ROWS"
put_spec docs "$SPEC_DOCS"
note "enable: $(enable_db bench docs | tr '\n' ' ' | cut -c1-120)"
cloud_sql bench "VACUUM ANALYZE" >/dev/null
MOVED=()

# one set of runs: sweep CONFIG DSN [benchload flags]
sweep() {
  local cfg=$1 dsn=$2; shift 2
  local proto flag c op shape
  for proto in extended simple; do
    flag=""; [ $proto = simple ] && flag="-simple"
    for c in $CLIENTS_LIST; do
      for op in read update; do # read: keys never seen before (insert is the disk of the host, not the proxy: left to paths)
        run_load overhead "config=$cfg" "protocol=$proto" "keys=cold" "clients=$c" "op=$op" -- \
          sql -dsn "'$dsn'" -op $op -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org 1 -orgs "$TOTAL_ORGS" -rows "$ROWS" $flag "$@"
      done
      run_load overhead "config=$cfg" "protocol=$proto" "keys=hot" "clients=$c" "op=read" -- \
        sql -dsn "'$dsn'" -op read -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org 1 -orgs "$TOTAL_ORGS" -rows "$ROWS" -hot 50 $flag "$@"
    done
  done
  # a connection for every operation
  run_load overhead "config=$cfg" "protocol=extended" "keys=cold" "clients=16" "op=read" "reconnect=1" -- \
    sql -dsn "'$dsn'" -op read -c 16 -warmup "$WARMUP" -d "$DURATION" -first-org 1 -orgs "$TOTAL_ORGS" -rows "$ROWS" -reconnect "$@"
}

sweep plain "$(dsn_cloud bench_plain app)"
sweep lokadb-direct "$(dsn_cloud bench app)" -set-org
IFS=';' read -ra variants <<<"$PROXY_VARIANTS"
for v in "${variants[@]}"; do
  name=${v%%=*}; PROXY_EXTRA=${v#*=}
  note "== proxy variant $name: ${PROXY_EXTRA:-(defaults)}"
  ROUTE_LOG=$([ "$name" = routelog ] && echo 1) PROXY_EXTRA=$PROXY_EXTRA proxy_config bench
  sweep "proxy-$name" "$(dsn_proxy bench)"
done
dropdb bench; dropdb bench_plain
scenario_done
