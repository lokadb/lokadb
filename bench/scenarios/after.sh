#!/usr/bin/env bash
# The short "after" set: only what the fixes since the first pass touch, on the code the stand runs now (meta.json says which).
#   1. the proxy's CPU on the cloud class (the keyCond cache): read, update, insert, feed at 1 and 32 clients, extended protocol
#   2. the feed: the proxy against the same table without lokadb (both with the index (organization_id, id): the baseline of a feed)
#   3. a connection per request (upstream_pool by default): the moved and the cloud organizations through the proxy, 16 clients
#   4. what the first pass left out: the connection-per-request baselines of `overhead` (plain, lokadb-direct) and the direct
#      profile's first query (the organization named in the transaction)
#   bash bench/scenarios/after.sh INVENTORY
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init after "${1:?inventory}"
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
CLIENTS_LIST=${CLIENTS_LIST:-"1 32"}
DURATION=${AFTER_DURATION:-60s}
REPEATS=${AFTER_REPEATS:-2}
K=${#ORGS[@]}
SAMPLE=1
note "organizations $TOTAL_ORGS x $ROWS documents; moved: ${ORGS[*]}; clients: $CLIENTS_LIST; $DURATION x $REPEATS"

mkdb bench_plain
setup_data bench_plain "$TOTAL_ORGS" "$ROWS"
mkdb bench
setup_data bench "$TOTAL_ORGS" "$ROWS"
put_spec docs "$SPEC_DOCS"
note "enable: $(enable_db bench docs | tr '\n' ' ' | cut -c1-120)"
cloud_sql bench "VACUUM ANALYZE" >/dev/null
cloud_sql bench_plain "VACUUM ANALYZE" >/dev/null
MOVED=()

reconnect() { # reconnect CONFIG CLASS DSN FIRST N [flags]: a connection per operation
  local cfg=$1 class=$2 dsn=$3 first=$4 n=$5; shift 5
  run_load after "config=$cfg" "class=$class" "protocol=extended" "keys=cold" "clients=16" "op=read" "reconnect=1" -- \
    sql -dsn "'$dsn'" -op read -c 16 -warmup "$WARMUP" -d "$DURATION" -first-org "$first" -orgs "$n" -rows "$ROWS" -reconnect "$@"
}

# 4a. the baselines of overhead's connection-per-request run, all organizations still in the cloud (the same rows as the first pass)
reconnect plain cloud "$(dsn_cloud bench_plain app)" 1 "$TOTAL_ORGS"
reconnect lokadb-direct cloud "$(dsn_cloud bench app)" 1 "$TOTAL_ORGS" -set-org
NO_POOL=1 proxy_config bench # as the first pass had it: no cache of cloud connections
reconnect proxy cloud "$(dsn_proxy bench)" 1 "$TOTAL_ORGS"

# the organizations move out
for o in "${ORGS[@]}"; do
  note "move-org $o: $(move_db bench docs "$o" | tail -2 | tr '\n' ' ')"
  MOVED+=("$o")
done
for o in "${ORGS[@]}"; do direct_credentials bench "$o" >/dev/null; done
NO_POOL=1 DIRECT_CREDS=1 proxy_config bench # 1 and 2 compare with the first pass: no cache of cloud connections
cloud_sql bench "VACUUM ANALYZE" >/dev/null
for o in "${ORGS[@]}"; do note "org-profile $o fdw: $(set_profile bench "$o" fdw | tr '\n' ' ')"; done

# 1 and 2. the cloud class through the proxy; the feed's baseline is the same table without lokadb
for c in $CLIENTS_LIST; do
  for op in read update insert feed; do
    run_load after "config=fdw" "class=cloud" "protocol=extended" "keys=cold" "clients=$c" "op=$op" -- \
      sql -dsn "'$(dsn_proxy bench)'" -op $op -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org $((K + 1)) -orgs $((TOTAL_ORGS - K)) -rows "$ROWS"
  done
  run_load after "config=plain" "class=cloud" "protocol=extended" "keys=cold" "clients=$c" "op=feed" -- \
    sql -dsn "'$(dsn_cloud bench_plain app)'" -op feed -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org $((K + 1)) -orgs $((TOTAL_ORGS - K)) -rows "$ROWS"
done

# 3. a connection per request through the proxy: the moved organizations and the cloud ones, without the cache of cloud
# connections (the first pass) and with it (the default since f448365)
reconnect fdw moved "$(dsn_proxy bench)" 1 "$K"
reconnect fdw cloud "$(dsn_proxy bench)" $((K + 1)) $((TOTAL_ORGS - K))
DIRECT_CREDS=1 proxy_config bench
reconnect fdw+upstream_pool moved "$(dsn_proxy bench)" 1 "$K"
reconnect fdw+upstream_pool cloud "$(dsn_proxy bench)" $((K + 1)) $((TOTAL_ORGS - K))

# 4b. the direct profile's first query: the organization is named in the transaction
for o in "${ORGS[@]}"; do note "org-profile $o direct: $(set_profile bench "$o" direct | tr '\n' ' ')"; done
run_load after "config=direct" "class=moved" "shape=tx" "op=connect" -- connect -dsn "'$(dsn_proxy bench)'" -org 1 -n 300 -coop

dropdb bench; dropdb bench_plain
scenario_done
