#!/usr/bin/env bash
# Many clients at once (BACKLOG block 14, item 2): CLIENTS_LIST connections (1 10 50 100 200 500 1000) that read and
# update, straight to the cloud Postgres ("plain") and through the proxy ("fdw": the organizations of the inventory
# moved, the rest in the cloud). What is watched: throughput and latency, the errors that appear at the limits
# (the proxy's max_connections, the cloud's, the organization's and the agent's, raised on this stand to ORG_MAX so that
# what is left is lokadb's own: the relay's max_cloud_streams per agent session (64) and max_org_conns (1000); every cloud backend that touches a moved organization
# holds its own postgres_fdw connection to that organization's database), the proxy's memory and CPU.
#   bash bench/scenarios/clients.sh INVENTORY
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init clients "${1:?inventory}"
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
CLIENTS_LIST=${CLIENTS_LIST:-"1 10 50 100 200 500 1000"}
K=${#ORGS[@]}
SAMPLE=1
note "max_connections: cloud $(cloud_sql postgres 'SHOW max_connections'), organizations $(org_sql "${ORGS[0]}" postgres 'SHOW max_connections'), proxy ${PROXY_MAX:-5000}"

# the baseline: the same tables in a database without lokadb (plain Postgres), straight from the clients
mkdb bench_plain
setup_data bench_plain "$TOTAL_ORGS" "$ROWS"
mkdb bench
setup_data bench "$TOTAL_ORGS" "$ROWS"
put_spec docs "$SPEC_DOCS"
note "enable: $(enable_db bench docs | tr '\n' ' ' | cut -c1-160)"
MOVED=()
for o in "${ORGS[@]}"; do note "move-org $o: $(move_db bench docs "$o" | tail -1 | cut -c1-120)"; MOVED+=("$o"); done
proxy_config bench
cloud_sql bench "VACUUM ANALYZE" >/dev/null

HOT_CLIENTS=${HOT_CLIENTS:-"1 50 200"} # client counts that also run with a few hot keys (the keys the proxy has seen)
for c in $CLIENTS_LIST; do
  for cls in moved cloud; do # the same ranges of organizations in every configuration
    if [ $cls = moved ]; then first=1; n=$K; else first=$((K + 1)); n=$((TOTAL_ORGS - K)); fi
    for keys in cold hot; do
      hot=""; if [ $keys = hot ]; then [[ " $HOT_CLIENTS " == *" $c "* ]] || continue; hot="-hot 50"; fi
      run_load clients "config=plain" "class=$cls" "clients=$c" "op=mix" "keys=$keys" --         sql -dsn "'$(dsn_cloud bench_plain app)'" -op read,update -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org $first -orgs $n -rows "$ROWS" $hot
      run_load clients "config=fdw" "class=$cls" "clients=$c" "op=mix" "keys=$keys" --         sql -dsn "'$(dsn_proxy bench)'" -op read,update -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org $first -orgs $n -rows "$ROWS" $hot
    done
  done
done
dropdb bench; dropdb bench_plain
scenario_done
