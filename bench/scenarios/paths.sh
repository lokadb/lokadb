#!/usr/bin/env bash
# The same SQL operations over every path: plain Postgres, lokadb fdw, fdw+direct, direct.
#   bash bench/scenarios/paths.sh INVENTORY
# TOTAL_ORGS (20) organizations of ROWS (20000) documents; the organizations of the inventory (org1..)
# are moved into their own databases, the rest stay in the cloud. For each configuration and class of
# organizations ("moved", "cloud"): read, feed, insert, update at CLIENTS (1 32) clients, and the cost
# of a new connection. What is compared: research/bench/METHOD.md, scenario "paths".
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init "${PATHS_NAME:-paths}" "${1:?inventory}" # PATHS_NAME: a re-measure of some variants into its own results
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
CLIENTS_LIST=${CLIENTS_LIST:-"1 32"}
CONFIGS=${CONFIGS:-"plain fdw fdw+direct direct"}
OPS=${OPS:-"read feed insert update"}
K=${#ORGS[@]}
note "organizations $TOTAL_ORGS x $ROWS documents; moved: ${ORGS[*]}; configurations: $CONFIGS; clients: $CLIENTS_LIST"

# one configuration's runs: measure CONFIG CLASS DSN FIRST_ORG ORGS [benchload flags]
measure() {
  local config=$1 class=$2 dsn=$3 first=$4 n=$5; shift 5
  local c op shape=auto
  [[ " $* " == *" -coop "* ]] && shape=tx # direct needs the organization named in a transaction (lokadb.tenant)
  for c in $CLIENTS_LIST; do
    for op in $OPS; do # keys never seen before: the first look at a key costs the proxy a lookup (PF2)
      run_load paths-$config "config=$config" "class=$class" "clients=$c" "op=$op" "shape=$shape" "keys=cold" --         sql -dsn "'$dsn'" -op $op -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org "$first" -orgs "$n" -rows "$ROWS" "$@"
    done
    run_load paths-$config "config=$config" "class=$class" "clients=$c" "op=read" "shape=$shape" "keys=hot" --       sql -dsn "'$dsn'" -op read -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org "$first" -orgs "$n" -rows "$ROWS" -hot 50 "$@"
  done
  local ccoop=""; [ $shape = tx ] && ccoop="-coop"
  run_load paths-$config "config=$config" "class=$class" "op=connect" "shape=$shape" -- connect -dsn "'$dsn'" -org "$first" -n 300 $ccoop
  # the same operations in a transaction that names the organization, the shape direct is measured in: profiles
  # compare on equal terms (one client; the transaction costs the profiles different exchanges)
  if [ $shape = auto ] && [ "$class" = moved ] && [ "$config" != plain ]; then
    for op in $OPS; do
      run_load paths-$config "config=$config" "class=$class" "clients=1" "op=$op" "shape=tx" "keys=cold" --         sql -dsn "'$dsn'" -op $op -c 1 -warmup "$WARMUP" -d "$DURATION" -first-org "$first" -orgs "$n" -rows "$ROWS" -coop
    done
  fi
}

for cfg in $CONFIGS; do
  note "== $cfg"
  case $cfg in
    plain)
      mkdb bench_plain
      setup_data bench_plain "$TOTAL_ORGS" "$ROWS"
      measure plain moved "$(dsn_cloud bench_plain app)" 1 "$K"
      dropdb bench_plain
      ;;
    fdw | fdw+direct | direct)
      if [ -z "${BENCH_READY:-}" ]; then
        mkdb bench
        setup_data bench "$TOTAL_ORGS" "$ROWS"
        put_spec docs "$SPEC_DOCS"
        note "enable: $(enable_db bench docs | tr '\n' ' ')"
        MOVED=()
        for o in "${ORGS[@]}"; do
          note "move-org $o: $(move_db bench docs "$o" | tail -2 | tr '\n' ' ')"
          MOVED+=("$o")
        done
        for o in "${ORGS[@]}"; do direct_credentials bench "$o" >/dev/null; done
        DIRECT_CREDS=1 proxy_config bench
        BENCH_READY=1
        cloud_sql bench "VACUUM ANALYZE" >/dev/null
      fi
      for o in "${ORGS[@]}"; do note "org-profile $o $cfg: $(set_profile bench "$o" "$cfg" | tr '\n' ' ')"; done
      coop=""; [ "$cfg" = direct ] && coop="-coop"
      measure "$cfg" moved "$(dsn_proxy bench)" 1 "$K" $coop
      if [ "$cfg" = fdw ]; then # the organizations that stayed in the cloud: the partitioned table through the proxy
        measure "$cfg" cloud "$(dsn_proxy bench)" $((K + 1)) $((TOTAL_ORGS - K))
      fi
      ;;
  esac
done
dropdb bench 2>/dev/null
scenario_done
