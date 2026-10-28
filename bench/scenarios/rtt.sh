#!/usr/bin/env bash
# What distance costs (BACKLOG block 14, items 4 and 9): the same operations against every organization of the
# inventory one at a time, each at its own distance, so each has its own real RTT to the cloud. Profiles fdw,
# fdw+direct, direct; the tag rtt_ms is the ping from the cloud to the organization's host at the start.
#   bash bench/scenarios/rtt.sh INVENTORY
# The organizations of the inventory should lie at different distances (about 0, 10, 25, 45 ms).
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init rtt "${1:?inventory}"
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
CLIENTS_LIST=${CLIENTS_LIST:-"1 16"}
OPS=${OPS:-"read feed update"}
DURATION=${RTT_DURATION:-30s} # many cells (organizations x profiles x clients x operations): shorter, two repeats
REPEATS=${RTT_REPEATS:-2}
CONFIGS=${CONFIGS:-"fdw fdw+direct direct"}

mkdb bench
setup_data bench "$TOTAL_ORGS" "$ROWS"
put_spec docs "$SPEC_DOCS"
note "enable: $(enable_db bench docs | tr '\n' ' ' | cut -c1-120)"
MOVED=()
for o in "${ORGS[@]}"; do note "move-org $o: $(move_db bench docs "$o" | tail -1 | cut -c1-120)"; MOVED+=("$o"); done
for o in "${ORGS[@]}"; do direct_credentials bench "$o" >/dev/null; done
DIRECT_CREDS=1 proxy_config bench
cloud_sql bench "VACUUM ANALYZE" >/dev/null

declare -A RTTMS
for o in "${ORGS[@]}"; do RTTMS[$o]=$(rtt cloud "org$o" 20 | cut -d/ -f2); note "org $o: RTT from the cloud ${RTTMS[$o]} ms"; done

for cfg in $CONFIGS; do
  for o in "${ORGS[@]}"; do note "org-profile $o $cfg: $(set_profile bench "$o" "$cfg" | tr '
' ' ' | cut -c1-100)"; done
  # direct reads must name the organization in a transaction (lokadb.tenant); the other profiles are measured the same way
  # (shape=tx) so the curves compare, and in autocommit (shape=auto) besides, at one client
  for shape in tx auto; do
    [ "$cfg" = direct ] && [ $shape = auto ] && continue
    coop="-coop"; [ $shape = auto ] && coop=""
    for o in "${ORGS[@]}"; do
      for c in $CLIENTS_LIST; do
        [ $shape = auto ] && [ "$c" != 1 ] && continue
        for op in $OPS; do
          run_load rtt "config=$cfg" "org=$o" "rtt_ms=${RTTMS[$o]}" "clients=$c" "op=$op" "shape=$shape" "keys=cold" --             sql -dsn "'$(dsn_proxy bench)'" -op $op -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org "$o" -orgs 1 -rows "$ROWS" $coop
        done
        [ $shape = tx ] && run_load rtt "config=$cfg" "org=$o" "rtt_ms=${RTTMS[$o]}" "clients=$c" "op=read" "shape=tx" "keys=hot" --           sql -dsn "'$(dsn_proxy bench)'" -op read -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org "$o" -orgs 1 -rows "$ROWS" -hot 50 $coop
      done
    done
  done
done
dropdb bench
scenario_done
