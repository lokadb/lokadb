#!/usr/bin/env bash
# COPY, cursors, LISTEN/NOTIFY (BACKLOG block 14, item 7) with real volumes: a moved organization of ROWS (300000)
# documents against the same organization in the cloud and plain Postgres. What is read is whatever the
# connection does: errors are results too (the defects LD1 in e2e/defects says what is expected).
#   bash bench/scenarios/misc.sh INVENTORY
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init misc "${1:?inventory}"
ROWS=${ROWS:-300000}
FETCH=${FETCH:-"100 1000 10000"}
put_spec docs "$SPEC_DOCS"
K=${#ORGS[@]}
REPEATS=${REPEATS:-3}

# the three paths: plain Postgres, lokadb with the organization in the cloud (org 2 of a partitioned table), lokadb moved (org 1)
mkdb bench_plain
setup_data bench_plain 2 "$ROWS"
mkdb bench
setup_data bench 2 "$ROWS"
note "enable: $(enable_db bench docs | tail -1 | cut -c1-120)"
MOVED=()
note "move-org ${ORGS[0]}: $(move_db bench docs "${ORGS[0]}" | tail -1 | cut -c1-120)"
MOVED=("${ORGS[0]}")
proxy_config bench
cloud_sql bench "VACUUM ANALYZE" >/dev/null
verify_begin bench

# path ID DSN ORG: every kind of statement
path() {
  local id=$1 dsn=$2 org=$3 f
  run_load misc "config=$id" "op=copy" -- copy -dsn "'$dsn'" -org "$org" -in-rows 10000
  for f in $FETCH; do run_load misc "config=$id" "fetch=$f" "op=cursor" -- cursor -dsn "'$dsn'" -org "$org" -fetch "$f" -n 3; done
  run_load misc "config=$id" "op=notify" -- notify -dsn "'$dsn'" -n 200
}
path plain "$(dsn_cloud bench_plain app)" 1
path fdw-cloud-org "$(dsn_proxy bench)" 2
path fdw-moved-org "$(dsn_proxy bench)" 1

# a big organization, the pages an application serves from it: a feed (ORDER BY id DESC LIMIT 20), a document, an update.
# Through the parent table the feed of a moved organization may pull the whole organization (it grows with ROWS).
bigorg() { # bigorg CONFIG DSN FIRSTORG
  local op c
  for c in 1 8; do
    for op in read feed update; do
      run_load misc "config=$1" "rows=$ROWS" "clients=$c" "op=$op" --         sql -dsn "'$2'" -op $op -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org "$3" -orgs 1 -rows "$ROWS"
    done
  done
}
bigorg plain "$(dsn_cloud bench_plain app)" 1
bigorg fdw-cloud-org "$(dsn_proxy bench)" 2
bigorg fdw-moved-org "$(dsn_proxy bench)" 1
note "org-profile fdw+direct: $(set_profile bench "${ORGS[0]}" fdw+direct | tr '\n' ' ' | cut -c1-120)"
bigorg fdw+direct-moved-org "$(dsn_proxy bench)" 1
verify_end bench misc
dropdb bench; dropdb bench_plain
scenario_done
