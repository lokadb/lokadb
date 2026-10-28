#!/usr/bin/env bash
# Moving an organization under a write load (block 5's scenario, block 14 item 3): a big organization, the
# modes, the batch size, a real network between the cloud and the organization.
#   bash bench/scenarios/move.sh INVENTORY
# SIZES = "rows:body_bytes ..." of organization 1 (default 300000:200 3000000:200 100000:100000 = 60 MB, 600 MB, 10 GB of
# 100 KB documents), MODES (logical trigger), BATCHES (10000 1000), WRITE_CLIENTS (8; 0: no writer). Each run: a fresh
# database; the writer inserts and updates all organizations through the proxy; move-org under it. Written:
#   the duration of the move and of every step (progress lines), the pause of writes, the writer's longest gap,
#   the cloud's WAL and replication slot every 5 s (raw/wal-*.csv), CPU and memory of the roles at the middle of the copy.
# The network is the inventory's; the trial run uses `bench/deploy.sh netem`.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init move "${1:?inventory}"
SIZES=${SIZES:-"300000:200 3000000:200 100000:100000"}
MODES=${MODES:-"logical trigger"}
BATCHES=${BATCHES:-"10000 1000"}
WRITE_CLIENTS=${WRITE_CLIENTS:-8}
WRITER_LIMIT=${WRITER_LIMIT:-20m} # the writer stops this long after the move began, so a move that does not catch up still ends (and says when)
MOVE_EXTRA=${MOVE_EXTRA:-}       # more move-org flags, e.g. -allow-customer-replication on a stand where the cloud and the organizations have one owner
OTHER_ORGS=${OTHER_ORGS:-4}
put_spec docs "$SPEC_DOCS"
o1=${MOVE_ORG:-${ORGS[0]}} # MOVE_ORG: the organization number of the one to move (the big one lives on the host chosen for it)

timeline_gap_all() {
  awk -F, 'NR > 1 { ok[$1] += $3; if ($1 > mx) mx = $1 } END { g = 0; best = 0; for (i = 0; i <= mx; i++) { if (ok[i] + 0 == 0) { g++; if (g > best) best = g } else g = 0 } print best }' "$1"
}

# watch_wal FILE: the cloud's slots and WAL every 5 s until killed
watch_wal() {
  on cloud "echo 'time,slot,lag_bytes,wal_dir_mb'; while true; do
    docker exec -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h 127.0.0.1 -U postgres -d postgres -qAt -F, -c \"SELECT now(), coalesce(slot_name, '-'), coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) FROM (SELECT 1) x LEFT JOIN pg_replication_slots ON true\" |
      while IFS=, read -r t s l; do echo \"\$t,\$s,\$l,\$(docker exec bb-cloud du -sm /var/lib/postgresql/18/docker/pg_wal 2>/dev/null | cut -f1)\"; done
    sleep 5; done" >"$1" 2>/dev/null &
  WATCH=$!
}

for size in $SIZES; do
  rows=${size%%:*}; body=${size##*:}
  for mode in $MODES; do
    for batch in $BATCHES; do
      [ "$mode" = logical ] && [ "$batch" != "${BATCHES%% *}" ] && continue # batch rows are the trigger mode's
      tag="rows$rows-body$body-$mode-batch$batch"
      note "== $tag"
      mkdb bench
      # the organization's database must be there (the first run after a rebuild once found it missing)
      [ -n "$(org_sql "$o1" postgres "SELECT 1 FROM pg_database WHERE datname = 'cust_bench'")" ] || { note "cust_bench missing at organization $o1: created again"; org_sql "$o1" postgres "CREATE DATABASE cust_bench" >/dev/null; org_sql "$o1" cust_bench "GRANT CREATE ON SCHEMA public TO lokadb_app" >/dev/null; }
      other=$((rows / 10 + 1000))
      setup_data bench $((OTHER_ORGS + 1)) "$other" -big "$rows" -big-org "$o1" -body "$body" -chunk 200000
      note "enable: $(enable_db bench docs | tail -1 | cut -c1-120)"
      MOVED=(); proxy_config bench
      cloud_sql bench "VACUUM ANALYZE" >/dev/null
      verify_begin bench
      if [ "$WRITE_CLIENTS" -gt 0 ]; then
        on load "rm -f $BENCH_HOME/writer.timeline; nohup $BENCH_HOME/bin/benchload sql -dsn '$(dsn_proxy bench)' -op insert,update -c $WRITE_CLIENTS -warmup 0s -d 6h \
          -first-org 1 -orgs $((OTHER_ORGS + 1)) -rows $other -big $rows -big-org $o1 -timeline $BENCH_HOME/writer.timeline >$BENCH_HOME/writer.out 2>&1 &"
        sleep 15
      fi
      watch_wal "$RAW/wal-$tag.csv"
      ( sleep 60; SAMPLE_TAGS="{\"run\": \"$tag\"}"; sample_resources "$SAMPLE_TAGS" ) &
      t0=$(date +%s.%N)
      ( sleep "$(secs "$WRITER_LIMIT")"; on load "pkill -INT -f 'benchload sql -dsn'" >/dev/null 2>&1; date -u +%FT%TZ >"$RAW/writer-stopped-$tag.txt" ) &
      LIMIT=$!
      move_db bench docs "$o1" ${mode:+-mode $mode} ${batch:+-batch-rows $batch} $MOVE_EXTRA >"$RAW/move-$tag.log" 2>&1
      rc=$?
      kill "$LIMIT" 2>/dev/null
      [ -f "$RAW/writer-stopped-$tag.txt" ] && note "the move did not end within $WRITER_LIMIT under the writer: the writer was stopped at $(cat "$RAW/writer-stopped-$tag.txt"), the move ended at $(date -u +%FT%TZ)"
      t1=$(date +%s.%N)
      kill "$WATCH" 2>/dev/null; wait 2>/dev/null
      if [ "$WRITE_CLIENTS" -gt 0 ]; then
        sleep 10
        on load "pkill -INT benchload; sleep 3; cat $BENCH_HOME/writer.out" | tail -2 >"$RAW/writer-$tag.jsonl"
        on load "cat $BENCH_HOME/writer.timeline" >"$RAW/writer-$tag.csv"
      fi
      note "move-org exit $rc in $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }') s; $(grep -oE 'writes_paused=[^ ]+|wait_last_changes=[^ ]+|drop_subscription=[^ ]+|last_transaction=[^ ]+' "$RAW/move-$tag.log" | tr '\n' ' ')"
      [ "$WRITE_CLIENTS" -gt 0 ] && note "writer: longest gap with no commit $(timeline_gap_all "$RAW/writer-$tag.csv") s; $(head -1 "$RAW/writer-$tag.jsonl" | cut -c1-220)"
      # the rows, once more: the organization's database against the cloud's count at the end
      note "rows at the organization: $(org_sql "$o1" cust_bench 'SELECT count(*) FROM documents'); through the cloud: $(cloud_sql bench "SELECT count(*) FROM documents WHERE organization_id = $o1")"
      [ "$rc" = 0 ] && MOVED=("$o1")
      VERIFY_SLACK=$WRITE_CLIENTS verify_end bench "move $tag"
      dropdb bench
    done
  done
done
scenario_done
