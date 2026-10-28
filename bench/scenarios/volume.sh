#!/usr/bin/env bash
# enable of a big table (BACKLOG 1.4, block 14 item 3) and the price of the DEFAULT partition for an organization
# that has been moved (the question of block 3, item 6).
#   bash bench/scenarios/volume.sh INVENTORY
# For each size S in SIZES (1000000 10000000 100000000 rows of documents, body 100 bytes): the table is made (the
# organizations: ORGS_CLOUD (19) share S rows, organization 1 has 20000), a writer inserts for them all the time
# through the proxy (benchload, 4 connections, timeline), and then
#   * enable runs: its duration and how long the writer got nothing through (the longest run of seconds with no commit);
#   * organization 1 is moved to its own database, the DEFAULT partition holding the rest of the rows;
#   * read, feed and update of organization 1 through the proxy: the extra Index Scan of DEFAULT shows in the latency
#     and in EXPLAIN (ANALYZE, BUFFERS) -> raw/explain-<S>.txt, next to the same statement on the foreign partition alone.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init volume "${1:?inventory}"
SIZES=${SIZES:-"1000000 10000000 100000000"}
ORGS_CLOUD=${ORGS_CLOUD:-19}
ORG1_ROWS=${ORG1_ROWS:-20000}
put_spec docs "$SPEC_DOCS"
o1=${ORGS[0]}
MOVED=()

timeline_gap_all() { # the longest run of seconds in which nobody committed
  awk -F, 'NR > 1 { ok[$1] += $3; if ($1 > mx) mx = $1 } END { g = 0; best = 0; for (i = 0; i <= mx; i++) { if (ok[i] + 0 == 0) { g++; if (g > best) best = g } else g = 0 } print best }' "$1"
}

for S in $SIZES; do
  note "== $S rows of documents"
  mkdb bench
  per=$((S / ORGS_CLOUD))
  # organization 1 first (ids 1..ORG1_ROWS), then 2 .. 1+ORGS_CLOUD with per rows each
  t0=$(date +%s.%N)
  setup_data bench $((ORGS_CLOUD + 1)) "$per" -big "$ORG1_ROWS" -body 100 -chunk 5000000
  note "data: $(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.0f", b - a }') s, $(cloud_sql bench "SELECT pg_size_pretty(pg_total_relation_size('documents'))") in documents"

  MOVED=(); verify_begin bench
  # the writer: through the proxy, as the application is, until we stop it. The proxy knows the database from the
  # start, so what enable does to a client that is already connected is part of the measurement (the proxy reads
  # lokadb's metadata when a client connects and again at the latest every 30 s)
  proxy_config bench
  on load "rm -f $BENCH_HOME/writer.timeline; nohup $BENCH_HOME/bin/benchload sql -dsn '$(dsn_proxy bench)' -op insert -c 4 -warmup 5s -d 4h \
    -first-org 2 -orgs $ORGS_CLOUD -rows $per -big $ORG1_ROWS -timeline $BENCH_HOME/writer.timeline >$BENCH_HOME/writer.out 2>&1 &"
  sleep 20
  t0=$(date +%s.%N); enable_db bench docs >/dev/null; t1=$(date +%s.%N)
  note "enable: $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }') s"
  sleep 20
  on load "pkill -INT benchload; sleep 3; cat $BENCH_HOME/writer.out" | tail -1 >>"$RAW/writer-enable-$S.jsonl"
  on load "cat $BENCH_HOME/writer.timeline" >"$RAW/writer-enable-$S.csv"
  note "writer during enable: longest gap with no commit $(timeline_gap_all "$RAW/writer-enable-$S.csv") s; $(tail -1 "$RAW/writer-enable-$S.jsonl" | cut -c1-200)"
  cloud_sql bench "VACUUM ANALYZE" >/dev/null

  t0=$(date +%s.%N); mv=$(move_db bench docs "$o1" | tail -2 | tr '\n' ' '); t1=$(date +%s.%N)
  MOVED=("$o1")
  note "move-org $o1: $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }') s"
  proxy_config bench
  set_profile bench "$o1" fdw >/dev/null

  # what the extra partition costs the moved organization
  for op in read feed update; do
    run_load volume "default_rows=$S" "config=fdw" "class=moved" "clients=1" "op=$op" -- \
      sql -dsn "'$(dsn_proxy bench)'" -op $op -c 1 -warmup "$WARMUP" -d "$DURATION" -first-org 1 -orgs 1 -rows "$ORG1_ROWS"
  done
  {
    for q in "SELECT d.title FROM documents d WHERE d.organization_id = 1 AND d.id = 5" \
             "SELECT id, title FROM documents WHERE organization_id = 1 ORDER BY id DESC LIMIT 20" \
             "SELECT d.title FROM documents__org$o1 d WHERE d.organization_id = 1 AND d.id = 5" \
             "SELECT id, title FROM documents__org$o1 WHERE organization_id = 1 ORDER BY id DESC LIMIT 20"; do
      echo "-- $q"
      on cloud "docker exec -i -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h 127.0.0.1 -U postgres -d bench -qAt" <<EOF
SELECT set_config('lokadb.org', '1', false);
SET ROLE app;
EXPLAIN (ANALYZE, BUFFERS, SUMMARY) $q;
EOF
    done
  } >"$RAW/explain-$S.txt" 2>&1
  note "explain: $(grep -E 'Planning Time|Execution Time' "$RAW/explain-$S.txt" | tr '\n' ' ')"
  verify_end bench "volume $S"
  dropdb bench
done
scenario_done
