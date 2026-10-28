#!/usr/bin/env bash
# Thousands of organizations (BACKLOG block 14, item 1): what a partition per organization costs.
#   bash bench/scenarios/orgs.sh INVENTORY
# For each N in NS (1 100 1000 3000): N organizations of ROWS (20) documents; lokadb enable; lokadb.partition_org
# for every organization (move-org's first step, so each has a partition of its own); then through the proxy
# read / update / insert at 1 and 32 clients, with the extended protocol (prepared plans) and the simple one
# (planned on every request), the cost of a connection (the proxy reads its metadata), and the planner's own
# numbers (EXPLAIN SUMMARY, memory of the backend, locks of an UPDATE).
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init orgs "${1:?inventory}"
NS=${NS:-"1 100 1000 3000"}
ROWS=${ROWS:-20}
CLIENTS_LIST=${CLIENTS_LIST:-"1 32"}
SAMPLE=1
put_spec docs "$SPEC_DOCS"
cloud_sql postgres "GRANT pg_read_all_stats TO app" >/dev/null # pg_backend_memory_contexts below

for n in $NS; do
  note "== N = $n organizations"
  mkdb bench
  setup_data bench "$n" "$ROWS"
  t0=$(date +%s.%N); en=$(enable_db bench docs); t1=$(date +%s.%N)
  note "enable: $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }') s"
  t0=$(date +%s.%N)
  seq 1 "$n" | awk '{ print "SELECT lokadb.partition_org(" q "documents" q ", " $1 "); SELECT lokadb.partition_org(" q "document_versions" q ", " $1 ");" }' q="'" |
    on cloud "docker exec -i -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h 127.0.0.1 -U postgres -d bench -v ON_ERROR_STOP=1 -qAt" >/dev/null
  t1=$(date +%s.%N)
  note "partition_org x $n: $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.1f", b - a }') s; partitions of documents: $(cloud_sql bench "SELECT count(*) FROM pg_inherits WHERE inhparent = 'documents'::regclass")"
  cloud_sql bench "VACUUM ANALYZE" >/dev/null
  # one more organization: the cost of move-org's first step for the next one
  cloud_sql bench "INSERT INTO documents (organization_id, user_id, title, body) SELECT $((n + 1)), 1, 'd', 'b' FROM generate_series(1, $ROWS)" >/dev/null
  t0=$(date +%s.%N); cloud_sql bench "SELECT lokadb.partition_org('documents', $((n + 1)))" >/dev/null; t1=$(date +%s.%N)
  note "partition_org of organization $((n + 1)) with $n present: $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.2f", b - a }') s"

  # the planner: planning time, executed time, backend memory, locks (as the application role, lokadb.org set as the proxy does)
  for q in "documents WHERE organization_id = 1 AND id = 1" "documents WHERE id = 1" "document_versions WHERE document_id = 1"; do
    out=$(on cloud "docker exec -i -e PGPASSWORD=$APP_PASSWORD bb-cloud psql -h 127.0.0.1 -U app -d bench -qAt" <<EOF
SELECT set_config('lokadb.org', '1', false);
EXPLAIN (ANALYZE, SUMMARY) SELECT * FROM $q;
SELECT 'mem ' || pg_size_pretty(sum(total_bytes)) FROM pg_backend_memory_contexts;
EOF
)
    note "explain $q: $(echo "$out" | grep -E 'Planning Time|Execution Time|^mem|Subplans Removed' | tr '\n' ' ')"
  done
  out=$(on cloud "docker exec -i -e PGPASSWORD=$APP_PASSWORD bb-cloud psql -h 127.0.0.1 -U app -d bench -qAt" <<EOF
SELECT set_config('lokadb.org', '1', false);
BEGIN;
EXPLAIN (ANALYZE, SUMMARY) UPDATE documents SET title = 'x' WHERE id = 1;
SELECT 'locks ' || count(*) FROM pg_locks WHERE pid = pg_backend_pid();
ROLLBACK;
EOF
)
  note "explain UPDATE documents WHERE id = 1: $(echo "$out" | grep -E 'Planning Time|Execution Time|^locks' | tr '\n' ' ')"

  MOVED=()
  proxy_config bench
  for proto in extended simple; do
    flag=""; [ $proto = simple ] && flag="-simple"
    for c in $CLIENTS_LIST; do
      for op in read update insert; do
        run_load orgs "orgs=$n" "protocol=$proto" "clients=$c" "op=$op" -- \
          sql -dsn "'$(dsn_proxy bench)'" -op $op -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org 1 -orgs "$n" -rows "$ROWS" $flag
      done
    done
  done
  run_load orgs "orgs=$n" "op=connect" -- connect -dsn "'$(dsn_proxy bench)'" -org 1 -n 300
  dropdb bench
done
scenario_done
