#!/usr/bin/env bash
# Thousands of organizations (block 14, item 1): what a partition per organization costs the
# cloud Postgres. For each N: the defects schema with N organizations (ROWS documents each,
# one version per document), lokadb enable, then lokadb.partition_org for every organization
# (move-org's first step), then pgbench as the application role with lokadb.org set the way
# the proxy sets it. No proxy, no FDW: the planner and the executor of the cloud only.
#
# Runs on its own copy of the defects stand (not the e2e stand, no lock needed):
#   COMPOSE_PROJECT_NAME=lokadb-defects-b14o bash e2e/bench_orgs.sh
# NS="1 100 1000 3000" ROWS=20 T=10 (seconds per pgbench run).
# Results: e2e/bench_results/orgs.txt (appended).
set -uo pipefail
export PG_FSYNC=${PG_FSYNC:-on} # measurements need a durable Postgres; e2e and defects run with fsync off
export PG_DATA_TMPFS=${PG_DATA_TMPFS:-/run/lokadb-unused} # and its data on the disk, not in memory
cd "$(dirname "$0")/defects"
export MSYS_NO_PATHCONV=1
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-lokadb-defects-b14o}
C="docker compose"
NS=${NS:-1 100 1000 3000}
ROWS=${ROWS:-20}
T=${T:-10}
OUT=../bench_results/orgs.txt
SPEC='{"tables": [{"name": "documents", "tenant_column": "organization_id"}, {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}]}'

log() { printf '\n== %s\n' "$*" | tee -a "$OUT"; }
since() { awk -v s="$1" -v e="$(date +%s.%N)" 'BEGIN { printf "%.1f", e - s }'; }
lokadb() { docker run --rm --network "${COMPOSE_PROJECT_NAME}_default" --entrypoint sh "$IMG" -c "echo '$SPEC' >/tmp/s.json && lokadb $*" 2>&1; }
psql_() { $C exec -T cloud psql -U postgres -v ON_ERROR_STOP=1 -qAt "$@"; }

# Stands (tasks/COORDINATION.md, section 3): at most two defects stands on the machine and none
# started while somebody else holds full-run. The slot is released when the script ends.
LOCKS=${LOKADB_LOCKS:-${TMPDIR:-/tmp}/lokadb-locks}; mkdir -p "$LOCKS" # the slot directory of this machine
if [ -d $LOCKS/full-run ] && ! grep -q '^b14' $LOCKS/full-run/owner 2>/dev/null; then
  echo "full-run is held ($(cat $LOCKS/full-run/owner 2>/dev/null)): not starting"; exit 3
fi
# slots go by the queue $LOCKS/DQUEUE: join it, take a slot only as its first line
grep -qx b14 $LOCKS/DQUEUE 2>/dev/null || echo b14 >>$LOCKS/DQUEUE
SLOT=
while [ -z "$SLOT" ]; do
  if [ "$(head -1 $LOCKS/DQUEUE)" = b14 ]; then
    for s in defects-1 defects-2; do mkdir $LOCKS/$s 2>/dev/null && SLOT=$s && break; done
  fi
  [ -n "$SLOT" ] || sleep 30
done
sed -i '/^b14$/d' $LOCKS/DQUEUE
echo "b14 $(date -Is) $(basename "$0")" >$LOCKS/$SLOT/owner
trap '$C down -v >/dev/null 2>&1; rm -rf $LOCKS/$SLOT' EXIT
$C down -v >/dev/null 2>&1
# the lokadb binary: the image of this worktree's defects stand (built once, BUILD=1 rebuilds)
IMG=${IMG:-lokadb-defects-b14-check}
if [ -n "${BUILD:-}" ] || ! docker image inspect "$IMG" >/dev/null 2>&1; then
  COMPOSE_PROJECT_NAME=${IMG%-check} docker compose build check >/dev/null 2>&1 || { echo "build failed"; exit 1; }
fi
$C up -d --wait cloud >/dev/null 2>&1 || exit 1
psql_ -c "CREATE ROLE app LOGIN PASSWORD 'app'"
log "$(date -Is) ROWS=$ROWS T=$T; $(psql_ -c 'SHOW server_version'), max_locks_per_transaction=$(psql_ -c 'SHOW max_locks_per_transaction')"

# pgbench scripts: lokadb.org first, as the proxy does (a separate statement)
$C exec -T cloud sh -c "mkdir -p /b && cd /b && cat >org_id.sql <<'EOF'
\set org random(1, :n)
\set id (:org - 1) * :rows + random(1, :rows)
SELECT set_config('lokadb.org', (:org)::text, false);
SELECT id, title FROM documents WHERE organization_id = :org AND id = :id;
EOF
cat >find.sql <<'EOF'
\set id random(1, :n * :rows)
\set org (:id - 1) / :rows + 1
SELECT set_config('lokadb.org', (:org)::text, false);
SELECT id, title FROM documents WHERE id = :id;
EOF
cat >child.sql <<'EOF'
\set id random(1, :n * :rows)
\set org (:id - 1) / :rows + 1
SELECT set_config('lokadb.org', (:org)::text, false);
SELECT content FROM document_versions WHERE document_id = :id;
EOF
cat >update.sql <<'EOF'
\set id random(1, :n * :rows)
\set org (:id - 1) / :rows + 1
SELECT set_config('lokadb.org', (:org)::text, false);
UPDATE documents SET title = 'u' WHERE id = :id;
EOF
cat >insert.sql <<'EOF'
\set org random(1, :n)
SELECT set_config('lokadb.org', (:org)::text, false);
INSERT INTO documents (organization_id, user_id, title) VALUES (:org, 1, 'i');
EOF"

for n in $NS; do
  db=orgs_$n
  log "N=$n organizations"
  psql_ -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db OWNER app"
  psql_ -d $db -c "CREATE EXTENSION postgres_fdw" \
    -c "CREATE TABLE users (id bigserial PRIMARY KEY, name text)" \
    -c "CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, user_id bigint REFERENCES users ON DELETE CASCADE, title text)" \
    -c "CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents ON DELETE SET NULL, content text)" \
    -c "ALTER TABLE users OWNER TO app" -c "ALTER TABLE documents OWNER TO app" -c "ALTER TABLE document_versions OWNER TO app" \
    -c "INSERT INTO users (name) VALUES ('u')" \
    -c "INSERT INTO documents (organization_id, user_id, title) SELECT o, 1, 'd' FROM generate_series(1, $n) o, generate_series(1, $ROWS) r ORDER BY o, r" \
    -c "INSERT INTO document_versions (document_id, content) SELECT id, 'v' || id FROM documents ORDER BY id"
  s=$(date +%s.%N)
  lokadb enable -db "'host=cloud dbname=$db user=postgres password=pw sslmode=disable'" -spec /tmp/s.json -owner app >/dev/null \
    || { echo "enable failed" | tee -a "$OUT"; continue; }
  echo "enable: $(since $s) s" | tee -a "$OUT"
  # every organization gets its own partition, one transaction per call (as move-org does)
  s=$(date +%s.%N)
  seq 1 "$n" | awk '{print "SELECT lokadb.partition_org('"'"'documents'"'"', " $1 "); SELECT lokadb.partition_org('"'"'document_versions'"'"', " $1 ");"}' \
    | $C exec -T cloud psql -U postgres -d $db -v ON_ERROR_STOP=1 -qAt >/dev/null || { echo "partition_org failed" | tee -a "$OUT"; continue; }
  echo "partition_org x $n: $(since $s) s; partitions of documents: $(psql_ -d $db -c "SELECT count(*) FROM pg_inherits WHERE inhparent = 'documents'::regclass")" | tee -a "$OUT"
  psql_ -d $db -c "VACUUM ANALYZE"
  # one more partition at N: the cost of move-org's first step for the next organization
  psql_ -d $db -c "INSERT INTO documents (organization_id, user_id, title) SELECT $((n + 1)), 1, 'd' FROM generate_series(1, $ROWS)"
  s=$(date +%s.%N)
  psql_ -d $db -c "SELECT lokadb.partition_org('documents', $((n + 1)))" >/dev/null
  echo "partition_org of organization $((n + 1)) with $n present: $(since $s) s" | tee -a "$OUT"

  # planning time and memory of one backend, as the application
  for q in "documents WHERE organization_id = 1 AND id = 1" "documents WHERE id = 1" "document_versions WHERE document_id = 1"; do
    plan=$($C exec -T -e PGPASSWORD=app cloud psql -h 127.0.0.1 -U app -d $db -qAt \
      -c "SELECT set_config('lokadb.org', '1', false)" -c "EXPLAIN (ANALYZE, SUMMARY) SELECT * FROM $q" \
      -c "SELECT 'mem ' || pg_size_pretty(sum(total_bytes)) FROM pg_backend_memory_contexts" \
      -c "SELECT 'locks ' || count(*) FROM pg_locks WHERE pid = pg_backend_pid()" 2>&1)
    echo "explain $q: $(echo "$plan" | grep -E 'Planning Time|Execution Time|^mem|Subplans Removed' | tr '\n' ' ')" | tee -a "$OUT"
  done
  upd=$($C exec -T -e PGPASSWORD=app cloud psql -h 127.0.0.1 -U app -d $db -qAt \
    -c "SELECT set_config('lokadb.org', '1', false)" -c "BEGIN" -c "EXPLAIN (ANALYZE, SUMMARY) UPDATE documents SET title = 'x' WHERE id = 1" \
    -c "SELECT 'locks ' || count(*) FROM pg_locks WHERE pid = pg_backend_pid()" -c "ROLLBACK" 2>&1)
  echo "explain UPDATE documents WHERE id = 1: $(echo "$upd" | grep -E 'Planning Time|Execution Time|^locks|ERROR' | tr '\n' ' ')" | tee -a "$OUT"

  # throughput: one client (latency), then 16 (shared lock table), simple and prepared
  for script in org_id find child update insert; do
    for mode in simple prepared; do
      for c in 1 16; do
        r=$($C exec -T -e PGPASSWORD=app cloud pgbench -h 127.0.0.1 -U app -n -M $mode -c $c -j $c -T "$T" \
          -D n=$n -D rows=$ROWS -f /b/$script.sql $db 2>&1)
        tps=$(echo "$r" | grep -oE 'tps = [0-9.]+' | head -1)
        lat=$(echo "$r" | grep -oE 'latency average = [0-9.]+ ms')
        fail=$(echo "$r" | grep -oE 'number of failed transactions: [0-9]+' )
        err=$(echo "$r" | grep -m1 -E 'ERROR|error' | cut -c1-160)
        echo "pgbench $script $mode c=$c: $tps, $lat; $fail $err" | tee -a "$OUT"
      done
    done
  done
  psql_ -c "DROP DATABASE $db WITH (FORCE)"
done
$C down -v >/dev/null 2>&1
