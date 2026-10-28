#!/usr/bin/env bash
# Many clients at once (block 14, item 2): pgbench with 1..CLIENTS connections through the
# proxy and, for comparison, straight to the cloud Postgres. ORGS organizations of ROWS
# documents; organization 1 is moved to the customer Postgres (postgres_fdw, no tunnel), the
# others stay in the cloud. For each run: transactions/s, latency, failures, the proxy's
# memory and the connections the cloud and the customer Postgres hold.
#
# Every cloud backend that reads a moved organization keeps its own postgres_fdw connection
# to that organization's database: the customer sees as many connections as the cloud has
# backends that touched it.
#
# Runs on its own copy of the defects stand (not the e2e stand, no lock needed):
#   COMPOSE_PROJECT_NAME=lokadb-defects-b14c bash e2e/bench_clients.sh
# CLIENTS="1 10 50 100 200 400" T=15 ORGS=20 ROWS=1000 CLOUD_MAX=1000 (cloud max_connections)
# Results: e2e/bench_results/clients.txt (appended).
set -uo pipefail
export PG_FSYNC=${PG_FSYNC:-on} # measurements need a durable Postgres; e2e and defects run with fsync off
export PG_DATA_TMPFS=${PG_DATA_TMPFS:-/run/lokadb-unused} # and its data on the disk, not in memory
cd "$(dirname "$0")/defects"
export MSYS_NO_PATHCONV=1
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-lokadb-defects-b14c}
C="docker compose"
CLIENTS=${CLIENTS:-1 10 50 100 200 400}
T=${T:-15}
ORGS=${ORGS:-20}
ROWS=${ROWS:-1000}
CLOUD_MAX=${CLOUD_MAX:-1000}
OUT=../bench_results/clients.txt
DB=cl
PROXY=$COMPOSE_PROJECT_NAME-proxy
SPEC='{"tables": [{"name": "documents", "tenant_column": "organization_id"}, {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}]}'
CLOUD="host=cloud dbname=$DB user=postgres password=pw sslmode=disable"
CUST="host=customer dbname=$DB user=postgres password=pw sslmode=disable"

log() { printf '\n== %s\n' "$*" | tee -a "$OUT"; }
psql_() { $C exec -T cloud psql -U postgres -v ON_ERROR_STOP=1 -qAt "$@"; }
lokadb() { docker run --rm --network "${COMPOSE_PROJECT_NAME}_default" --entrypoint sh "$IMG" -c "echo '$SPEC' >/tmp/s.json && lokadb $*" 2>&1; }
conns() { $C exec -T "$1" psql -U postgres -qAt -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND datname = '$DB'"; }

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
trap 'docker rm -f "$PROXY" >/dev/null 2>&1; $C down -v >/dev/null 2>&1; rm -rf $LOCKS/$SLOT' EXIT
$C down -v >/dev/null 2>&1
docker rm -f "$PROXY" >/dev/null 2>&1
# the lokadb binary: the image of this worktree's defects stand (built once, BUILD=1 rebuilds)
IMG=${IMG:-lokadb-defects-b14-check}
if [ -n "${BUILD:-}" ] || ! docker image inspect "$IMG" >/dev/null 2>&1; then
  COMPOSE_PROJECT_NAME=${IMG%-check} docker compose build check >/dev/null 2>&1 || { echo "build failed"; exit 1; }
fi
# the cloud with room for every client (the customer keeps the default 100)
cat >/tmp/b14c-override.yml <<EOF
services:
  cloud:
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_connections=$CLOUD_MAX"]
EOF
C="docker compose -f compose.yml -f $(cygpath -w /tmp/b14c-override.yml 2>/dev/null || echo /tmp/b14c-override.yml)"
$C up -d --wait cloud customer >/dev/null 2>&1 || exit 1
for h in cloud customer; do
  $C exec -T $h psql -U postgres -qc "CREATE ROLE app LOGIN PASSWORD 'app'" -c "CREATE DATABASE $DB OWNER app"
done
log "$(date -Is) CLIENTS=$CLIENTS T=$T ORGS=$ORGS ROWS=$ROWS; max_connections cloud $($C exec -T cloud psql -U postgres -qAtc 'SHOW max_connections'), customer $($C exec -T customer psql -U postgres -qAtc 'SHOW max_connections')"
psql_ -d $DB -c "CREATE EXTENSION postgres_fdw" \
  -c "CREATE TABLE users (id bigserial PRIMARY KEY, name text)" \
  -c "CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, user_id bigint REFERENCES users ON DELETE CASCADE, title text)" \
  -c "CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents ON DELETE SET NULL, content text)" \
  -c "ALTER TABLE users OWNER TO app" -c "ALTER TABLE documents OWNER TO app" -c "ALTER TABLE document_versions OWNER TO app" \
  -c "INSERT INTO users (name) VALUES ('u')" \
  -c "INSERT INTO documents (organization_id, user_id, title) SELECT o, 1, 'd' FROM generate_series(1, $ORGS) o, generate_series(1, $ROWS) r ORDER BY o, r" \
  -c "INSERT INTO document_versions (document_id, content) SELECT id, 'v' FROM documents"
lokadb enable -db "'$CLOUD'" -spec /tmp/s.json -owner app | grep -iE "error" | head -3
lokadb move-org -db "'$CLOUD'" -spec /tmp/s.json -owner app -org 1 -customer "'$CUST'" -publisher "'$CLOUD'" | grep -iE "error" | head -3
psql_ -d $DB -c "VACUUM ANALYZE"

# the proxy in a container on the stand's network
cfg="{\"listen\": \":6432\", \"upstream\": \"host=cloud port=5432 sslmode=disable\", \"customers\": {\"$DB\": {}}, \"fanout\": {\"$DB\": {\"mode\": \"reject\"}}}"
docker run -d --name "$PROXY" --network "${COMPOSE_PROJECT_NAME}_default" --entrypoint sh "$IMG" -c "echo '$cfg' >/tmp/p.json && exec lokadb proxy -config /tmp/p.json" >/dev/null
sleep 2

# pgbench scripts. Through the proxy the application names the organization; straight to the
# cloud it sets lokadb.org itself, as the proxy would.
$C exec -T cloud sh -c "mkdir -p /b && cd /b && cat >proxy_cloud.sql <<'EOF'
\set org random(2, :orgs)
\set id (:org - 1) * :rows + random(1, :rows)
SELECT id, title FROM documents WHERE organization_id = :org AND id = :id;
EOF
cat >proxy_moved.sql <<'EOF'
\set id random(1, :rows)
SELECT id, title FROM documents WHERE organization_id = 1 AND id = :id;
EOF
cat >direct_cloud.sql <<'EOF'
\set org random(2, :orgs)
\set id (:org - 1) * :rows + random(1, :rows)
SELECT set_config('lokadb.org', (:org)::text, false);
SELECT id, title FROM documents WHERE organization_id = :org AND id = :id;
EOF
cat >direct_moved.sql <<'EOF'
\set id random(1, :rows)
SELECT set_config('lokadb.org', '1', false);
SELECT id, title FROM documents WHERE organization_id = 1 AND id = :id;
EOF"

run() { # run SCRIPT HOST PORT CLIENTS
  local r tps lat fail err mem
  r=$($C exec -T -e PGPASSWORD=app cloud pgbench -h "$2" -p "$3" -U app -n -c "$4" -j "$(( $4 < 8 ? $4 : 8 ))" -T "$T" \
    -D orgs=$ORGS -D rows=$ROWS -f /b/$1.sql $DB 2>&1 &
    sleep $((T / 2)); echo "@conns cloud=$(conns cloud) customer=$(conns customer) proxy_mem=$(docker stats --no-stream --format '{{.MemUsage}}' "$PROXY" | cut -d/ -f1)"; wait)
  tps=$(echo "$r" | grep -oE 'tps = [0-9.]+' | head -1)
  lat=$(echo "$r" | grep -oE 'latency average = [0-9.]+ ms')
  fail=$(echo "$r" | grep -oE 'number of failed transactions: [0-9]+ \([0-9.]+%\)')
  err=$(echo "$r" | grep -m1 -iE 'error|fatal|aborted' | cut -c1-200)
  echo "$1 c=$4: $tps, $lat; $fail; $(echo "$r" | grep '^@conns' | cut -c8-) ${err:+; $err}" | tee -a "$OUT"
}
for c in $CLIENTS; do
  log "$c clients"
  run direct_cloud cloud 5432 "$c"
  run proxy_cloud "$PROXY" 6432 "$c"
  run direct_moved cloud 5432 "$c"
  run proxy_moved "$PROXY" 6432 "$c"
done
docker logs "$PROXY" 2>&1 | grep -ciE "level=(warn|error)" | sed 's/^/proxy warnings and errors in its log: /' | tee -a "$OUT"
docker logs "$PROXY" 2>&1 | grep -iE "level=(warn|error)" | sed 's/^time=[^ ]* //' | cut -c1-200 | sort | uniq -c | sort -rn | head -5 | tee -a "$OUT"
docker rm -f "$PROXY" >/dev/null 2>&1
$C down -v >/dev/null 2>&1
