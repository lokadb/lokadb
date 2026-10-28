#!/usr/bin/env bash
# Volumes (block 14, item 3): enable of a large table and the move of a large organization,
# with the application writing all along: e2e/stall inserts into documents for random
# organizations on 4 connections; the longest write and the longest gap with no commit at
# all are the downtime each step causes.
#
#   documents          ROWS rows over ORGS organizations; organization 1 has ORG1 of them
#   document_versions  one version per document
#
# Runs on its own copy of the defects stand (not the e2e stand, no lock needed):
#   COMPOSE_PROJECT_NAME=lokadb-defects-b14v bash e2e/bench_volume.sh
# ROWS=5000000 ORGS=20 ORG1=1000000 MODE= (move-org -mode, default auto)
# Results: e2e/bench_results/volume.txt (appended).
set -uo pipefail
export PG_FSYNC=${PG_FSYNC:-on} # measurements need a durable Postgres; e2e and defects run with fsync off
export PG_DATA_TMPFS=${PG_DATA_TMPFS:-/run/lokadb-unused} # and its data on the disk, not in memory
cd "$(dirname "$0")/defects"
export MSYS_NO_PATHCONV=1
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-lokadb-defects-b14v}
C="docker compose"
ROWS=${ROWS:-5000000}
ORGS=${ORGS:-20}
ORG1=${ORG1:-1000000}
MODE=${MODE:-}
OUT=../bench_results/volume.txt
DB=vol
SPEC='{"tables": [{"name": "documents", "tenant_column": "organization_id"}, {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}]}'
CLOUD="host=cloud dbname=$DB user=postgres password=pw sslmode=disable"
CUST="host=customer dbname=$DB user=postgres password=pw sslmode=disable"

log() { printf '\n== %s\n' "$*" | tee -a "$OUT"; }
since() { awk -v s="$1" -v e="$(date +%s.%N)" 'BEGIN { printf "%.1f", e - s }'; }
psql_() { $C exec -T cloud psql -U postgres -v ON_ERROR_STOP=1 -qAt "$@"; }
lokadb() { docker run --rm --network "${COMPOSE_PROJECT_NAME}_default" --entrypoint sh "$IMG" -c "echo '$SPEC' >/tmp/s.json && lokadb $*" 2>&1; }

# writer NAME: e2e/stall in the background on the stand's network, writing as the application
# (every write sets lokadb.org first, as the proxy does); stalls NAME stops it and prints its report
writer() {
  docker rm -f "$COMPOSE_PROJECT_NAME-$1" >/dev/null 2>&1
  docker run -d --name "$COMPOSE_PROJECT_NAME-$1" --network "${COMPOSE_PROJECT_NAME}_default" -v "$STALL:/s" --entrypoint /s/stall     postgres:18 -dsn "host=cloud dbname=$DB user=app password=app sslmode=disable" -c 4 -orgs "$ORGS" ${2:+-duration $2} >/dev/null
}
stalls() {
  docker stop -t 30 "$COMPOSE_PROJECT_NAME-$1" >/dev/null 2>&1
  docker logs "$COMPOSE_PROJECT_NAME-$1" 2>&1 | tail -1
  docker rm -f "$COMPOSE_PROJECT_NAME-$1" >/dev/null 2>&1
}

STALL=$(cd ../stall && pwd -W 2>/dev/null || pwd)
[ -x ../stall/stall ] || MSYS_NO_PATHCONV=1 docker run --rm -v "$STALL:/src" -w /src golang:1.25 sh -c "CGO_ENABLED=0 go build -o stall ." || exit 1
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
trap 'docker rm -f $COMPOSE_PROJECT_NAME-base $COMPOSE_PROJECT_NAME-enable $COMPOSE_PROJECT_NAME-move >/dev/null 2>&1; $C down -v >/dev/null 2>&1; rm -rf $LOCKS/$SLOT' EXIT
$C down -v >/dev/null 2>&1
# the lokadb binary: the image of this worktree's defects stand (built once, BUILD=1 rebuilds)
IMG=${IMG:-lokadb-defects-b14-check}
if [ -n "${BUILD:-}" ] || ! docker image inspect "$IMG" >/dev/null 2>&1; then
  COMPOSE_PROJECT_NAME=${IMG%-check} docker compose build check >/dev/null 2>&1 || { echo "build failed"; exit 1; }
fi
$C up -d --wait cloud customer >/dev/null 2>&1 || exit 1
for h in cloud customer; do
  $C exec -T $h psql -U postgres -qc "CREATE ROLE app LOGIN PASSWORD 'app'" -c "CREATE DATABASE $DB OWNER app"
done
log "$(date -Is) ROWS=$ROWS ORGS=$ORGS ORG1=$ORG1 MODE=${MODE:-auto}"

s=$(date +%s.%N)
psql_ -d $DB -c "CREATE EXTENSION postgres_fdw" \
  -c "CREATE TABLE users (id bigserial PRIMARY KEY, name text)" \
  -c "CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, user_id bigint REFERENCES users ON DELETE CASCADE, title text, body text)" \
  -c "CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents ON DELETE SET NULL, content text)" \
  -c "ALTER TABLE users OWNER TO app" -c "ALTER TABLE documents OWNER TO app" -c "ALTER TABLE document_versions OWNER TO app" \
  -c "INSERT INTO users (name) VALUES ('u')" \
  -c "INSERT INTO documents (organization_id, user_id, title, body) SELECT CASE WHEN g <= $ORG1 THEN 1 ELSE 2 + g % ($ORGS - 1) END, 1, 'd' || g, repeat('x', 100) FROM generate_series(1, $ROWS) g" \
  -c "INSERT INTO document_versions (document_id, content) SELECT id, 'v' FROM documents" \
  -c "CREATE INDEX ON documents (organization_id)" -c "CREATE INDEX ON document_versions (document_id)" \
  -c "VACUUM ANALYZE"
echo "data: $(since $s) s; documents $(psql_ -d $DB -c "SELECT pg_size_pretty(pg_total_relation_size('documents'))"), versions $(psql_ -d $DB -c "SELECT pg_size_pretty(pg_total_relation_size('document_versions'))")" | tee -a "$OUT"

# the same writer before LokaDB: the baseline (no lokadb.org needed yet)
writer base 10s; sleep 12
echo "writer, no LokaDB: $(stalls base)" | tee -a "$OUT"

log "enable with the writer running"
writer enable
sleep 5
s=$(date +%s.%N)
out=$(lokadb enable -db "'$CLOUD'" -spec /tmp/s.json -owner app)
echo "enable: $(since $s) s; $(echo "$out" | grep -iE 'error' | head -3)" | tee -a "$OUT"
sleep 5
echo "writer during enable: $(stalls enable)" | tee -a "$OUT"
psql_ -d $DB -c "VACUUM ANALYZE"

log "move-org of organization 1 ($ORG1 rows) with the writer running"
writer move
sleep 5
s=$(date +%s.%N)
out=$(lokadb move-org -db "'$CLOUD'" -spec /tmp/s.json -owner app -org 1 -customer "'$CUST'" -publisher "'$CLOUD'" ${MODE:+-mode $MODE})
echo "move-org: $(since $s) s" | tee -a "$OUT"
echo "$out" | grep -E 'step|copy done|caught up|switched|paus|error|done' | sed 's/^time=[^ ]* level=[A-Z]* //' | cut -c1-200 | tee -a "$OUT"
sleep 5
echo "writer during move-org: $(stalls move)" | tee -a "$OUT"
echo "organization 1 at the customer: $($C exec -T customer psql -U postgres -d $DB -qAt -c 'SELECT count(*) FROM documents'), in the cloud partitions: $(psql_ -d $DB -c "SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid WHERE organization_id = 1 AND c.relkind = 'r'")" | tee -a "$OUT"
$C down -v >/dev/null 2>&1
