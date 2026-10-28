#!/usr/bin/env bash
# Downtime of enable under load (1.4 / R7). Loads research/realistic/helpdesk.sql (~2.1M rows in the
# six tables below) into Postgres 18, runs pgbench against it (reads and writes of the LokaDB tables
# and of a cloud table, straight to Postgres), and runs lokadb enable in the middle. pgbench prints
# throughput every second; the seconds with 0 transactions are the downtime.
#
#   bash e2e/enabletime/run.sh [label] [extra enable args...]
#   PREPARE=1 bash e2e/enabletime/run.sh after   (enable -prepare under the load, then enable)
#
# Own container and network (lokadb-et-b3*): independent of the e2e and defects stands.
set -uo pipefail
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1
ROOT=$(cd ../.. && pwd -W 2>/dev/null || pwd)
LABEL=${1:-before}
shift || true
N=lokadb-et-b3
PG=$N-pg
OUT=results
mkdir -p "$OUT"
LOAD_SECONDS=${LOAD_SECONDS:-900} # stopped 15 s after enable
ENABLE_AT=${ENABLE_AT:-20}

docker rm -f "$PG" >/dev/null 2>&1
docker network create "$N" >/dev/null 2>&1
docker run -d --name "$PG" --network "$N" -e POSTGRES_PASSWORD=pw postgres:18 \
  -c shared_buffers=512MB -c max_wal_size=8GB -c max_connections=200 >/dev/null || exit 1
for _ in $(seq 1 60); do docker exec "$PG" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && break; sleep 1; done
sleep 2

echo "loading helpdesk.sql..."
docker cp ../../research/realistic/helpdesk.sql "$PG":/tmp/helpdesk.sql
docker exec "$PG" psql -q -U postgres -v ON_ERROR_STOP=1 -f /tmp/helpdesk.sql >/dev/null || exit 1
docker exec "$PG" psql -U postgres -tAc "VACUUM ANALYZE" >/dev/null
docker exec "$PG" psql -U postgres -tAc "SELECT string_agg(t || ' ' || n, ', ') || '; total ' || sum(n) FROM (
  SELECT 'conversations' t, count(*) n FROM conversations UNION ALL SELECT 'messages', count(*) FROM messages
  UNION ALL SELECT 'contacts', count(*) FROM contacts UNION ALL SELECT 'notes', count(*) FROM notes
  UNION ALL SELECT 'attachments', count(*) FROM attachments UNION ALL SELECT 'reporting_events', count(*) FROM reporting_events) x" \
  | tee "$OUT/$LABEL.rows"

echo "building lokadb..."
docker run --rm -v "$ROOT":/src -v lokadb-gomod:/go/pkg/mod -v lokadb-gocache:/root/.cache/go-build -v "$N-bin":/out \
  -w /src/proxy golang:1.25 go build -o /out/lokadb ./cmd/lokadb || exit 1

# a cloud organization's day: read its conversations and messages, write a note;
# plus a cloud table (accounts)
docker exec -i "$PG" sh -c 'cat >/tmp/load.sql' <<'EOF'
\set acc random(2, 300)
SELECT id, status FROM conversations WHERE account_id = :acc ORDER BY id DESC LIMIT 10;
SELECT id, message_type FROM messages WHERE account_id = :acc ORDER BY account_id, created_at DESC LIMIT 20;
INSERT INTO notes (content, account_id, contact_id, created_at, updated_at) VALUES ('load', :acc, 1, now(), now());
UPDATE accounts SET updated_at = now() WHERE id = :acc;
EOF

# lokadb runs in a container started beforehand: starting one on a busy machine took minutes
docker rm -f "$N-run" >/dev/null 2>&1
docker run -d --name "$N-run" --network "$N" -v "$N-bin":/out -v "$ROOT/e2e/enabletime/spec.json":/spec.json golang:1.25 sleep infinity >/dev/null || exit 1

echo "load, enable at ${ENABLE_AT}s ($LABEL)"
docker exec "$PG" pgbench -U postgres -n -c 16 -j 4 -T "$LOAD_SECONDS" -P 1 --progress-timestamp -f /tmp/load.sql postgres >"$OUT/$LABEL.pgbench" 2>&1 &
LOAD=$!
sleep "$ENABLE_AT"
run() { docker exec "$N-run" /out/lokadb enable -db "host=$PG user=postgres password=pw dbname=postgres sslmode=disable" -spec /spec.json -owner postgres "$@"; }
# PREPARE=1: enable -prepare first (online), then enable switches
if [ -n "${PREPARE:-}" ]; then
  PSTART=$(date +%s.%N)
  run -prepare >"$OUT/$LABEL.prepare" 2>&1
  PRC=$?
  PEND=$(date +%s.%N)
  sleep 5
fi
START=$(date +%s.%N)
run "$@" >"$OUT/$LABEL.enable" 2>&1
RC=$?
END=$(date +%s.%N)
sleep 15
docker exec "$PG" pkill -INT pgbench
wait $LOAD
docker rm -f "$PG" "$N-run" >/dev/null 2>&1
# progress lines: "progress: <epoch> s, <tps> tps, lat <ms> ms ..."; inside a window
window() {
  awk -v s="$2" -v e="$3" -v what="$1" -v rc="$4" '
    /^progress:/ { t = $2 + 0; tps = $4 + 0; lat = $7 + 0
      if (t < s) { before += tps; nb++ }
      if (t >= s && t <= e + 1) { n++; if (tps == 0) zero++; if (lat > maxlat) maxlat = lat; sum += tps } }
    END { printf "%s: rc=%s, took %.1f s; under load before: %.0f tps; during it: %d s, %d of them with 0 tps, %.0f tps on average, worst per-second latency %.0f ms\n",
            what, rc, e - s, (nb ? before / nb : 0), n, zero, (n ? sum / n : 0), maxlat }' "$OUT/$LABEL.pgbench"
}
{
  [ -n "${PREPARE:-}" ] && window "$LABEL enable -prepare" "$PSTART" "$PEND" "$PRC"
  window "$LABEL enable" "$START" "$END" "$RC"
} | tee "$OUT/$LABEL.summary"
