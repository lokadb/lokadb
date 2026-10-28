#!/usr/bin/env bash
# Thousands of organizations with many sessions (tasks/review/perf.md PF1): the metadata the proxy reads for every
# session at connect and again every 30 s costs the cloud time and memory in proportion to organizations x sessions;
# it shows in p99 and in the cloud's CPU, not in a short run's mean. Long runs (more than twice the metadata
# lifetime), the proxy with and without meta_cache.
#   bash bench/scenarios/sessions.sh INVENTORY
# NS (1000 3000), SESSIONS (10 100), META (off on), DURATION (330s), REPEATS 1: one run is five minutes.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init sessions "${1:?inventory}"
NS=${NS:-"1000 3000"}
SESSIONS=${SESSIONS:-"10 100"}
META=${META:-"off on"}
ROWS=${ROWS:-20}
DURATION=${DURATION:-330s}
REPEATS=${REPEATS_SESSIONS:-1}
SAMPLE=1
put_spec docs "$SPEC_DOCS"
cloud_sql postgres "GRANT pg_read_all_stats TO app" >/dev/null
MOVED=()

for n in $NS; do
  note "== N = $n organizations"
  mkdb bench
  setup_data bench "$n" "$ROWS"
  note "enable: $(enable_db bench docs | tr '\n' ' ' | cut -c1-100)"
  seq 1 "$n" | awk '{ print "SELECT lokadb.partition_org(" q "documents" q ", " $1 "); SELECT lokadb.partition_org(" q "document_versions" q ", " $1 ");" }' q="'" |
    on cloud "docker exec -i -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h 127.0.0.1 -U postgres -d bench -v ON_ERROR_STOP=1 -qAt" >/dev/null
  cloud_sql bench "VACUUM ANALYZE" >/dev/null
  for m in $META; do
    extra=""; [ $m = off ] && extra='"meta_cache": {"off": true}' # on is the default of the proxy
    note "-- meta_cache $m"
    PROXY_EXTRA=$extra proxy_config bench
    cloud_sql postgres "SELECT pg_stat_statements_reset()" >/dev/null 2>&1
    for sess in $SESSIONS; do
      for proto in extended simple; do
        flag=""; [ $proto = simple ] && flag="-simple"
        run_load sessions "orgs=$n" "meta_cache=$m" "sessions=$sess" "protocol=$proto" "clients=$sess" "op=mix" -- \
          sql -dsn "'$(dsn_proxy bench)'" -op read,update -c "$sess" -warmup "$WARMUP" -d "$DURATION" -first-org 1 -orgs "$n" -rows "$ROWS" -timeline "$BENCH_HOME/sessions.timeline" $flag
        on load "cat $BENCH_HOME/sessions.timeline" >"$RAW/timeline-$n-$m-$sess-$proto.csv" 2>/dev/null
      done
    done
    # what the metadata cost the cloud: the statements the proxy reads at connect and refresh
    cloud_sql bench "SELECT left(regexp_replace(query, '\s+', ' ', 'g'), 90) || ' | calls ' || calls || ' | total ms ' || round(total_exec_time) FROM pg_stat_statements WHERE query ILIKE '%pg_inherits%' OR query ILIKE '%pg_index%' OR query ILIKE '%pg_trigger%' ORDER BY total_exec_time DESC LIMIT 6" 2>/dev/null |
      sed "s/^/metadata N=$n meta_cache=$m: /" >>"$RAW/metadata-statements.txt"
  done
  dropdb bench
done
scenario_done
