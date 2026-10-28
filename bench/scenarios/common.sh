#!/usr/bin/env bash
# Shared by the scenarios (sourced): databases, enable, move, the proxy's configuration, the
# routing profile of a moved organization, one load run with its result, and the run's metadata.
#
# A scenario script is  bash bench/scenarios/<name>.sh INVENTORY  and writes
#   research/bench/<date>-<name>/{meta.json,raw/*.jsonl,notes.txt}
# Environment: BENCH_KEY (ssh), BENCH_DATE (default today), WARMUP, DURATION, REPEATS (see METHOD.md),
# BENCH_TRIAL=1 marks a run whose numbers are for debugging the scripts only (meta.json says so).
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../lib.sh
. "$here/../lib.sh"

WARMUP=${WARMUP:-15s}
DURATION=${DURATION:-60s}
REPEATS=${REPEATS:-3}
BENCH_DATE=${BENCH_DATE:-$(date +%F)}
SPEC_DOCS='{"tables": [{"name": "documents", "tenant_column": "organization_id"}, {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}]}'

scenario_init() { # scenario_init NAME INVENTORY
  SCEN=$1
  inv_load "$2"; secrets_load
  OUT=$REPO/research/bench/$BENCH_DATE-$SCEN
  RAW=$OUT/raw
  mkdir -p "$RAW"
  : >"$OUT/notes.txt"
  CP="host=${IP[cloud]} port=$CLOUD_PORT dbname=controlplane user=postgres password=$PG_PASSWORD sslmode=disable"
  echo "${ORGS[*]}" >"$STATE/orgs" # the organizations of the stand, for the live view
  note "scenario $SCEN, started $(date -Is)"
  ping_all_json >"$OUT/rtt-start.json"
}

note() { echo "$*" | tee -a "$OUT/notes.txt"; }

# ping_all_json: RTT from the cloud to every other role, by the role's name (ms min/avg/max of 10 pings)
ping_all_json() {
  local o first=1 pairs=("cloud proxy" "cloud relay" "cloud load")
  for o in "${ORGS[@]}"; do pairs+=("cloud org$o"); done
  echo "{"
  local p
  for p in "${pairs[@]}"; do
    [ "$first" = 1 ] || echo ","
    first=0
    set -- $p
    printf '  "%s>%s": "%s"' "$1" "$2" "$(rtt "$1" "$2" 10)"
  done
  echo; echo "}"
}

# hostkey NAME: the roles that share NAME's host, joined by + ("proxy+relay"); results name hosts by role, never by address
hostkey() {
  local n key=""
  for n in "${NAMES[@]}"; do [ "${SSH[$n]}" = "${SSH[$1]}" ] && key+="${key:++}$n"; done
  echo "$key"
}

# scenario_done: RTT at the end and the run's metadata (versions, servers, settings)
scenario_done() {
  ping_all_json >"$OUT/rtt-end.json"
  local h hostsjson="" first=1
  for h in $(hosts_unique); do
    [ "$first" = 1 ] || hostsjson+=","
    first=0
    hostsjson+=$(on "$h" "printf '\"%s\": {\"cpus\": %s, \"mem_mb\": %s, \"disk\": \"%s\", \"kernel\": \"%s\"}' '$(hostkey "$h")' \$(nproc) \$(awk '/MemTotal/ {print int(\$2/1024)}' /proc/meminfo) \"\$(lsblk -dno SIZE,ROTA,TYPE \$(df --output=source $BENCH_HOME | tail -1 | sed 's/[0-9]*\$//') 2>/dev/null | head -1 | tr -s ' ')\" \$(uname -r)")
  done
  cat >"$OUT/meta.json" <<EOF
{
  "scenario": "$SCEN", "date": "$BENCH_DATE", "finished": "$(date -Is)",
  "trial": ${BENCH_TRIAL:-0},
  "lokadb_commit": "$(tr '\n' ' ' <"$BENCH_DIR/dist/COMMIT")",
  "postgres": "$(cloud_sql postgres 'SHOW server_version')",
  "cloud_settings": "$(cloud_sql postgres "SELECT string_agg(name || '=' || setting || coalesce(unit, ''), ' ' ORDER BY name) FROM pg_settings WHERE name IN ('shared_buffers','effective_cache_size','max_connections','work_mem','fsync','synchronous_commit','wal_level','max_locks_per_transaction')")",
  "proxy_extra": "$(echo "${PROXY_EXTRA:-}" | sed 's/"/\\"/g')", "route_log": ${ROUTE_LOG:-0},
  "limits": {"org_max_connections": "$(org_sql "${ORGS[0]}" postgres 'SHOW max_connections' 2>/dev/null)", "agent_max_conns": "$ORG_MAX",
    "relay": "$(on relay "grep -oE '\"max_[a-z_]+\": *[0-9]+' $BENCH_HOME/relay.json | tr -d '\"' | tr '\n' ' '" 2>/dev/null)"},
  "warmup": "$WARMUP", "duration": "$DURATION", "repeats": $REPEATS,
  "hosts": {$hostsjson}
}
EOF
  note "finished $(date -Is); results in $OUT"
  scrub_results "$OUT"
}

# scrub_results DIR: nothing that says where a server stood stays in the results: addresses (the loopback aside) and
# user@host become <ip> and <host>; the check below fails the run's end if anything is left (bench/check-clean.py)
scrub_results() {
  find "$1" -type f \( -name '*.txt' -o -name '*.log' -o -name '*.json' -o -name '*.jsonl' -o -name '*.csv' -o -name '*.md' \) -print0 |
    xargs -0 -r sed -i -E 's/[A-Za-z0-9_.-]+@([0-9]{1,3}\.){3}[0-9]{1,3}/<host>/g; /127\.0\.0\.1|0\.0\.0\.0/!s/([0-9]{1,3}\.){3}[0-9]{1,3}/<ip>/g'
  python3 "$BENCH_DIR/check-clean.py" "$1" || note "!! the results of $SCEN still name a place or an address: fix before anything is committed"
}

# ---- databases -------------------------------------------------------------------------------------

# mkdb DB: the cloud database (owner app, postgres_fdw) and, for every organization, cust_DB
mkdb() {
  local db=$1 o
  cloud_sql postgres "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null
  cloud_sql postgres "CREATE DATABASE $db OWNER app" >/dev/null
  cloud_sql "$db" "CREATE EXTENSION IF NOT EXISTS postgres_fdw" >/dev/null
  cloud_sql "$db" "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
  for o in "${ORGS[@]}"; do
    org_sql "$o" postgres "DROP DATABASE IF EXISTS cust_$db WITH (FORCE)" >/dev/null
    org_sql "$o" postgres "CREATE DATABASE cust_$db" >/dev/null
    org_sql "$o" "cust_$db" "GRANT CREATE ON SCHEMA public TO lokadb_app" >/dev/null
  done
}

dropdb() { # dropdb DB
  local o
  cloud_sql postgres "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null
  for o in "${ORGS[@]}"; do org_sql "$o" postgres "DROP DATABASE IF EXISTS cust_$1 WITH (FORCE)" >/dev/null; done
}

# load on the load host: bl ARGS... (benchload; BENCH_TAGS tags every result line)
bl() { on load "$BENCH_HOME/bin/benchload $*"; }

# setup_data DB ORGS ROWS [benchload setup flags]: the schema and the rows, as the owner app
setup_data() {
  local db=$1 orgs=$2 rows=$3; shift 3
  bl setup -dsn "'$(dsn_cloud "$db" app)'" -orgs "$orgs" -rows "$rows" "$@" | tee -a "$OUT/notes.txt"
  cloud_sql "$db" "CREATE INDEX IF NOT EXISTS documents_org ON documents (organization_id)" >/dev/null
  cloud_sql "$db" "CREATE INDEX IF NOT EXISTS versions_doc ON document_versions (document_id)" >/dev/null
  cloud_sql "$db" "ANALYZE" >/dev/null
}

# ---- lokadb on the proxy host (the admin commands run next to the proxy: in the cloud network) -------

put_spec() { # put_spec NAME JSON
  echo "$2" | on proxy "cat >$BENCH_HOME/spec-$1.json"
}
admin_dsn() { echo "host=${IP[cloud]} port=$CLOUD_PORT dbname=$1 user=postgres password=$PG_PASSWORD sslmode=disable"; }
cust_dsn() { # cust_dsn DB ORG USER: the organization's database as the cloud reaches it, through the relay
  echo "host=relay port=$(org_endpoint_port "$2") dbname=cust_$1 user=$3 password=$( [ "$3" = postgres ] && echo "$PG_PASSWORD" || echo "$APP_PASSWORD") sslmode=disable"
}

enable_db() { # enable_db DB SPECNAME
  lokadb_on proxy "enable -db '$(admin_dsn "$1")' -spec $BENCH_HOME/spec-$2.json -owner app" 2>&1 | tail -2
}

move_db() { # move_db DB SPECNAME ORG [move-org flags]: the organization into its own database, profile MOVE_PROFILE (fdw) unless the flags name one
  local db=$1 spec=$2 o=$3; shift 3
  lokadb_on proxy "move-org -db '$(admin_dsn "$db")' -spec $BENCH_HOME/spec-$spec.json -owner app -org $o \
    -customer '$(cust_dsn "$db" "$o" postgres)' -customer-app '$(cust_dsn "$db" "$o" lokadb_app)' \
    -publisher 'host=${IP[org$o]} port=$(org_agent_port "$o") dbname=$db user=postgres password=$PG_PASSWORD sslmode=disable' \
    -cp '$CP' ${MOVE_MODE:+-mode $MOVE_MODE} $([[ " $* " == *" -profile "* ]] || echo "-profile ${MOVE_PROFILE:-fdw}") $*" 2>&1
}

# proxy_config DB[:fanout],... : the proxy knows these databases; every moved organization (see MOVED)
# is reached through the relay by its own roles. MAX connections of the proxy: PROXY_MAX.
MOVED=()
proxy_config() {
  local cust="" capp="" fan="" spec o db fm
  for spec in "$@"; do
    db=${spec%%:*}; fm=reject; [ "$spec" != "$db" ] && fm=${spec#*:}
    local c="" a=""
    for o in "${MOVED[@]}"; do
      c+="\"$o\": \"$(cust_dsn "$db" "$o" lokadb_app)\", "
      a+="\"$o\": \"$(cust_dsn "$db" "$o" lokadb_dml)\", "
    done
    cust+="\"$db\": {${c%, }}, "; capp+="\"$db\": {${a%, }}, "; fan+="\"$db\": {\"mode\": \"$fm\"}, "
  done
  # The log line "route" of every statement goes through one mutex and, under systemd, to journald: off by default
  # since 26.10; ROUTE_LOG=1 turns it on (the variant of `overhead` that measures it).
  local extra=${PROXY_EXTRA:-}
  [ -n "${ROUTE_LOG:-}" ] && extra='"route_log": true'${extra:+, $extra}
  # the cache of cloud connections is on by default (since f448365) for a database with moved organizations: NO_POOL=1
  # turns it off, the way every run of the first pass had it
  [ -n "${NO_POOL:-}" ] && [[ "$extra" != *upstream_pool* ]] && extra+="${extra:+, }\"upstream_pool\": false"
  local creds=""
  # the profile direct reads the organizations' roles from the control plane (direct_credentials)
  [ -n "${DIRECT_CREDS:-}" ] && creds=", \"control_plane\": \"$CP\", \"credentials_key\": \"$BENCH_HOME/credentials.key\""
  on proxy "cat >$BENCH_HOME/proxy.json" <<EOF
{"listen": ":$PROXY_PORT", "upstream": "host=${IP[cloud]} port=$CLOUD_PORT sslmode=disable", "max_connections": ${PROXY_MAX:-5000},
 "customers": {${cust%, }}, "customers_app": {${capp%, }}, "fanout": {${fan%, }}$creds${extra:+, $extra}}
EOF
  svc_start proxy proxy "$BENCH_HOME/bin/lokadb proxy -config $BENCH_HOME/proxy.json"
  local i
  for i in $(seq 1 30); do on load "nc -z ${IP[proxy]} $PROXY_PORT" 2>/dev/null && return 0; sleep 1; done
  die "the proxy does not listen: journalctl -u bb-proxy on ${IP[proxy]}"
}

# direct_credentials DB ORG: the roles of the organization in the control plane (profile direct)
direct_credentials() {
  local db=$1 o=$2 kind
  on proxy "test -f $BENCH_HOME/credentials.key || { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' 
' >$BENCH_HOME/credentials.key; chmod 600 $BENCH_HOME/credentials.key; }"
  for kind in direct:lokadb_dml owner:lokadb_app; do
    echo "$(cust_dsn "$db" "$o" "${kind#*:}")" | lokadb_on proxy "org-credentials -db '$(admin_dsn "$db")' -control-plane '$CP' -key $BENCH_HOME/credentials.key -org $o -kind ${kind%%:*} -dsn -" 2>&1 | tail -1
  done
}

# set_profile DB ORG fdw|fdw+direct|direct
set_profile() {
  local db=$1 o=$2 prof=$3 extra=""
  extra="-customer-app '$(cust_dsn "$db" "$o" lokadb_app)' -direct-role lokadb_dml"
  [ "$prof" = fdw ] && extra=""
  [ "$prof" = direct ] && extra="$extra -control-plane '$CP' -key $BENCH_HOME/credentials.key"
  lokadb_on proxy "org-profile -db '$(admin_dsn "$db")' -org $o $extra $prof" 2>&1 | tail -3
}

# ---- one load run ------------------------------------------------------------------------------------

# tags_json KEY=VALUE...: BENCH_TAGS for a run
tags_json() {
  local out="{\"scenario\": \"$SCEN\"" kv
  for kv in "$@"; do out+=", \"${kv%%=*}\": \"${kv#*=}\""; done
  echo "$out}"
}

# sample_resources TAGS_JSON: CPU and memory of every role while the load runs (one JSON line to RAW/resources.jsonl):
# the proxy and the relay processes (top, 2 s), the Postgres containers (docker stats), the whole host's CPU, and the
# connections the cloud and the organizations' Postgres hold.
sample_resources() {
  local tags=$1 n h line
  local conns
  conns="{\"cloud\": $(cloud_sql postgres "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'" 2>/dev/null || echo null)"
  for n in "${ORGS[@]}"; do conns+=", \"org$n\": $(org_sql "$n" postgres "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'" 2>/dev/null || echo null)"; done
  echo "{\"role\": \"connections\", \"connections\": $conns}, \"tags\": $tags}" >>"$RAW/resources.jsonl"
  for n in cloud $(for o in "${ORGS[@]}"; do echo "org$o"; done); do
    on "$n" "docker stats --no-stream --format '{{json .}}' bb-$n" 2>/dev/null | sed "s/^{/{\"role\": \"$n\", \"tags\": $tags, /" >>"$RAW/resources.jsonl"
  done
  for n in proxy relay; do
    line=$(on "$n" "p=\$(pgrep -f 'lokadb $n -config' | head -1); [ -n \"\$p\" ] && top -b -n 2 -d 2 -p \$p | tail -1 | awk '{printf \"{\\\"cpu_pct\\\": %s, \\\"rss_mb\\\": %.0f}\", \$9, \$6/1024}'" 2>/dev/null)
    echo "{\"role\": \"$n\", \"process\": ${line:-null}, \"tags\": $tags}" >>"$RAW/resources.jsonl"
  done
  for h in $(hosts_unique); do
    line=$(on "$h" "top -b -n 2 -d 2 | grep '^%Cpu' | tail -1 | awk '{printf \"%.1f\", 100 - \$8}'" 2>/dev/null)
    echo "{\"role\": \"host\", \"host\": \"$(hostkey "$h")\", \"cpu_busy_pct\": ${line:-null}, \"tags\": $tags}" >>"$RAW/resources.jsonl"
  done
}

# run_load FILE TAGS... -- benchload ARGS...: repeats REPEATS times; every line goes to RAW/FILE.jsonl.
# SAMPLE=1: the resources are sampled in the middle of the first repeat.
run_load() {
  local file=$1; shift
  local tags=() a
  while [ "${1:-}" != "--" ]; do tags+=("$1"); shift; done
  shift
  local r tj
  for r in $(seq 1 "$REPEATS"); do
    tj=$(tags_json "${tags[@]}" "repeat=$r")
    if [ -n "${SAMPLE:-}" ] && [ "$r" = 1 ]; then
      ( sleep "$(( $(secs "$WARMUP") + $(secs "$DURATION") / 2 - 2 ))"; sample_resources "$tj" ) &
    fi
    out=$(on load "BENCH_TAGS='$tj' $BENCH_HOME/bin/benchload $*") || note "run failed: ${tags[*]} repeat $r"
    echo "$out" >>"$RAW/$file.jsonl"
    # what a live view shows (bench/live-snapshot.py): the last run, by role; no addresses
    printf '{"t": "%s", "scenario": "%s", "tags": %s, "lines": [%s]}
' "$(date -u +%FT%TZ)" "$SCEN" "$tj" "$(echo "$out" | grep '^{' | paste -sd, -)" >>"$STATE/live.jsonl"
    wait
  done
  note "$(tail -n 4 "$RAW/$file.jsonl" | cut -c1-200 | head -4)"
}

secs() { case $1 in *m) echo $(( ${1%m} * 60 )) ;; *s) echo "${1%s}" ;; *) echo "$1" ;; esac; }

# timeline_gap FILE CLASS: the longest run of seconds in which CLASS completed nothing (a CSV of benchload -timeline)
# The data check (bench/verify-data.py). verify_counts DB: "place|organization|rows" of the documents in the cloud's
# own partitions (the table itself before enable) and in the database of every moved organization (MOVED).
verify_counts() {
  local db=$1 o q
  q=$(cloud_sql "$db" "SELECT string_agg(format('SELECT ''cloud'', organization_id, count(*) FROM %s GROUP BY 2', t.relid), ' UNION ALL ') FROM pg_partition_tree('documents') t JOIN pg_class c ON c.oid = t.relid WHERE t.isleaf AND c.relkind = 'r'")
  [ -n "$q" ] && cloud_sql "$db" "$q"
  for o in "${MOVED[@]}"; do org_sql "$o" "cust_$db" "SELECT 'org$o', organization_id, count(*) FROM documents GROUP BY 2"; done
}
# verify_begin DB: the counts now, and where the raw files end (what is written after is what the check counts)
verify_begin() {
  verify_counts "$1" >"$OUT/.verify-before" 2>/dev/null
  rm -rf "$OUT/.verify-mark"; mkdir -p "$OUT/.verify-mark"
  local f; for f in "$RAW"/*.jsonl; do [ -f "$f" ] && wc -l <"$f" >"$OUT/.verify-mark/$(basename "$f")"; done
}
# verify_end DB LABEL [FILE...]: the counts against verify_begin's, the inserts benchload counted in the raw lines
# written since (and in FILE...); one line into notes and data-check.txt
verify_end() {
  local db=$1 label=$2 f n; shift 2
  verify_counts "$db" >"$OUT/.verify-after" 2>/dev/null
  : >"$OUT/.verify-new"
  for f in "$RAW"/*.jsonl; do
    [ -f "$f" ] || continue
    n=$(cat "$OUT/.verify-mark/$(basename "$f")" 2>/dev/null || echo 0)
    tail -n +$((n + 1)) "$f" >>"$OUT/.verify-new"
  done
  local moved; moved=$(IFS=,; echo "${MOVED[*]}")
  note "$(python3 "$BENCH_DIR/verify-data.py" "$OUT/.verify-before" "$OUT/.verify-after" "$moved" "$label" "$OUT/.verify-new" "$@" | tee -a "$OUT/data-check.txt")"
  rm -rf "$OUT/.verify-before" "$OUT/.verify-after" "$OUT/.verify-new" "$OUT/.verify-mark"
}

timeline_gap() {
  awk -F, -v c="$2" 'NR > 1 && $2 == c { ok[$1] = $3; if ($1 > mx) mx = $1 } END { g = 0; best = 0; for (i = 0; i <= mx; i++) { if (ok[i] + 0 == 0) { g++; if (g > best) best = g } else g = 0 } print best }' "$1"
}
