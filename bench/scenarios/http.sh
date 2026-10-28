#!/usr/bin/env bash
# Whole HTTP requests of Laravel and Django (BACKLOG block 14, item 9): the same pages over plain
# Postgres, lokadb fdw and fdw+direct. The applications are the e2e ones with five routes added
# (bench/apps): a document page, a feed, an insert, an update, a report.
#   bash bench/scenarios/http.sh INVENTORY
# APPS (laravel django), CONFIGS (plain fdw fdw+direct), TOTAL_ORGS (20), ROWS (20000), CLIENTS_LIST (1 16).
# The applications run on the load host in Docker; Laravel opens a connection per request (PHP), Django keeps
# its connections (gunicorn, CONN_MAX_AGE). RTT between the load host and the proxy is whatever the inventory has.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init http "${1:?inventory}"
APPS=${APPS:-"laravel django"}
CONFIGS=${CONFIGS:-"plain fdw fdw+direct"}
POOL_APPS=${POOL_APPS:-laravel} # the applications that also run with the connection pools (PHP: a connection per request)
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
CLIENTS_LIST=${CLIENTS_LIST:-"1 16"}
OPS=${OPS:-"read feed insert update"}
K=${#ORGS[@]}

build_apps() {
  log "building the application images on the load host"
  (cd "$REPO" && tar czf "$STATE/apps.tgz" e2e/laravel e2e/django bench/apps)
  put load "$STATE/apps.tgz" $BENCH_HOME/apps.tgz
  on load "rm -rf $BENCH_HOME/apps && mkdir -p $BENCH_HOME/apps && tar xzf $BENCH_HOME/apps.tgz -C $BENCH_HOME/apps && cd $BENCH_HOME/apps &&
    for a in $APPS; do docker build -q -f bench/apps/\$a/Dockerfile -t bb-\$a . >/dev/null || exit 1; done"
}

# app_env APP DBHOST DBPORT DBNAME: the environment of the application container
app_env() {
  case $1 in
    laravel) echo "-e DB_CONNECTION=pgsql -e DB_HOST=$2 -e DB_PORT=$3 -e DB_DATABASE=$4 -e DB_USERNAME=app -e DB_PASSWORD=$APP_PASSWORD" ;;
    django) echo "-e DB_HOST=$2 -e DB_PORT=$3 -e DB_NAME=$4 -e DB_USER=app -e DB_PASSWORD=$APP_PASSWORD" ;;
  esac
}

# app_load APP DB: the application's own migration on the cloud database, then the rows
app_load() {
  local app=$1 db=$2 migrate
  case $app in
    laravel) migrate="migrate --force" ;;
    django) migrate="python manage.py migrate docs" ;;
  esac
  on load "docker run --rm --network host $(app_env "$app" "${IP[cloud]}" $CLOUD_PORT "$db") bb-$app $migrate" >/dev/null || die "$app migrate"
  local org_t=organizations proj_t=projects doc=documents ver=document_versions ts="" tsv=""
  case $app in
    laravel) ts=", created_at, updated_at"; tsv=", now(), now()" ;;
    django) org_t=docs_organization proj_t=docs_project doc=docs_document ver=docs_documentversion ;;
  esac
  cloud_sql "$db" "INSERT INTO $org_t (name$ts) SELECT 'o' || g$tsv FROM generate_series(1, $TOTAL_ORGS) g" >/dev/null
  cloud_sql "$db" "INSERT INTO $proj_t (organization_id, name$ts) SELECT g, 'p'$tsv FROM generate_series(1, $TOTAL_ORGS) g" >/dev/null
  cloud_sql "$db" "INSERT INTO $doc (organization_id, project_id, slug, title, body$ts) SELECT o, o, 's' || r, 'doc ' || r, repeat('x', 200)$tsv FROM generate_series(1, $TOTAL_ORGS) o, generate_series(1, $ROWS) r ORDER BY o, r" >/dev/null
  cloud_sql "$db" "INSERT INTO $ver (document_id, content$ts) SELECT id, 'v'$tsv FROM $doc ORDER BY id" >/dev/null
  cloud_sql "$db" "CREATE INDEX IF NOT EXISTS ${ver}_doc ON $ver (document_id)" >/dev/null
  cloud_sql "$db" "ANALYZE" >/dev/null
}

app_start() { # app_start APP DBHOST DBPORT DBNAME
  local run=""
  [ "$1" = laravel ] && run="--entrypoint sh bb-laravel /app/serve.sh" || run="bb-$1"
  app_errors
  on load "docker rm -f bb-app >/dev/null 2>&1; docker run -d --name bb-app --network host $(app_env "$1" "$2" "$3" "$4") $run >/dev/null"
  local i
  for i in $(seq 1 60); do on load "curl -fs http://127.0.0.1:8000/ >/dev/null" 2>/dev/null && return 0; sleep 1; done
  die "$1 does not answer: docker logs bb-app on the load host"
}
# app_errors: the errors the application wrote (an HTTP 500 is named by them), kept before its container goes
app_errors() {
  local f="$RAW/app-errors-$(date +%H%M%S).log"
  on load "docker logs bb-app 2>&1 | grep -E 'ERROR|Exception|Fatal' | grep -v '^ *#' | head -100" >"$f" 2>/dev/null
  [ -s "$f" ] || rm -f "$f"
}
app_stop() { app_errors; on load "docker rm -f bb-app >/dev/null 2>&1"; }

# warm the application and its connections before the measured time
app_warm() { on load "$BENCH_HOME/bin/benchload http -base http://127.0.0.1:8000 -op read,feed -c 8 -warmup 0s -d 15s -orgs $K -rows $ROWS" >/dev/null 2>&1; }

# app_alive: the application answers; if not, its log goes to the results and the variant stops (no runs of nothing)
app_alive() {
  on load "curl -fs -m 5 http://127.0.0.1:8000/ >/dev/null" 2>/dev/null && return 0
  on load "docker logs --tail 200 bb-app 2>&1; docker inspect bb-app --format 'exit {{.State.ExitCode}} oom {{.State.OOMKilled}} at {{.State.FinishedAt}}'" >"$RAW/app-down-$(date +%H%M%S).log" 2>&1
  note "!! the application does not answer: the variant stops (its log: raw/app-down-*.log)"
  HTTP_FAILED=1
  return 1
}

measure_http() { # measure_http APP CONFIG CLASS FIRST N
  local app=$1 cfg=$2 cls=$3 first=$4 n=$5 c op
  for c in $CLIENTS_LIST; do
    for op in $OPS; do
      app_alive || return 1
      SAMPLE=$([ "$c" -gt 1 ] && echo 1) run_load http-$app "app=$app" "config=$cfg" "class=$cls" "clients=$c" "op=$op" "keys=cold" --         http -base http://127.0.0.1:8000 -op $op -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org "$first" -orgs "$n" -rows "$ROWS"
    done
    app_alive || return 1
    run_load http-$app "app=$app" "config=$cfg" "class=$cls" "clients=$c" "op=read" "keys=hot" --       http -base http://127.0.0.1:8000 -op read -c "$c" -warmup "$WARMUP" -d "$DURATION" -first-org "$first" -orgs "$n" -rows "$ROWS" -hot 50
  done
}

build_apps
for app in $APPS; do
  spec=$app
  put_spec "$spec" "$(cat "$REPO/e2e/specs/$app.json")"
  BENCH_READY=
  cfgs=$CONFIGS
  [[ " $POOL_APPS " == *" $app "* ]] && cfgs="$cfgs fdw+pools fdw+direct+pools"
  for cfg in $cfgs; do
    note "== $app $cfg"
    base=${cfg%+pools}; pools=""; [ "$base" != "$cfg" ] && pools=1
    case $base in
      plain)
        mkdb "${app}_plain"; app_load "$app" "${app}_plain"
        app_start "$app" "${IP[cloud]}" $CLOUD_PORT "${app}_plain"; app_warm
        measure_http "$app" plain moved 1 "$K"
        app_stop; dropdb "${app}_plain"
        ;;
      fdw | fdw+direct)
        if [ -z "$BENCH_READY" ]; then
          mkdb "$app"; app_load "$app" "$app"
          note "enable: $(enable_db "$app" "$spec" | tr '\n' ' ' | cut -c1-200)"
          MOVED=()
          for o in "${ORGS[@]}"; do note "move-org $o: $(move_db "$app" "$spec" "$o" | tail -1 | cut -c1-120)"; MOVED+=("$o"); done
          cloud_sql "$app" "VACUUM ANALYZE" >/dev/null
          BENCH_READY=1
        fi
        # the pools variant: the connection pools of the cloud and of the direct path (the way lokadb is set up under PHP);
        # without them every request opens its own postgres_fdw connections to the organization (about 8 round trips)
        extra=""; [ -n "$pools" ] && extra='"upstream_pool": {}, "direct_pool": {}, "meta_cache": {"verify": "1s"}'
        NO_POOL=$([ -z "$pools" ] && echo 1) PROXY_EXTRA=$extra proxy_config "$app" # without pools: no cache of cloud connections either (on by default since f448365)
        for o in "${ORGS[@]}"; do set_profile "$app" "$o" "$base" >/dev/null; done
        app_start "$app" "${IP[proxy]}" $PROXY_PORT "$app"; app_warm
        measure_http "$app" "$cfg" moved 1 "$K"
        [ "$base" = fdw ] && measure_http "$app" "$cfg" cloud $((K + 1)) $((TOTAL_ORGS - K))
        app_stop
        ;;
    esac
  done
  dropdb "$app" 2>/dev/null
done
scenario_done
[ -z "${HTTP_FAILED:-}" ] # a variant that stopped makes the scenario fail: run-all says so
