#!/usr/bin/env bash
# Many agents (BACKLOG block 14, item 2): AGENT_COUNTS (10 50 200) agents connect to one relay while organization 1
# works through it. What is measured: the time until every agent has enrolled and its tunnel is up, the relay's
# memory, CPU, open files and threads, the latency of organization 1 (read/update through the proxy) with the agents
# connected and idle, and how long it takes for all of them to come back after the relay restarts.
#   bash bench/scenarios/agents.sh INVENTORY
# The extra agents all run on the host of the last organization of the inventory (AGENTS_ORG) and point at its Postgres: it is the relay that is
# measured, not 200 databases. Their organization numbers are 101, 102, ...
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init agents "${1:?inventory}"
AGENT_COUNTS=${AGENT_COUNTS:-"10 50 200"}
ROWS=${ROWS:-20000}
o1=${AGENTS_ORG:-${ORGS[${#ORGS[@]}-1]}} # the organization whose host runs the extra agents and which is measured: the last of the inventory (the big server)
host1=org$o1
put_spec docs "$SPEC_DOCS"
mkdb bench
setup_data bench $((o1 + 1)) "$ROWS"
note "enable: $(enable_db bench docs | tail -1 | cut -c1-120)"
note "move-org $o1: $(move_db bench docs "$o1" | tail -1 | cut -c1-120)"
MOVED=("$o1")
proxy_config bench
cloud_sql bench "VACUUM ANALYZE" >/dev/null
verify_begin bench

relay_stats() { # relay_stats LABEL: the relay process
  on relay "p=\$(pgrep -f 'lokadb relay -config' | head -1); echo \"rss_mb=\$(awk '/VmRSS/ {print int(\$2/1024)}' /proc/\$p/status) threads=\$(awk '/Threads/ {print \$2}' /proc/\$p/status) fds=\$(ls /proc/\$p/fd | wc -l)\""
}

started=0
for m in $AGENT_COUNTS; do
  note "== $m extra agents"
  t0=$(date +%s.%N)
  for n in $(seq $((started + 1)) "$m"); do
    org=$((100 + n))
    lokadb_on relay "org-endpoint -cp '$CP' -org $org -listen :$((15000 + org))" >/dev/null
    lokadb_on relay "token -cp '$CP' -org $org -ttl 30m" | on "$host1" "mkdir -p $BENCH_HOME/xagent$org && cat >$BENCH_HOME/xagent$org/token"
    on "$host1" "systemd-run -q --unit bb-xagent$org --property=Restart=on-failure $BENCH_HOME/bin/lokadb agent -relay relay:$RELAY_PORT -ca $BENCH_HOME/certs/ca.crt -state $BENCH_HOME/xagent$org/state -token-file $BENCH_HOME/xagent$org/token -db 127.0.0.1:$(org_pg_port "$o1")"
  done
  started=$m
  # every tunnel up: the cloud reaches the database through the endpoint of the last agent
  for i in $(seq 1 120); do
    up=0
    for n in 1 $m; do
      on cloud "docker exec -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h relay -p $((15000 + 100 + n)) -U postgres -d postgres -qAtc 'SELECT 1'" >/dev/null 2>&1 && up=$((up + 1))
    done
    [ "$up" = 2 ] && break
    sleep 2
  done
  note "all $m agents enrolled and connected: $(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.0f", b - a }') s; relay $(relay_stats)"
  SAMPLE=1 run_load agents "agents=$m" "class=moved" "clients=16" "op=mix" -- \
    sql -dsn "'$(dsn_proxy bench)'" -op read,update -c 16 -warmup "$WARMUP" -d "$DURATION" -first-org "$o1" -orgs 1 -rows "$ROWS"
  # the relay restarts: how long until every agent is back
  t0=$(date +%s.%N)
  svc_start relay relay "$BENCH_HOME/bin/lokadb relay -config $BENCH_HOME/relay.json"
  for i in $(seq 1 300); do
    ok=0
    for n in 1 $((m / 2 + 1)) "$m"; do
      on cloud "docker exec -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h relay -p $((15000 + 100 + n)) -U postgres -d postgres -qAtc 'SELECT 1'" >/dev/null 2>&1 && ok=$((ok + 1))
    done
    [ "$ok" = 3 ] && break
    sleep 2
  done
  note "after the relay restarted, three agents of $m back (first, middle, last) in: $(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.0f", b - a }') s; relay $(relay_stats)"
done
# clean up
for n in $(seq 1 "$started"); do
  org=$((100 + n))
  on "$host1" "systemctl stop bb-xagent$org 2>/dev/null; rm -rf $BENCH_HOME/xagent$org" &
  lokadb_on relay "org-endpoint -cp '$CP' -org $org -remove" >/dev/null 2>&1
done
wait
verify_end bench agents
dropdb bench
scenario_done
