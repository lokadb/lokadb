#!/usr/bin/env bash
# One command from a list of hosts to the whole LokaDB stand, and one to remove it (bench/lib.sh
# describes the inventory). Nothing is tied to a particular server: the hosts are the inventory's.
#
#   bash bench/deploy.sh up      INVENTORY    Docker, Postgres (cloud, one per organization), relay, agents, proxy
#   bash bench/deploy.sh push    INVENTORY    new binaries (bench/build.sh) to the hosts; restart the relay, agents and proxy with them
#   bash bench/deploy.sh down    INVENTORY    remove all of it from every host (data included)
#   bash bench/deploy.sh status  INVENTORY    what runs where, is the tunnel up
#   bash bench/deploy.sh ping    INVENTORY    RTT between the hosts that talk to each other (JSON, to stdout)
#   bash bench/deploy.sh netem   INVENTORY add MS|del    trial only: add MS ms of round trip between the agents and the relay
#
# BENCH_KEY: the ssh key (default ~/.ssh/id_ed25519). Needs root on the hosts, Ubuntu/Debian with systemd.
# Postgres is durable (fsync on, synchronous_commit on) and keeps its data on the host's disk.
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
. "$here/lib.sh"
cmd=${1:-}; shift || true
inv_load "${1:?inventory file}"; shift || true
secrets_load

prep_host() { # prep_host NAME: Docker, directories, binaries, /etc/hosts
  local n=$1
  on "$n" "set -e; command -v docker >/dev/null || { apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io >/dev/null; }
    systemctl enable --now docker >/dev/null 2>&1 || true
    # a connection per request (reconnect runs, the proxy without a pool) leaves sockets in TIME_WAIT: without these the local ports run out (cannot assign requested address)
    sysctl -qw net.ipv4.tcp_tw_reuse=1 net.ipv4.ip_local_port_range='1024 65000'
    mkdir -p $BENCH_HOME/bin $BENCH_HOME/run $BENCH_HOME/data
    docker image inspect postgres:18 >/dev/null 2>&1 || docker pull -q postgres:18 >/dev/null"
  put "$n" "$here/dist/lokadb" "$BENCH_HOME/bin/lokadb.new" && put "$n" "$here/dist/benchload" "$BENCH_HOME/bin/benchload.new"
  on "$n" "cd $BENCH_HOME/bin && mv -f lokadb.new lokadb && mv -f benchload.new benchload && chmod +x lokadb benchload"
  local block="# lokadb-bench begin"$'\n'
  local h; for h in "${NAMES[@]}"; do block+="${IP[$h]} $h"$'\n'; done
  block+="# lokadb-bench end"
  on "$n" "sed -i '/# lokadb-bench begin/,/# lokadb-bench end/d' /etc/hosts; cat >>/etc/hosts" <<<"$block"
}

pg_run() { # pg_run NAME CONTAINER PORT DATA ROLE: Postgres in Docker, durable, tuned to the host's memory
  local n=$1 c=$2 port=$3 data=$4 role=$5 mem sb ec maxc
  mem=${BENCH_MEM_MB:-$(on "$n" "awk '/MemTotal/ {print int(\$2/1024)}' /proc/meminfo")} # BENCH_MEM_MB: size Postgres for less than the host has (shared hosts)
  [ "$role" = cloud ] && mem=${CLOUD_MEM_MB:-$mem} # CLOUD_MEM_MB: the cloud host also runs other things (the e2e of the project): size Postgres for less
  sb=$((mem / 4)); ec=$((mem * 3 / 4))
  maxc=$ORG_MAX; [ "$role" = cloud ] && maxc=${CLOUD_MAX:-1500}
  # pg_hba: the image trusts 127.0.0.1, and everything the agent and the relay forward arrives from there;
  # postgres_fdw refuses a server that does not ask for a password. Passwords on every TCP connection.
  # The cloud is closed to everybody but the machines of the stand (BACKLOG block 8, item 11: no way round the proxy):
  # the superuser from the proxy (admin commands), the relay (control plane, the organizations' subscriptions) and itself,
  # the application role from the proxy and itself. The load host is let in too, on purpose: the baselines of the
  # measurements (plain Postgres, the application straight to the cloud) and the applications' own migrations go
  # round the proxy; lokadb check -proxy-hosts is not run on this stand.
  local hba="local all all trust
" ip
  if [ "$role" = cloud ]; then
    for ip in "${IP[proxy]}" "${IP[relay]}" "${IP[cloud]}" 127.0.0.1; do hba+="host all postgres $ip/32 scram-sha-256
"; done
    for ip in "${IP[proxy]}" "${IP[load]}" "${IP[cloud]}" 127.0.0.1; do hba+="host all app $ip/32 scram-sha-256
"; done
    # the role of a logical move (lokadb_repl_<oid>_org<N>, made by move-org): the subscription of the organization's database
    # reaches the cloud through the relay, so the cloud sees the relay's address
    hba+="host all /^lokadb_repl_ ${IP[relay]}/32 scram-sha-256
"
    hba+="host all all 0.0.0.0/0 reject
host all all ::/0 reject
"
  else
    hba+="host all all 0.0.0.0/0 scram-sha-256
host all all ::/0 scram-sha-256
"
  fi
  on "$n" "docker rm -f $c >/dev/null 2>&1; mkdir -p $data; printf '$hba' >$data.hba
    docker run -d --name $c --network host --restart unless-stopped -e POSTGRES_PASSWORD=$PG_PASSWORD -v $data:/var/lib/postgresql -v $data.hba:/etc/pg_hba.conf:ro postgres:18 \
      -c hba_file=/etc/pg_hba.conf -c port=$port -c listen_addresses='127.0.0.1,${IP[$n]}' -c max_connections=$maxc \
      -c shared_buffers=${sb}MB -c effective_cache_size=${ec}MB -c work_mem=16MB -c maintenance_work_mem=1GB \
      -c wal_level=logical -c max_wal_senders=32 -c max_replication_slots=32 -c max_worker_processes=48 \
      -c max_logical_replication_workers=24 -c max_sync_workers_per_subscription=4 -c max_locks_per_transaction=256 \
      -c shared_preload_libraries=pg_stat_statements -c max_wal_size=16GB -c checkpoint_timeout=15min \
      -c random_page_cost=1.1 -c track_io_timing=on >/dev/null
    for i in \$(seq 1 120); do docker exec $c pg_isready -h 127.0.0.1 -p $port -U postgres >/dev/null 2>&1 && exit 0; sleep 1; done; echo 'Postgres $c did not start' >&2; exit 1"
}

up() {
  # always: the stand runs the code of this tree. BENCH_NO_BUILD=1 takes the binaries in dist/ as they are, when Docker is
  # not at hand and the code has not changed since they were built (dist/COMMIT names the code they are)
  [ -n "${BENCH_NO_BUILD:-}" ] && [ -f "$here/dist/lokadb" ] || bash "$here/build.sh" >/dev/null || die "build failed"
  log "hosts: Docker, binaries, /etc/hosts"
  local n
  for n in $(hosts_unique); do prep_host "$n" & done; wait

  log "cloud Postgres on ${IP[cloud]}"
  pg_run cloud bb-cloud $CLOUD_PORT $BENCH_HOME/data/cloud cloud || die "cloud Postgres"
  cloud_sql postgres "DO \$\$ BEGIN CREATE ROLE app LOGIN PASSWORD '$APP_PASSWORD'; EXCEPTION WHEN duplicate_object THEN NULL; END \$\$" >/dev/null
  cloud_sql postgres "CREATE DATABASE controlplane" >/dev/null 2>&1 || true # exists after a second up

  log "relay on ${IP[relay]}"
  local orgs_json="" o
  for o in "${ORGS[@]}"; do orgs_json+="\"$o\": \":$(org_endpoint_port "$o")\", "; done
  orgs_json=${orgs_json%, }
  local cp="host=${IP[cloud]} port=$CLOUD_PORT dbname=controlplane user=postgres password=$PG_PASSWORD sslmode=disable"
  on relay "mkdir -p $BENCH_HOME/certs && $BENCH_HOME/bin/lokadb certs -dir $BENCH_HOME/certs -relay-host relay >/dev/null 2>&1; cat >$BENCH_HOME/relay.json" <<EOF
{
  "listen": ":$RELAY_PORT", "enroll_listen": ":$ENROLL_PORT",
  "cert": "$BENCH_HOME/certs/relay.crt", "key": "$BENCH_HOME/certs/relay.key",
  "ca": "$BENCH_HOME/certs/ca.crt", "ca_key": "$BENCH_HOME/certs/ca.key", "cert_ttl": "168h",
  "control_plane": "$cp",
  "orgs": {$orgs_json},
  "cloud": "${IP[cloud]}:$CLOUD_PORT", "cloud_access": "window",
  "max_cloud_streams": ${RELAY_MAX_CLOUD_STREAMS:-64}, "max_org_conns": ${RELAY_MAX_ORG_CONNS:-1000}, "max_handshakes": ${RELAY_MAX_HANDSHAKES:-256},
  "org_allow": ["${IP[cloud]}/32", "${IP[proxy]}/32", "127.0.0.1/32"]
}
EOF
  svc_start relay relay "$BENCH_HOME/bin/lokadb relay -config $BENCH_HOME/relay.json"
  sleep 3
  on relay "cat $BENCH_HOME/certs/ca.crt" >"$STATE/ca.crt"

  for o in "${ORGS[@]}"; do
    log "organization $o: Postgres and agent on ${IP[org$o]}"
    pg_run "org$o" "bb-org$o" "$(org_pg_port "$o")" "$BENCH_HOME/data/org$o" org || die "org$o Postgres"
    org_sql "$o" postgres "DO \$\$ BEGIN CREATE ROLE lokadb_app LOGIN PASSWORD '$APP_PASSWORD'; EXCEPTION WHEN duplicate_object THEN NULL; END \$\$" >/dev/null
    org_sql "$o" postgres "DO \$\$ BEGIN CREATE ROLE lokadb_dml LOGIN PASSWORD '$APP_PASSWORD'; EXCEPTION WHEN duplicate_object THEN NULL; END \$\$" >/dev/null
    on "org$o" "mkdir -p $BENCH_HOME/certs $BENCH_HOME/agent$o"
    put "org$o" "$STATE/ca.crt" "$BENCH_HOME/certs/ca.crt"
    lokadb_on relay "token -cp '$cp' -org $o -ttl 30m" | on "org$o" "cat >$BENCH_HOME/agent$o/token && chmod 600 $BENCH_HOME/agent$o/token"
    svc_start "org$o" "agent$o" "$BENCH_HOME/bin/lokadb agent -relay relay:$RELAY_PORT -ca $BENCH_HOME/certs/ca.crt -state $BENCH_HOME/agent$o/state -token-file $BENCH_HOME/agent$o/token -db 127.0.0.1:$(org_pg_port "$o") -cloud-listen :$(org_agent_port "$o") -cloud-allow ${IP[org$o]},127.0.0.1 -max-conns $ORG_MAX ${AGENT_SESSIONS:+-sessions $AGENT_SESSIONS}"
  done

  log "proxy on ${IP[proxy]} (no databases yet: the scenarios write its configuration)"
  on proxy "cat >$BENCH_HOME/proxy.json" <<EOF
{"listen": ":$PROXY_PORT", "upstream": "host=${IP[cloud]} port=$CLOUD_PORT sslmode=disable", "customers": {}, "max_connections": 5000}
EOF
  svc_start proxy proxy "$BENCH_HOME/bin/lokadb proxy -config $BENCH_HOME/proxy.json"

  log "the tunnel to every organization"
  for o in "${ORGS[@]}"; do
    local ok=
    for i in $(seq 1 60); do
      if on cloud "docker exec -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h relay -p $(org_endpoint_port "$o") -U postgres -d postgres -qAtc 'SELECT 1'" >/dev/null 2>&1; then ok=1; break; fi
      sleep 2
    done
    [ -n "$ok" ] && echo "organization $o: the cloud reaches its database through relay:$(org_endpoint_port "$o")" || die "organization $o: no tunnel (journalctl -u bb-agent$o on ${IP[org$o]}, bb-relay on ${IP[relay]})"
  done
  ping_all >"$STATE/rtt-up.json"
  log "up; secrets in $STATE/secrets.env, RTT in $STATE/rtt-up.json"
}

down() {
  local n o
  for o in "${ORGS[@]}"; do svc_stop "org$o" "agent$o"; done
  svc_stop proxy proxy; svc_stop relay relay
  for n in $(hosts_unique); do
    on "$n" "docker ps -aq --filter name='^bb-' | xargs -r docker rm -f >/dev/null 2>&1
      for u in \$(systemctl list-units --all --plain --no-legend 'bb-*' 2>/dev/null | awk '{print \$1}'); do systemctl stop \$u 2>/dev/null; systemctl reset-failed \$u 2>/dev/null; done
      sed -i '/# lokadb-bench begin/,/# lokadb-bench end/d' /etc/hosts
      [ -f $BENCH_HOME/netem ] && tc qdisc del dev lo root 2>/dev/null; rm -rf $BENCH_HOME" &
  done
  wait
  # what the runs logged (live.jsonl, progress.log, nodes.jsonl, scenario logs) is kept: a replay of the run is built from it
  if ls "$STATE"/*.jsonl "$STATE"/*.log >/dev/null 2>&1; then
    local keep="$BENCH_DIR/state-archive/$INV_NAME-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$keep" && cp -a "$STATE"/*.jsonl "$STATE"/*.log "$STATE"/orgs "$keep"/ 2>/dev/null
    echo "run logs kept in $keep"
  fi
  rm -rf "$STATE"
  echo "removed from: $(hosts_unique | while read -r n; do echo -n "${SSH[$n]} "; done)"
}

status() {
  local n
  for n in $(hosts_unique); do
    echo "== $n (${SSH[$n]})"
    on "$n" "docker ps --filter name='^bb-' --format '  {{.Names}} {{.Status}}'; systemctl list-units 'bb-*' --plain --no-legend 2>/dev/null | awk '{print \"  \" \$1, \$3, \$4}'"
  done
}

# ping_all: round trip between every pair of hosts that talk to each other, as JSON
ping_all() {
  local o first=1
  echo "{"
  pair() { [ "$first" = 1 ] || echo ","; first=0; printf '  "%s>%s": "%s"' "$1" "$2" "$(rtt "$1" "$2" 10)"; }
  pair load proxy; pair proxy cloud; pair proxy relay; pair cloud relay; pair load cloud
  for o in "${ORGS[@]}"; do pair "org$o" relay; pair relay "org$o"; pair cloud "org$o"; done
  echo; echo "}"
}

# trial only: the agents' traffic to the relay (ports 7443/7444) gets MS ms of round trip on loopback
netem() {
  local n=$1 ms=$2 half
  half=$((ms / 2))
  for h in $(hosts_unique); do
    on "$h" "tc qdisc del dev lo root 2>/dev/null; rm -f $BENCH_HOME/netem; [ '$n' = del ] && exit 0; touch $BENCH_HOME/netem
      tc qdisc add dev lo root handle 1: prio bands 3 priomap 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2
      tc qdisc add dev lo parent 1:1 handle 10: netem delay ${half}ms
      for p in $RELAY_PORT $ENROLL_PORT; do
        tc filter add dev lo protocol ip parent 1:0 prio 1 u32 match ip dport \$p 0xffff flowid 1:1
        tc filter add dev lo protocol ip parent 1:0 prio 1 u32 match ip sport \$p 0xffff flowid 1:1
      done" || return 1
  done
  echo "agents <-> relay: $n ${ms:-} ms of round trip (tc on lo, ports $RELAY_PORT/$ENROLL_PORT)"
}

case $cmd in
  up) up ;;
  down) down ;;
  status) status ;;
  ping) ping_all ;;
  push)
    bash "$here/build.sh" >/dev/null || die "build failed"
    for n in $(hosts_unique); do prep_host "$n" & done; wait
    svc_start relay relay "$BENCH_HOME/bin/lokadb relay -config $BENCH_HOME/relay.json"
    for o in "${ORGS[@]}"; do svc_start "org$o" "agent$o" "$BENCH_HOME/bin/lokadb agent -relay relay:$RELAY_PORT -ca $BENCH_HOME/certs/ca.crt -state $BENCH_HOME/agent$o/state -token-file $BENCH_HOME/agent$o/token -db 127.0.0.1:$(org_pg_port "$o") -cloud-listen :$(org_agent_port "$o") -cloud-allow ${IP[org$o]},127.0.0.1 -max-conns $ORG_MAX ${AGENT_SESSIONS:+-sessions $AGENT_SESSIONS}"; done
    svc_start proxy proxy "$BENCH_HOME/bin/lokadb proxy -config $BENCH_HOME/proxy.json" ;;
  netem) if [ "${1:-}" = del ]; then netem del 0; else netem add "${1:?ms}"; fi ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
