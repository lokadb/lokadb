#!/usr/bin/env bash
# Shared by bench/deploy.sh and bench/scenarios/*.sh (sourced). The inventory names the hosts:
#
#   # role   name   ssh target      address the other hosts reach it at
#   cloud    cloud  root@CLOUD_SSH  CLOUD_ADDR
#   proxy    proxy  root@PROXY_SSH  PROXY_ADDR
#   relay    relay  root@PROXY_SSH  PROXY_ADDR     (may share a host with the proxy)
#   load     load   root@LOAD_SSH   LOAD_ADDR
#   org      org1   root@ORG1_SSH   ORG1_ADDR      (name org<N>: organization N's database and agent)
# The inventory names real hosts: keep it outside the repository (*.inv is ignored); results name hosts by role only.
#
# Several roles may share a host (the trial run puts them all on one); ports are fixed per role:
#   cloud 5432, proxy 6432, relay 7443 (agents) 7444 (enrollment), org N: Postgres 5440+N,
#   the relay's endpoint of the organization 15000+N, the agent's way to the cloud 5450+N.
# The host names cloud, proxy, relay, load, org<N> are written to /etc/hosts of every host.
set -uo pipefail

BENCH_DIR=${BENCH_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
REPO=$(cd "$BENCH_DIR/.." && pwd)
BENCH_KEY=${BENCH_KEY:-$HOME/.ssh/id_ed25519}
BENCH_HOME=/opt/lokadb-bench
CLOUD_PORT=5432 PROXY_PORT=6432 RELAY_PORT=7443 ENROLL_PORT=7444
# Limits of the stand that are not lokadb's: connections an organization's Postgres takes and an agent forwards
# (every cloud backend that touches a moved organization holds one; the defaults, 100 and 200, are what `clients` would measure).
ORG_MAX=${ORG_MAX:-1500}

declare -A SSH IP ROLE
NAMES=() ORGS=()

log() { printf '\n== %s [%s]\n' "$*" "$(date +%T)"; }
die() { echo "!! $*" >&2; exit 1; }

inv_load() { # inv_load FILE
  local f=$1 role name ssh ip
  [ -f "$f" ] || die "inventory $f not found (see bench/inventory.example)"
  INV=$f
  INV_NAME=$(basename "$f" | sed 's/\.[a-z]*$//')
  STATE=$BENCH_DIR/state/$INV_NAME
  mkdir -p "$STATE"
  while read -r role name ssh ip _; do
    case $role in '' | '#'*) continue ;; esac
    [ -n "$ip" ] || die "inventory line for $name: role name ssh ip"
    case $ssh in *@*) ;; *) ssh=root@$ssh ;; esac   # a bare address: root
    NAMES+=("$name"); SSH[$name]=$ssh; IP[$name]=$ip; ROLE[$name]=$role
    [ "$role" = org ] && ORGS+=("${name#org}")
  done <"$f"
  for r in cloud proxy relay load; do [ -n "${IP[$r]:-}" ] || die "inventory has no '$r' host"; done
  [ ${#ORGS[@]} -gt 0 ] || die "inventory has no org host"
}

ssh_opts=(-i "$BENCH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30 -o ConnectTimeout=20)
on() { local n=$1; shift; ssh "${ssh_opts[@]}" "${SSH[$n]}" "$@"; }              # on NAME 'command'
put() { local n=$1 src=$2 dst=$3; scp -q "${ssh_opts[@]}" "$src" "${SSH[$n]}:$dst"; }   # put NAME local remote
hosts_unique() { # one name per ssh target
  local seen=" " n
  for n in "${NAMES[@]}"; do
    case "$seen" in *" ${SSH[$n]} "*) ;; *) seen+="${SSH[$n]} "; echo "$n" ;; esac
  done
}

org_pg_port() { echo $((5440 + $1)); }
org_endpoint_port() { echo $((15000 + $1)); }
org_agent_port() { echo $((5450 + $1)); }

# secrets: generated once per inventory, kept in bench/state/<inventory>/secrets.env (gitignored)
secrets_load() {
  if [ ! -f "$STATE/secrets.env" ]; then
    {
      echo "PG_PASSWORD=$(head -c 24 /dev/urandom | base64 | tr -dc A-Za-z0-9 | head -c 20)"
      echo "APP_PASSWORD=$(head -c 24 /dev/urandom | base64 | tr -dc A-Za-z0-9 | head -c 20)"
    } >"$STATE/secrets.env"
    chmod 600 "$STATE/secrets.env"
  fi
  # shellcheck disable=SC1090,SC1091
  . "$STATE/secrets.env"
}

# DSNs as the hosts of the scenarios use them
dsn_cloud() { local u=${2:-postgres} p=$PG_PASSWORD; [ "$u" = postgres ] || p=$APP_PASSWORD; echo "host=${IP[cloud]} port=$CLOUD_PORT dbname=$1 user=$u password=$p sslmode=disable"; }
dsn_proxy() { echo "host=${IP[proxy]} port=$PROXY_PORT dbname=$1 user=app password=$APP_PASSWORD sslmode=disable"; }
dsn_org() { echo "host=${IP[org$1]} port=$(org_pg_port "$1") dbname=${2:-postgres} user=postgres password=$PG_PASSWORD sslmode=disable"; }

# psql in the cloud's container: cloud_sql DB 'SQL'
cloud_sql() { on cloud "docker exec -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h 127.0.0.1 -U postgres -d $1 -v ON_ERROR_STOP=1 -qAtc $(printf '%q' "$2")"; }
org_sql() { local n=$1; on "org$n" "docker exec -e PGPASSWORD=$PG_PASSWORD bb-org$n psql -h 127.0.0.1 -p $(org_pg_port "$n") -U postgres -d $2 -v ON_ERROR_STOP=1 -qAtc $(printf '%q' "$3")"; }

# run the lokadb binary of the relay host (control plane, admin commands against the cloud)
lokadb_on() { local n=$1; shift; on "$n" "$BENCH_HOME/bin/lokadb $*"; }

# svc_start HOST UNIT COMMAND...: a transient systemd unit (logs: journalctl -u bb-UNIT)
svc_start() { local h=$1 u=$2; shift 2; on "$h" "systemctl stop bb-$u 2>/dev/null; systemctl reset-failed bb-$u 2>/dev/null; systemd-run -q --unit bb-$u --property=Restart=on-failure --property=LimitNOFILE=1048576 $*"; }
svc_stop() { on "$1" "systemctl stop bb-$2 2>/dev/null; true"; }

# rtt FROM TO [count]: "min/avg/max" ms of ping
rtt() { on "$1" "ping -c ${3:-20} -i 0.2 -q ${IP[$2]} 2>/dev/null | awk -F'[=/ ]+' '/rtt|round-trip/ {print \$(NF-4)\"/\"\$(NF-3)\"/\"\$(NF-2)}'"; }
