#!/usr/bin/env bash
# An organization's database becomes unavailable while everybody works (block 14, the summary scenario): what its own
# users get (errors, time to the error) and what everybody else gets (organizations of the cloud and the other moved ones).
#   bash bench/scenarios/outage.sh INVENTORY
# Two kinds of outage on organization 1, each 30 s of load, the outage from second 20 to second 50 of 80:
#   db_down      its Postgres is stopped (the connection is refused at once)
#   agent_down   its agent is stopped (the tunnel is gone: the relay has nobody to hand the connection to)
#   blackhole    its host drops the packets of its Postgres port (iptables DROP; only where the host is not shared:
#                OUTAGE_BLACKHOLE=1), the worst case: every statement waits for a timeout
# The timeline (raw/outage-<kind>.csv) has, per second and per class (org1, org2.., cloud): operations done, errors,
# mean latency, mean time to the error.
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"
scenario_init outage "${1:?inventory}"
TOTAL_ORGS=${TOTAL_ORGS:-20}
ROWS=${ROWS:-20000}
CLIENTS=${CLIENTS:-32}
KINDS=${KINDS:-"db_down agent_down"}
[ -n "${OUTAGE_BLACKHOLE:-}" ] && KINDS="$KINDS blackhole"
K=${#ORGS[@]}
o1=${ORGS[0]}
put_spec docs "$SPEC_DOCS"
mkdb bench
setup_data bench "$TOTAL_ORGS" "$ROWS"
note "enable: $(enable_db bench docs | tail -1 | cut -c1-120)"
MOVED=()
for o in "${ORGS[@]}"; do note "move-org $o: $(move_db bench docs "$o" | tail -1 | cut -c1-120)"; MOVED+=("$o"); done
proxy_config bench
cloud_sql bench "VACUUM ANALYZE" >/dev/null
verify_begin bench
classes=""; for o in "${ORGS[@]}"; do classes+="$o=moved$o,"; done
for o in $(seq $((K + 1)) "$TOTAL_ORGS"); do classes+="$o=cloud,"; done

for kind in $KINDS; do
  note "== $kind"
  on load "rm -f $BENCH_HOME/outage.timeline; nohup $BENCH_HOME/bin/benchload sql -dsn '$(dsn_proxy bench)' -op read,update -c $CLIENTS -warmup 5s -d 75s \
    -first-org 1 -orgs $TOTAL_ORGS -rows $ROWS -classes '${classes%,}' -timeline $BENCH_HOME/outage.timeline >$BENCH_HOME/outage.out 2>&1 &"
  sleep $((5 + 20))
  note "outage begins $(date +%T)"
  case $kind in
    db_down) on "org$o1" "docker stop -t 5 bb-org$o1" >/dev/null ;;
    agent_down) svc_stop "org$o1" "agent$o1" ;;
    blackhole) on "org$o1" "iptables -I INPUT -p tcp --dport $(org_pg_port "$o1") -j DROP; iptables -I OUTPUT -p tcp --sport $(org_pg_port "$o1") -j DROP" ;;
  esac
  sleep 30
  note "outage ends $(date +%T)"
  case $kind in
    db_down) on "org$o1" "docker start bb-org$o1" >/dev/null ;;
    agent_down) svc_start "org$o1" "agent$o1" "$BENCH_HOME/bin/lokadb agent -relay relay:$RELAY_PORT -ca $BENCH_HOME/certs/ca.crt -state $BENCH_HOME/agent$o1/state -token-file $BENCH_HOME/agent$o1/token -db 127.0.0.1:$(org_pg_port "$o1") -cloud-listen :$(org_agent_port "$o1") -max-conns $ORG_MAX ${AGENT_SESSIONS:+-sessions $AGENT_SESSIONS}" ;;
    blackhole) on "org$o1" "iptables -D INPUT -p tcp --dport $(org_pg_port "$o1") -j DROP; iptables -D OUTPUT -p tcp --sport $(org_pg_port "$o1") -j DROP" ;;
  esac
  sleep 25
  on load "cat $BENCH_HOME/outage.out" | grep '^{' >>"$RAW/outage-$kind.jsonl"
  on load "cat $BENCH_HOME/outage.timeline" >"$RAW/outage-$kind.csv"
  # until the tunnel and the database are back
  for i in $(seq 1 60); do
    on cloud "docker exec -e PGPASSWORD=$PG_PASSWORD bb-cloud psql -h relay -p $(org_endpoint_port "$o1") -U postgres -d postgres -qAtc 'SELECT 1'" >/dev/null 2>&1 && break
    sleep 2
  done
  note "$kind: $(awk -F, 'NR > 1 { ok[$2] += $3; er[$2] += $4 } END { for (c in ok) printf "%s ok=%d err=%d; ", c, ok[c], er[c] }' "$RAW/outage-$kind.csv")"
done
verify_end bench outage
dropdb bench
scenario_done
