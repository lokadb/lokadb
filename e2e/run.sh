#!/usr/bin/env bash
# End-to-end check: three unmodified applications (Laravel, Django, Go/GORM; and goapp's uuid
# mode, uuidapp, an application whose organizations are numbered by uuid) talk to Postgres
# through the LokaDB proxy. Organization 1 ("Org A") is moved into its own
# database; organization 2 stays in the cloud database.
set -uo pipefail
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1
# Two stands can run at once: COMPOSE_PROJECT_NAME names the containers, E2E_NET the subnets
# (10.<E2E_NET>.x.0/24). The second stand: E2E_NET=232 COMPOSE_PROJECT_NAME=lokadb-e2e2.
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-lokadb-e2e}" E2E_NET="${E2E_NET:-231}"
P=$COMPOSE_PROJECT_NAME
# The logs, secrets/, workload/ and specs/ live in this directory: one stand per tree.
here=$(pwd -W 2>/dev/null || pwd)
other=$(docker ps --format '{{.Label "com.docker.compose.project"}} {{.Label "com.docker.compose.project.working_dir"}}' |
  tr '\134' / | awk -v d="$here" -v p="$P" 'tolower($2) == tolower(d) && $1 != p {print $1; exit}')
[ -n "$other" ] && { echo "!! the stand $other runs from this directory; run the second stand from another worktree"; exit 1; }
C="docker compose"
APPS="${*:-laravel django goapp}"
# E2E_ONLY=core|laravel|django|goapp|uuidapp: one part of the run (e2e/vm/e2e-par.sh runs them at once,
# each on a stand of its own); unset: everything, as before. The start (stand, agent, tunnel) is in every part.
part() { [ -z "${E2E_ONLY:-}" ] || [ "$E2E_ONLY" = "$1" ]; }
FAILED=0
# lokadb check reads pg_stat_statements and the workload the proxy records
export CLOUD_PRELOAD=pg_stat_statements LOKADB_RECORD=/workload/workload.json
# BACKLOG block 8, item 11: the application's role reaches the cloud only from the proxy (and
# the second proxy of the TLS check): the cloud's pg_hba (initdb/cloud-hba.sh)
PROXY_IP=10.$E2E_NET.1.250 PROXYTLS_IP=10.$E2E_NET.1.251
export CLOUD_APP_HOSTS="$PROXY_IP/32 $PROXYTLS_IP/32"

log() { printf '\n\033[1m== %s [%s]\033[0m\n' "$*" "$(date +%T)"; }
fail() { echo "!! $*"; FAILED=1; }
cloud_sql() { $C exec -T cloud psql -U postgres -d "$1" -tAc "$2"; }
cust_sql() { $C exec -T customer psql -U postgres -d "$1" -tAc "$2"; }
wait_healthy() {
  for _ in $(seq 1 600); do
    [ "$($C ps --format '{{.Health}}' "$1")" = healthy ] && return 0
    sleep 1
  done
  return 1
}
# wait_tunnel: the customer database answers through relay -> agent
wait_tunnel() {
  for _ in $(seq 1 60); do
    $C exec -T cloud psql "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# app <name> <phase>: the application's own commands, unchanged.
app() {
  local name=$1 phase=$2
  case "$name:$phase" in
    laravel:migrate1) $C run --rm -T laravel migrate --force ;;
    laravel:seed) $C run --rm -T laravel demo:seed ;;
    laravel:migrate2) $C run --rm -T laravel migrate --path=database/migrations_r2 --force ;;
    laravel:*) $C run --rm -T laravel demo:scenario "$phase" ;;
    django:migrate1) $C run --rm -T -e APP_RELEASE=1 django migrate docs 0001 ;;
    django:seed) $C run --rm -T -e APP_RELEASE=1 django demo_seed ;;
    django:r1) $C run --rm -T -e APP_RELEASE=1 django demo_scenario r1 ;;
    django:migrate2) $C run --rm -T -e APP_RELEASE=2 django migrate docs ;;
    django:*) $C run --rm -T -e APP_RELEASE=2 django demo_scenario "$phase" ;;
    goapp:migrate1) $C run --rm -T goapp migrate 001 ;;
    goapp:seed) $C run --rm -T goapp seed ;;
    goapp:migrate2) $C run --rm -T goapp migrate 002 ;;
    goapp:*) $C run --rm -T goapp scenario "$phase" ;;
  esac
}

CP="host=cloud dbname=controlplane user=postgres password=pw sslmode=disable"
lokadb() { $C run --rm -T lokadb "$@"; }
# run_check <log> <args>: lokadb check; the report is printed and kept in <log>,
# CHECK_RC is its exit code (0 ready, 2 needs review, 3 blocked, 4 not supported by lokadb yet)
run_check() {
  local out=$1
  shift
  lokadb check "$@" >"$out" 2>&1
  CHECK_RC=$?
  grep -v "^ Container" "$out"
}
reported() { grep -q "\[$2\]" "$1"; }
# same_discovery <label> <admin dsn> <organizations table> <tenant column>: block 1: -org-table proposes the
# tables the way -tenant-column does on the demo app's real schema (names, tenant columns and parents)
same_discovery() {
  local keep=$CHECK_RC picks='"(name|tenant_column|parent_table|parent_column)": "[A-Za-z_.]*"'
  run_check "discover_$1_column.log" -db "$2" -tenant-column "$4" >/dev/null
  run_check "discover_$1_org.log" -db "$2" -org-table "$3" >/dev/null
  local a b
  a=$(sed -n '/^Suggested spec/,$p' "discover_$1_column.log" | grep -oE "$picks" | tr -d '\r' | sort | paste -sd';')
  b=$(sed -n '/^Suggested spec/,$p' "discover_$1_org.log" | grep -oE "$picks" | tr -d '\r' | sort | paste -sd';')
  if [ -z "$b" ]; then fail "$1: -org-table $3 proposed no tables"
  elif [ "$a" = "$b" ]; then echo "PASS  $1: -org-table $3 proposes the same tables as -tenant-column $4"
  else fail "$1: -org-table $3 proposes [$b], -tenant-column $4 [$a]"; fi
  CHECK_RC=$keep
}
# enroll_only <token file>: a fresh agent (new key, empty state) tries to enroll and exits
enroll_only() {
  $C run --rm -T agent agent -relay relay:7443 -ca /ca/ca.crt -state /tmp/fresh-state \
    -token-file "/secrets/$1" -db customer:5432 -enroll-only 2>&1 | grep -v "^ Container" | sed 's/^time=[^ ]* //' | tail -1
  return "${PIPESTATUS[0]}"
}

log "starting the cloud (Postgres, proxy, relay) and the organization's Postgres"
rm -f secrets/token* check_*.log
rm -rf workload && mkdir -p workload
$C up -d --wait cloud customer proxy relay || exit 1

log "agent enrollment with a one-time token"
lokadb token -cp "$CP" -org 1 -ttl 10m >secrets/token || fail "token"
echo "token issued (shown once, the registry keeps only its sha256): $(cut -c1-8 secrets/token)..."
$C up -d agent || exit 1
if wait_tunnel; then part core && echo "PASS  the agent generated its key, enrolled and opened the tunnel"; else fail "the agent did not connect to the relay"; fi
if part core; then
if enroll_only token; then fail "a used token enrolled again"; else echo "PASS  the same token cannot enroll a second agent"; fi
lokadb token -cp "$CP" -org 1 -ttl 1s >secrets/token-expired
sleep 2
if enroll_only token-expired; then fail "an expired token enrolled"; else echo "PASS  an expired token is refused"; fi
echo "made-up-token" >secrets/token-forged
if enroll_only token-forged; then fail "a forged token enrolled"; else echo "PASS  a forged token is refused"; fi

log "network isolation: the cloud has no way into the organization's network"
for svc in cloud proxy relay; do
  if $C exec -T "$svc" getent hosts customer >/dev/null 2>&1; then
    fail "$svc can resolve the customer database host"
  else
    echo "PASS  $svc cannot reach the customer database host directly"
  fi
done
echo "agent ports published to anybody: $($C port agent 5433 2>/dev/null || echo none)"
$C exec -T cloud psql "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable" -tAc \
  "SELECT 'PASS  through relay -> agent the cloud reaches: ' || current_setting('cluster_name', true) || inet_server_addr()::text || ' (' || version() || ')'" \
  | cut -c1-140 || fail "tunnel"

log "who may use the tunnel: the cloud network only, the cloud database only during a move, keys 0600"
# psql from a throwaway container on one of the stand's networks
probe() { docker run --rm --network "${P}_$1" postgres:18 psql "$2 connect_timeout=5" -tAc "SELECT 1" 2>&1; }
out=$(probe internet "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable")
if [ "$out" = 1 ]; then fail "from the internet network the organization's endpoint answers"; else
  echo "PASS  from outside the cloud network the relay drops connections to the organization's endpoint (org_allow)"; fi
out=$($C exec -T customer psql "host=agent port=5433 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" 2>&1)
case "$out" in
  *"not open to organization 1"*) echo "PASS  no move in progress: the agent's way to the cloud database is closed (cloud_access window)" ;;
  *) fail "the cloud database answers the organization's network outside a move: $out" ;;
esac
# R3-13: a window admits the move's role, to its database, for replication only. Open one by
# hand for a role and database that do not exist, and try other ones through the agent.
cloud_sql controlplane "INSERT INTO lokadb_cp.cloud_access (org, until, role, dbname) VALUES ('1', now() + interval '2 minutes', 'lokadb_repl_probe', 'probe')
  ON CONFLICT (org) DO UPDATE SET until = excluded.until, role = excluded.role, dbname = excluded.dbname" >/dev/null || fail "open a probe window"
sleep 1.5 # past the relay's kept answer
out=$($C exec -T customer psql "host=agent port=5433 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" 2>&1)
case "$out" in
  *"admits only the move's role"*) echo "PASS  in a move's window another role and database are refused by the relay: $(echo "$out" | grep -o 'FATAL.*' | head -1 | cut -c1-120)" ;;
  *) fail "another role through the cloud window: $out" ;;
esac
out=$($C exec -T customer psql "host=agent port=5433 dbname=probe user=lokadb_repl_probe password=x sslmode=disable replication=database" -tAc "SELECT 1" 2>&1)
case "$out" in
  *"admits only the move's role"*) fail "the window's own role was refused by the relay: $out" ;;
  *FATAL*) echo "PASS  the window's role and database get through to the cloud database (it answers: $(echo "$out" | grep -o 'FATAL.*' | head -1 | cut -c1-80))" ;;
  *) fail "the window's role through the cloud window: $out" ;;
esac
cloud_sql controlplane "DELETE FROM lokadb_cp.cloud_access WHERE org = '1'" >/dev/null
out=$(probe custnet "host=agent port=5433 dbname=postgres user=postgres password=pw sslmode=disable")
if [ "$out" = 1 ] || grep -q "not open" <<<"$out"; then fail "another host of the organization's network got through the agent: $out"; else
  echo "PASS  only the organization's database may use the agent's forwarding (-cloud-allow default)"; fi
# BACKLOG block 8, item 11: past the proxy the application's role does not get into the cloud
out=$(probe cloudnet "host=cloud dbname=laravel user=app password=app sslmode=disable")
case "$out" in
  *"pg_hba.conf rejects connection"*|*"no pg_hba.conf entry"*) echo "PASS  the application's role reaches the cloud database only from the proxy (pg_hba): $(echo "$out" | grep -o 'FATAL.*' | head -1 | cut -c1-110)" ;;
  *) fail "the application's role reached the cloud database past the proxy: $out" ;;
esac
modes=$($C run --rm -T --no-deps --entrypoint stat certs -c '%a %n' /certs/ca.key /certs/relay.key 2>/dev/null | tr -d '\r' | tr '\n' ' ')
case "$modes" in
  "600 /certs/ca.key 600 /certs/relay.key ") echo "PASS  the CA key and the relay key are readable by their owner only ($modes)" ;;
  *) fail "key file modes: $modes" ;;
esac

log "compatibility check: a schema with problems (badapp), the organization's database already has a documents table"
bad="host=cloud user=postgres password=pw dbname=badapp sslmode=disable"
run_check check_badapp.log -db "$bad" -spec /specs/badapp.json -org 1 \
  -customer "host=relay port=15001 dbname=cust_badapp user=postgres password=pw sslmode=disable"
# The organization's own documents table is in public: move-org would put its tables in schema lokadb.
[ "$CHECK_RC" = 4 ] && echo "PASS  nothing blocks the migration, lokadb cannot move some tables yet (exit 4)" || fail "badapp: exit $CHECK_RC, want 4 (not supported yet)"
for code in unique_not_tenant_scoped dependents_repointed triggers_carried rls_enabled identity_always orphan_rows id_type child_without_fk \
  customer_schema no_workload grants_carried; do
  reported check_badapp.log $code && echo "PASS  reported: $code" || fail "badapp: $code not reported"
done
# block 4: attachments references documents by (organization_id, id): enable carries such an FK
if reported check_badapp.log fk_multicolumn; then fail "badapp: fk_multicolumn reported for a tenant-scoped FK"
else echo "PASS  the tenant-scoped FK of attachments is not reported as unsupported"; fi
# Only what stops the move whatever lokadb does is a blocker; lokadb's own limitations and behavior changes are separate.
blockers=$(sed -n '/^Blockers/,/^$/p' check_badapp.log | grep -o '^  \[[a-z_]*\]' | tr -d ' []' | sort | xargs)
[ -z "$blockers" ] && echo "PASS  no blockers: the organization's own documents table only sends lokadb's tables to schema lokadb" || fail "badapp blockers: '$blockers'"
run_check check_badapp_public.log -db "$bad" -spec /specs/badapp.json -org 1 -customer-schema public \
  -customer "host=relay port=15001 dbname=cust_badapp user=postgres password=pw sslmode=disable" >/dev/null
[ "$CHECK_RC" = 3 ] && reported check_badapp_public.log customer_table_exists \
  && echo "PASS  with -customer-schema public the organization's documents table blocks it (exit 3, customer_table_exists)" \
  || fail "badapp -customer-schema public: exit $CHECK_RC"
sed -n '/^Not supported by lokadb yet/,/^$/p' check_badapp.log | grep -q '\[id_type\]' && echo "PASS  id_type is reported as a lokadb limitation" || fail "badapp: id_type not under 'Not supported by lokadb yet'"
sed -n '/^Changes after the migration/,/^$/p' check_badapp.log | grep -q '\[unique_not_tenant_scoped\]' && echo "PASS  a global unique is reported as a behavior change" || fail "badapp: unique_not_tenant_scoped not under 'Changes after the migration'"
run_check check_badapp_weak.log -db "$bad" -spec /specs/badapp.json \
  -customer "host=relay port=15001 dbname=cust_badapp user=weak password=weak sslmode=disable" >/dev/null
reported check_badapp_weak.log customer_privileges && echo "PASS  reported: customer_privileges ($(grep -o 'role weak cannot[^.]*' check_badapp_weak.log | cut -c1-90)...)" \
  || fail "badapp: customer_privileges not reported"
run_check check_badapp_discover.log -db "$bad" -tenant-column organization_id >/dev/null
# Discovery leaves out document_versions (no FK), so its orphans too: only lokadb's limitations remain.
[ "$CHECK_RC" = 4 ] && echo "PASS  without real blockers the verdict is NOT SUPPORTED YET (exit 4)" || fail "badapp discovery: exit $CHECK_RC, want 4"
found=$(sed -n '/^Suggested spec/,$p' check_badapp_discover.log | grep -o '"name": "[a-z_]*"' | cut -d'"' -f4 | xargs)
[ "$found" = "attachments documents users" ] && echo "PASS  -tenant-column found the tables: $found" || fail "badapp discovery: '$found'"
same_discovery badapp "$bad" organizations organization_id

log "move-org puts its tables next to a table the organization already has (guardapp)"
guard="host=cloud user=postgres password=pw dbname=guardapp sslmode=disable"
lokadb enable -db "$guard" -spec /specs/guardapp.json -owner app >/dev/null 2>&1 || fail "guardapp enable"
out=$(lokadb move-org -db "$guard" -spec /specs/guardapp.json -owner app -org 1 \
  -customer "host=relay port=15001 dbname=cust_guardapp user=postgres password=pw sslmode=disable" 2>&1)
rc=$?
own=$(cust_sql cust_guardapp "SELECT note FROM public.documents")
moved=$(cust_sql cust_guardapp "SELECT count(*) FROM lokadb.documents" 2>&1)
viacloud=$(cloud_sql guardapp "SELECT count(*) FROM documents WHERE organization_id = 1")
if [ $rc = 0 ] && grep -q "tables go to schema lokadb" <<<"$out" && [ "$own" = "the organization's own table" ] \
  && [ "$moved" = "$viacloud" ] && [ "${moved:-0}" -gt 0 ] 2>/dev/null; then
  echo "PASS  move-org put the organization's tables in schema lokadb ($moved rows, read through the cloud); its own public.documents and its row are intact"
else
  fail "guardapp: move-org exit $rc, the organization's table holds '$own'"
  echo "$out" | tail -5
fi

log "richapp: move-org carries extensions, an enum, a domain, a function index, an ICU collation, CHECK, generated columns and a table without a primary key"
rich="host=cloud user=postgres password=pw dbname=richapp sslmode=disable"
rcust="host=relay port=15001 dbname=cust_richapp user=postgres password=pw sslmode=disable"
run_check check_richapp.log -db "$rich" -spec /specs/richapp.json -customer "$rcust" -org 1 >/dev/null
case $CHECK_RC in
  0 | 2) echo "PASS  richapp: nothing blocks the migration or is unsupported (exit $CHECK_RC)" ;;
  *) fail "richapp: lokadb check exit $CHECK_RC"; grep -E '^  \[' check_richapp.log ;;
esac
reported check_richapp.log customer_creates && echo "PASS  richapp: the report lists what move-org will create in the organization's database" \
  || fail "richapp: customer_creates not reported"
lokadb enable -db "$rich" -spec /specs/richapp.json -owner app >/dev/null 2>&1 || fail "richapp enable"
# The table without a primary key is updated all through the move, while it is published too.
# Through the proxy, as an application writes: it pauses the organization while move-org
# splits it out of DEFAULT (a writer past the proxy would not wait).
for _ in $(seq 1 600); do echo "UPDATE notes SET txt = txt || 'x' WHERE organization_id = 1 AND id = 1; SELECT pg_sleep(0.05);"; done |
  $C exec -T cloud psql "host=proxy port=6432 dbname=richapp user=app password=app sslmode=disable" -q -o /dev/null >rich_updates.log 2>&1 &
upd=$!
sleep 1
lokadb move-org -db "$rich" -spec /specs/richapp.json -owner app -org 1 -customer "$rcust" \
  -customer-app "host=relay port=15001 dbname=cust_richapp user=lokadb_app password=lokadb_app sslmode=disable" \
  -publisher "host=agent port=5433 dbname=richapp user=postgres password=pw sslmode=disable" -cp "$CP" -mode logical -allow-customer-replication >richapp_move.log 2>&1 \
  || { fail "richapp move-org"; grep -v "^ Container" richapp_move.log | tail -5; }
wait $upd
grep "created in the organization's database" richapp_move.log | sed 's/.*sql=/  created: /' | cut -c1-100
stats="SELECT count(*) || ' ' || sum(body_len) || ' ' || sum(words) || ' ' || count(*) FILTER (WHERE status = 'published') FROM documents"
viacloud=$(cloud_sql richapp "$stats WHERE organization_id = 1")
atorg=$(cust_sql cust_richapp "$stats")
[ "$viacloud" = "$atorg" ] && [ "${atorg%% *}" = 100 ] \
  && echo "PASS  richapp: the organization's rows and generated columns match (rows, sum of stored, sum of virtual, published: $atorg)" \
  || fail "richapp: through the cloud '$viacloud', in the organization's database '$atorg'"
errs=$(grep -c ERROR rich_updates.log)
note_c=$(cloud_sql richapp "SELECT length(txt) FROM notes WHERE organization_id = 1 AND id = 1")
note_o=$(cust_sql cust_richapp "SELECT length(txt) FROM notes WHERE id = 1")
[ "$errs" = 0 ] && [ "$note_c" = "$note_o" ] && [ "${note_o:-0}" -gt 1 ] \
  && echo "PASS  richapp: the table without a primary key took every update during the move ($((note_o - 1)) updates, no error)" \
  || { fail "richapp: updates without a primary key: $errs errors, length cloud '$note_c' organization '$note_o'"; grep ERROR rich_updates.log | sort | uniq -c | head -3; }
new=$(cloud_sql richapp "INSERT INTO documents (organization_id, title, body, status, price) VALUES (1, 'New', 'a b c', 'published', 3) RETURNING body_len || ' ' || words" 2>&1 | head -1)
[ "$new" = "5 3" ] && echo "PASS  richapp: a new row of the moved organization gets its generated columns (stored 5, virtual 3)" || fail "richapp: insert after the move: '$new'"
bad=$(cloud_sql richapp "INSERT INTO documents (organization_id, title, body) VALUES (1, '', 'x')" 2>&1)
grep -q documents_title_check <<<"$bad" && grep -q "remote SQL command" <<<"$bad" \
  && echo "PASS  richapp: the organization's database enforces the CHECK constraint" || fail "richapp: CHECK not enforced: '$bad'"
bad=$(cloud_sql richapp "INSERT INTO documents (organization_id, title, price) VALUES (1, 'neg', -1)" 2>&1)
grep -q posint_check <<<"$bad" && echo "PASS  richapp: the domain refuses a negative price" || fail "richapp: domain not enforced: '$bad'"
n=$(cloud_sql richapp "SELECT count(*) FROM documents WHERE organization_id = 1 AND slug = 'SLUG-2'")
[ "$n" = 1 ] && echo "PASS  richapp: citext compares without case in the moved rows" || fail "richapp: citext lookup found '$n'"
shape=$(cust_sql cust_richapp "SELECT (SELECT string_agg(extname, ',' ORDER BY extname) FROM pg_extension WHERE extname IN ('citext', 'pg_trgm'))
  || ' ' || (SELECT attcollation::regcollation::text FROM pg_attribute WHERE attrelid = 'documents'::regclass AND attname = 'name')
  || ' ' || (SELECT string_agg(attname || '=' || attgenerated::text, ',' ORDER BY attname) FROM pg_attribute WHERE attrelid = 'documents'::regclass AND attgenerated <> '')
  || ' ' || (SELECT count(*) FROM pg_indexes WHERE tablename = 'documents' AND indexname IN ('documents_body_trgm', 'documents_norm_title'))")
[ "$shape" = 'citext,pg_trgm "en-x-icu" body_len=s,words=v 2' ] \
  && echo "PASS  richapp: the organization's table keeps the extensions, collation, generated columns and indexes ($shape)" \
  || fail "richapp: the organization's table: '$shape'"

log "lokadb init: the first install's files in one command"
init_out=$($C run --rm -T --entrypoint sh lokadb -c '
adm="host=cloud dbname=postgres user=postgres password=pw sslmode=disable"
lokadb init -dir /tmp/i -relay-host relay -admin "$adm" -cp-role init_cp -cp-db init_cp -cp-sslmode disable >/tmp/o1 2>&1 || { cat /tmp/o1; exit 1; }
sum() { sha256sum /tmp/i/ca.crt /tmp/i/relay/ca.crt /tmp/i/relay/ca.key /tmp/i/relay/relay.crt /tmp/i/credentials.key /tmp/i/control-plane.dsn /tmp/i/relay.json | sha256sum; }
before=$(sum)
lokadb init -dir /tmp/i -relay-host relay -admin "$adm" -cp-role init_cp -cp-db init_cp -cp-sslmode disable >/tmp/o2 2>&1 || { cat /tmp/o2; exit 1; }
[ "$before" = "$(sum)" ] || { echo "a second run rewrote a file"; exit 1; }
pw=$(grep -o "password=.[^ ]*" /tmp/i/control-plane.dsn | head -1 | cut -d"'"'"'" -f2)
{ [ ${#pw} -ge 24 ] && ! grep -qF "$pw" /tmp/o1 /tmp/o2; } || { echo "the password is short or printed"; exit 1; }
[ ! -e /tmp/i/ca.key ] && [ "$(stat -c %a /tmp/i/relay/ca.key)" = 600 ] && [ "$(stat -c %a /tmp/i/control-plane.dsn)" = 600 ] || { echo "modes or the CA key place"; exit 1; }
lokadb token -cp file:/tmp/i/control-plane.dsn -org 9 >/dev/null 2>&1 || { echo "the generated control-plane.dsn does not work"; exit 1; }
echo INIT-OK' 2>&1)
case "$init_out" in
  *INIT-OK*) echo "PASS  lokadb init made the control plane role and database (random password, not printed), the keys (0600, the CA key apart), the certificates and configs; a second run changed nothing; the generated DSN works (lokadb token -cp file:...)" ;;
  *) fail "lokadb init: $init_out" ;;
esac
cloud_sql postgres "DROP DATABASE IF EXISTS init_cp WITH (FORCE)" >/dev/null 2>&1; cloud_sql postgres "DROP ROLE IF EXISTS init_cp" >/dev/null 2>&1
fi # part core

# block 6: what the application sees of its schema (public), to compare before enable and after
# disable: columns, defaults, identity, constraints, indexes, triggers, views, owners, privileges.
catalog() {
  cloud_sql "$1" "SELECT x FROM (
    SELECT 'col ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod) || ' notnull=' || a.attnotnull
           || ' identity=' || a.attidentity::text || ' generated=' || a.attgenerated::text || ' default=' || coalesce(pg_get_expr(d.adbin, d.adrelid), '')
      FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
      LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
      WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p', 'f', 'v', 'm')
    UNION ALL SELECT 'con ' || conrelid::regclass::text || ' ' || conname || ' ' || pg_get_constraintdef(oid) || ' valid=' || convalidated
      FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND conrelid <> 0
    UNION ALL SELECT 'idx ' || pg_get_indexdef(indexrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
      WHERE c.relnamespace = 'public'::regnamespace
    UNION ALL SELECT 'trg ' || pg_get_triggerdef(g.oid) || ' ' || g.tgenabled::text FROM pg_trigger g JOIN pg_class c ON c.oid = g.tgrelid
      WHERE c.relnamespace = 'public'::regnamespace AND NOT g.tgisinternal
    UNION ALL SELECT 'view ' || viewname || ' ' || definition FROM pg_views WHERE schemaname = 'public'
    UNION ALL SELECT 'rel ' || relname || ' ' || relkind::text || ' ' || pg_get_userbyid(relowner) || ' ' || coalesce(relacl::text, '')
      FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'p', 'f', 'v', 'm', 'S')
    UNION ALL SELECT 'schema ' || nspname FROM pg_namespace WHERE nspname = 'lokadb'
  ) s(x)" | LC_ALL=C sort # comm compares in this order
}

# block 11: lokadb check -ci (CI of the application). The baseline files live in workload/ (gitignored,
# emptied every run), mounted writable at /ci only for these runs.
ci_dir="$(pwd -W 2>/dev/null || pwd)/workload"
# run_ci <log> <args>: lokadb check -ci, CHECK_RC is its exit code (0 nothing new, 6 new items)
run_ci() {
  local out=$1
  shift
  $C run --rm -T -v "$ci_dir:/ci" lokadb check "$@" >"$out" 2>&1
  CHECK_RC=$?
}

for name in $APPS; do
  part "$name" || continue
  db=$name
  docs=documents vers=document_versions
  [ "$name" = django ] && docs=docs_document vers=docs_documentversion
  admin="host=cloud user=postgres password=pw dbname=$db sslmode=disable"
  # the organization's database as the cloud reaches it: only through the relay
  customer="host=relay port=15001 dbname=cust_$db user=postgres password=pw sslmode=disable"
  # the cloud database as the organization's database reaches it (subscription): through the agent
  publisher="host=agent port=5433 dbname=$db user=postgres password=pw sslmode=disable"
  # the unprivileged role the cloud keeps there afterwards (owns the moved tables)
  customer_app="host=relay port=15001 dbname=cust_$db user=lokadb_app password=lokadb_app sslmode=disable"

  log "$name: release 1 migrations + seed (through the proxy, LokaDB not enabled yet)"
  app "$name" migrate1 >/dev/null || fail "$name migrate1"
  app "$name" seed || fail "$name seed"

  log "$name: compatibility check before enable (schema, pg_stat_statements, the proxy's recording of the steps above)"
  sleep 3 # the proxy writes its recording every 2 s
  run_check "check_$name.log" -db "$admin" -spec "/specs/$name.json" -workload /workload/workload.json -proxy-config /etc/lokadb.json -customer "$customer" -org 1 \
    -proxy-hosts "$PROXY_IP,$PROXYTLS_IP"
  if grep -q "cloud_direct_access" "check_$name.log"; then fail "$name: check finds the application's role open past the proxy: $(grep -A2 cloud_direct_access "check_$name.log" | head -3)"; else
    echo "PASS  $name: check finds the cloud's pg_hba admits the application's role from the proxies only (cloud_direct_access)"; fi
  case "$name" in django) same_discovery "$name" "$admin" docs_organization organization_id ;; *) same_discovery "$name" "$admin" organizations organization_id ;; esac
  case $CHECK_RC in
    0 | 2) echo "PASS  $name: nothing blocks the migration (exit $CHECK_RC)" ;;
    *) fail "$name: lokadb check exit $CHECK_RC" ;;
  esac

  log "$name: lokadb enable"
  catalog "$db" >"catalog_before_$name.txt" # block 6: compared after disable
  $C run --rm -T lokadb enable -db "$admin" -spec "/specs/$name.json" -owner app || fail "$name enable"

  log "$name: move organization 1 into its own database while both organizations keep writing"
  $C run --rm -T loadgen -dsn "host=proxy port=6432 dbname=$db user=app password=app sslmode=disable" \
    -docs "$docs" -versions "$vers" -duration 600s -until-moved 1 -bulk "${BULK:-20000}" ${MEASURE_ONLY:+-keep} >"load_$name.log" 2>&1 &
  load=$!
  # start the move once the bulk rows are in and the steady write loop is running
  for _ in $(seq 1 600); do grep -qE '^(bulk:|LOAD)' "load_$name.log" 2>/dev/null && break; sleep 1; done
  sleep 2
  # -mode auto is change capture by triggers in the cloud (no subscription, no replication slot, no
  # role that reads the cloud's change stream: K-1); goapp names it. django moves by logical
  # replication, which has to be asked for (-allow-customer-replication): the subscription over
  # the tunnel stays covered.
  mode=auto want=trigger flag=
  [ "$name" = goapp ] && mode=trigger
  [ "$name" = django ] && mode=logical want=logical flag=-allow-customer-replication
  timeout 1200 $C run --rm -T lokadb move-org -db "$admin" -spec "/specs/$name.json" -owner app -org 1 -customer "$customer" -customer-app "$customer_app" -publisher "$publisher" -cp "$CP" -mode "$mode" $flag 2>&1 | tee "move_$name.log" | grep -v "^ Container"
  [ "${PIPESTATUS[0]}" = 0 ] || fail "$name move-org"
  grep -q "msg=\"move mode\" org=1 mode=$want" "move_$name.log"     && echo "PASS  $name moved in $([ $want = trigger ] && echo 'trigger mode (no subscription, no replication slot)' || echo 'logical mode (-allow-customer-replication)')"     || fail "$name: move mode is not $want"
  wait $load || fail "$name: writes during the move"
  grep -v "^ Container" "load_$name.log"
  [ -n "${MEASURE_ONLY:-}" ] && continue # only the move under load was measured

  # block 10, stage 8a: organization 1 goes the direct path from here on (tasks/direct-path.md)
  out=$(lokadb org-profile -db "$admin" -org 1 -customer-app "$customer_app" -direct-role lokadb_dml fdw+direct 2>&1)
  [ $? = 0 ] && [ "$(cloud_sql "$db" "SELECT profile FROM lokadb.orgs WHERE org = 1")" = fdw+direct ] \
    && echo "PASS  $name: org-profile set organization 1 to fdw+direct (its reads and UPDATEs go to its database straight)" \
    || fail "$name: org-profile fdw+direct: $(tail -2 <<<"$out")"
  direct_since=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  log "$name: scenario r1 (same application code, same connection string)"
  app "$name" r1 || fail "$name r1"

  log "$name: release 2 migration (adds documents.status) + scenario r2"
  app "$name" migrate2 || fail "$name migrate2"
  app "$name" r2 || fail "$name r2"

  log "$name: where do the rows physically live?"
  cust=$(cust_sql "cust_$db" "SELECT count(*) FILTER (WHERE organization_id = 1) || ' ' || count(*) FILTER (WHERE organization_id <> 1) || ' ' || count(*) FILTER (WHERE status = 'published') FROM $docs")
  custv=$(cust_sql "cust_$db" "SELECT count(*) FILTER (WHERE organization_id = 1) || ' ' || count(*) FILTER (WHERE organization_id <> 1) FROM $vers")
  # every plain table in the cloud database that holds rows of these tables: the local
  # partitions still attached, plus anything left over from the move (*__o1*)
  local_rows() {
    cloud_sql "$db" "SELECT string_agg(format('SELECT organization_id, %s FROM %I', '$2', c.relname), ' UNION ALL ')
      FROM pg_class c WHERE c.relkind = 'r' AND c.relnamespace = 'public'::regnamespace
      AND (c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = '$1'::regclass) OR c.relname LIKE '$1\_\_o1%')"
  }
  docs_union=$(local_rows "$docs" body)
  vers_union=$(local_rows "$vers" NULL) # '' would land inside the format() literal as a lone quote
  cloud=$(cloud_sql "$db" "SELECT count(*) FILTER (WHERE organization_id = 1) || ' ' || count(*) FILTER (WHERE organization_id = 2) || ' ' || count(*) FILTER (WHERE body LIKE 'secret A%') FROM ($docs_union) t(organization_id, body)")
  cloudv=$(cloud_sql "$db" "SELECT count(*) FILTER (WHERE organization_id = 1) FROM ($vers_union) t(organization_id, x)")
  parts=$(cloud_sql "$db" "SELECT string_agg(c.relname || ':' || c.relkind::text, ' ' ORDER BY c.relname) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = '$docs'::regclass")
  read -r c1 cother cpub <<<"$cust"
  read -r v1 vother <<<"$custv"
  read -r k1 k2 ksecret <<<"$cloud"
  echo "customer DB cust_$db: $docs org1=$c1 other=$cother published=$cpub | $vers org1=$v1 other=$vother"
  echo "cloud DB $db:        local tables org1=$k1 org2=$k2 'secret A' bodies=$ksecret | $vers org1=$cloudv"
  echo "partitions of $docs: $parts   (r = local table, f = foreign table)"
  [ "$c1" -gt 0 ] && [ "$cother" = 0 ] && [ "$cpub" = 1 ] && [ "$v1" -gt 0 ] && [ "$vother" = 0 ] \
    && [ "$k1" = 0 ] && [ "$k2" -gt 0 ] && [ "$ksecret" = 0 ] && [ "$cloudv" = 0 ] \
    && echo "PASS  org 1 rows exist only in its own database; cloud keeps only org 2" \
    || fail "$name: placement check"
  # block 17: goapp's tables keyed by a uuid the cloud makes (shares; share_visits under them by
  # a uuid FK): 2 shares per organization seeded with a visit each, one more of each in r1
  if [ "$name" = goapp ]; then
    sc=$(cust_sql cust_goapp "SELECT (SELECT count(*) FILTER (WHERE organization_id = 1) || '/' || count(*) FILTER (WHERE organization_id <> 1) FROM shares) || ' ' || (SELECT count(*) FROM share_visits)")
    sk=$(cloud_sql goapp "SELECT (SELECT count(*) FILTER (WHERE organization_id = 1) || '/' || count(*) FILTER (WHERE organization_id = 2) FROM shares__cloud)
      || ' ' || (SELECT count(*) FROM share_visits__cloud WHERE organization_id = 1)
      || ' ' || (SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'shares__refs'::regclass AND attname = 'id')
      || ' ' || (SELECT count(*) FROM shares__refs WHERE tenant = 1)")
    [ "$sc" = "3/0 3" ] && [ "$sk" = "0/3 0 uuid 3" ] \
      && echo "PASS  goapp: shares (uuid key) and share_visits (uuid FK) of org 1 live only in its database ($sc); the cloud's refs table is keyed by uuid ($sk)" \
      || fail "goapp: uuid-keyed shares: in the organization's database '$sc', in the cloud '$sk'"
  fi

  log "$name: organization 1's agent goes offline (tunnel down)"
  $C stop agent >/dev/null
  app "$name" offline || fail "$name offline (agent down)"
  $C start agent >/dev/null && wait_tunnel && echo "PASS  the restarted agent reconnected with its stored certificate (no new token)" \
    || fail "the agent did not reconnect"

  log "$name: organization 1's database goes offline (agent up)"
  $C stop -t 60 customer >/dev/null
  app "$name" offline || fail "$name offline (database down)"
  $C start customer >/dev/null && wait_healthy customer && wait_tunnel || fail "customer did not come back"

  log "$name: compatibility check of everything this run sent (LokaDB enabled, organization 1 moved)"
  sleep 3
  run_check "check_${name}_after.log" -db "$admin" -spec "/specs/$name.json" -workload /workload/workload.json -proxy-config /etc/lokadb.json
  case $CHECK_RC in
    0 | 2) echo "PASS  $name: the recorded workload of the whole run was checked (exit $CHECK_RC)" ;;
    *) fail "$name: lokadb check after the run exit $CHECK_RC" ;;
  esac

  # block 11: the whole run accepted as the baseline; the same workload against it is clean
  args=(-db "$admin" -spec "/specs/$name.json" -workload /workload/workload.json -proxy-config /etc/lokadb.json)
  run_ci "check_${name}_ci_write.log" "${args[@]}" -ci -write-baseline "/ci/ci-$name.json"
  accepted=$(grep -c '"key"' "workload/ci-$name.json" 2>/dev/null)
  run_ci "check_${name}_ci.log" "${args[@]}" -ci -baseline "/ci/ci-$name.json"
  if [ "$CHECK_RC" = 0 ] && [ "${accepted:-0}" -gt 0 ] && grep -q "lokadb check -ci: OK, nothing new" "check_${name}_ci.log"; then
    echo "PASS  $name: check -ci passes against the baseline of this run ($accepted accepted items)"
  else
    fail "$name: check -ci against its own baseline exit $CHECK_RC ($accepted accepted)"; grep -v "^ Container" "check_${name}_ci.log" | head -20
  fi
  # block 11, item 6: check -rtt, the estimated latency of the recorded transactions of a moved organization
  run_ci "check_${name}_rtt.log" "${args[@]}" -rtt 20
  shapes=$(grep -cE '^ +[0-9.]+ ms +[0-9.]+ ms +[0-9.]+ ms +x' "check_${name}_rtt.log")
  if { [ "$CHECK_RC" = 0 ] || [ "$CHECK_RC" = 2 ]; } && grep -q "Latency of a moved organization at RTT 20 ms (estimate" "check_${name}_rtt.log" \
    && reported "check_${name}_rtt.log" latency_estimate && [ "$shapes" -gt 0 ]; then
    echo "PASS  $name: check -rtt 20 estimates the recorded transactions ($(grep -m1 'all recorded transactions' "check_${name}_rtt.log" | sed 's/^ *//'))"
  else
    fail "$name: check -rtt 20 exit $CHECK_RC"; grep -v "^ Container" "check_${name}_rtt.log" | head -20
  fi
  # block 10: routing profiles (tasks/routing-declarations.md, stage 7)
  log "$name: routing profiles: the share of the direct path (check -profile), org-profile under the organization's pause"
  run_check "check_${name}_profile.log" -db "$admin" -spec "/specs/$name.json" -workload /workload/workload.json -proxy-config /etc/lokadb.json -profile fdw+direct -org 1 >/dev/null
  share=$(grep -o '\[profile_direct_share\] profile fdw+direct: [0-9]* of [0-9]* query patterns[^(]*([0-9.]*% of [0-9]* executions)' "check_${name}_profile.log")
  if [[ $CHECK_RC =~ ^[02]$ ]] && [ -n "$share" ] && ! grep -q '\[profile_direct_share\] profile fdw+direct: 0 of' "check_${name}_profile.log"; then
    echo "PASS  $name: check -profile fdw+direct estimates the direct path: ${share#*: }"
    grep -E '^  \[profile_(fdw_reason|two_connections|fdw_after_direct|fdw_after_cloud|insert_cloud_id)\]' "check_${name}_profile.log" | cut -c1-200
  else
    fail "$name: check -profile fdw+direct (exit $CHECK_RC): $(grep -E '\[profile_|error' "check_${name}_profile.log" | head -3)"
  fi
  run_check "check_${name}_profile_direct.log" -db "$admin" -spec "/specs/$name.json" -workload /workload/workload.json -proxy-config /etc/lokadb.json -profile direct -org 1 >/dev/null
  [ "$CHECK_RC" = 3 ] && reported "check_${name}_profile_direct.log" profile_direct_unnamed_read \
    && echo "PASS  $name: check -profile direct blocks reads that do not name the organization (no lokadb.tenant yet)" \
    || fail "$name: check -profile direct: exit $CHECK_RC, profile_direct_unnamed_read not reported"
  # the statements of organization 1 since org-profile fdw+direct: how many went straight
  plog=$($C logs --no-color --since "$direct_since" proxy 2>&1 | grep 'msg=route' | grep "db=$db" | grep 'to="tenant 1"')
  ndirect=$(grep -c 'path=direct' <<<"$plog")
  nroute=$(grep -c . <<<"$plog")
  [ "${ndirect:-0}" -gt 0 ] \
    && echo "PASS  $name: $ndirect of $nroute statements of organization 1 went the direct path (proxy log path=direct; check -profile estimated ${share#*: })" \
    || fail "$name: no statement of organization 1 went the direct path ($nroute routed)"
  out=$(lokadb org-profile -db "$admin" -org 2 fdw 2>&1)
  [ $? = 1 ] && grep -q "not in its own database" <<<"$out" \
    && echo "PASS  $name: org-profile refuses an organization that is not moved" || fail "$name: org-profile of org 2: $out"
  # an open transaction of organization 1 through the proxy holds its pause lock shared:
  # org-profile waits for it to finish, then switches
  $C exec -T cloud psql -q "host=proxy port=6432 dbname=$db user=app password=app sslmode=disable" >/dev/null 2>&1 <<SQL &
BEGIN;
SELECT count(*) FROM $docs WHERE organization_id = 1;
-- hold the lock until org-profile waits for it (an advisory lock not granted), then a second more
DO \$\$BEGIN
  FOR i IN 1..600 LOOP
    EXIT WHEN EXISTS (SELECT FROM pg_locks WHERE locktype = 'advisory' AND NOT granted);
    PERFORM pg_sleep(0.1);
  END LOOP;
  PERFORM pg_sleep(1);
END\$\$;
COMMIT;
SQL
  holder=$!
  # the holder's transaction has taken the organization's lock (shared) before the switch starts
  for _ in $(seq 1 20); do
    [ "$(cloud_sql "$db" "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND mode = 'ShareLock' AND granted")" -gt 0 ] && break
    sleep 0.5
  done
  locks=$(cloud_sql "$db" "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND mode = 'ShareLock' AND granted")
  out=$(lokadb org-profile -db "$admin" -org 1 fdw 2>&1)
  rc=$?
  wait $holder
  held=$?
  prof=$(cloud_sql "$db" "SELECT profile FROM lokadb.orgs WHERE org = 1")
  # attempt > 1: the pause had to wait for the open transaction (each wait is under deadlock_timeout)
  attempt=$(grep -o 'organization paused.*attempt=[0-9]*' <<<"$out" | grep -o '[0-9]*$')
  if [ $rc = 0 ] && [ $held = 0 ] && [ "$prof" = fdw ] && [ "${attempt:-0}" -gt 1 ]; then
    echo "PASS  $name: org-profile waited for the organization's open transaction (paused at attempt $attempt; that transaction committed) and set lokadb.orgs.profile = fdw"
  else
    fail "$name: org-profile fdw: rc=$rc holder=$held profile='$prof' attempt='$attempt' shared advisory locks before='$locks': $(tail -2 <<<"$out")"
  fi
  # block 10, stage 5: the cooperative mode, goapp with its one-line middleware
  if [ "$name" = goapp ]; then
    log "goapp: the cooperative mode (set_config('lokadb.tenant', …, true) in its transaction wrapper)"
    app goapp coop || fail "goapp: cooperative mode"
  fi
done

if part goapp && [[ " $APPS " == *" goapp "* ]]; then
  if [ "${E2E_ONLY:-}" = goapp ]; then # guardapp (log mode) below: enabled and moved by the core part in a full run
    lokadb enable -db "host=cloud user=postgres password=pw dbname=guardapp sslmode=disable" -spec /specs/guardapp.json -owner app >/dev/null 2>&1 || fail "guardapp enable"
    lokadb move-org -db "host=cloud user=postgres password=pw dbname=guardapp sslmode=disable" -spec /specs/guardapp.json -owner app -org 1 \
      -customer "host=relay port=15001 dbname=cust_guardapp user=postgres password=pw sslmode=disable" >/dev/null 2>&1 || fail "guardapp move-org"
  fi
  log "fanout policy: goapp refuses statements that name no organization (reject), guardapp only logs them (log)"
  out=$($C exec -T cloud psql "host=proxy port=6432 dbname=goapp user=app password=app sslmode=disable" 2>&1 <<'SQL'
SELECT count(*) FROM documents WHERE title LIKE 'fanout-%';
SELECT count(*) FROM documents WHERE title LIKE $1 \bind 'fanout-%' \g
UPDATE documents SET title = title WHERE title LIKE 'fanout-%';
BEGIN;
SELECT count(*) FROM documents WHERE title LIKE 'fanout-%';
SELECT 1 AS after_refusal;
ROLLBACK;
SELECT * FROM documents WHERE slug = 'no-such-slug' ORDER BY documents.id LIMIT 1;
SELECT count(*) AS by_ids FROM documents WHERE id = ANY($1) \bind '{1,2,3}' \g
SELECT count(*) FROM documents WHERE id = ANY($1) \bind '{1,NULL}' \g
SELECT count(*) FROM documents__refs;
SQL
)
  refusals=$(grep -c "ERROR:  lokadb: the statement does not name the organization of documents rows" <<<"$out")
  [ "$refusals" = 5 ] && echo "PASS  refused with the proxy's error: a read (simple and extended protocol), a write, a read inside BEGIN, id = ANY with a NULL" \
    || { fail "goapp: $refusals refusals, want 5"; echo "$out"; }
  grep -q 'by_ids' <<<"$out" && echo "PASS  id = ANY(\$1) with a text array parameter runs" || fail "goapp: id = ANY(\$1) did not run"
  grep -q 'ERROR:  lokadb: documents__refs is a partition or helper table of documents' <<<"$out" \
    && echo "PASS  a query on LokaDB's own table (documents__refs) is refused whatever the fanout policy" || fail "goapp: documents__refs was not refused"
  grep -q 'HINT:  Restrict it by the organization column' <<<"$out" && echo "PASS  the error carries a HINT on what to do" || fail "goapp: no HINT"
  grep -q 'current transaction is aborted' <<<"$out" && echo "PASS  inside BEGIN the refusal aborts the transaction like any server error" \
    || fail "goapp: the transaction was not aborted"
  grep -qE '^\(0 rows\)' <<<"$out" && echo "PASS  a statement on the allow list runs (lookup by slug)" || fail "goapp: the allowed lookup did not run"
  n=$($C exec -T cloud psql "host=proxy port=6432 dbname=guardapp user=app password=app sslmode=disable" -tAc "SELECT count(*) FROM documents WHERE title LIKE '%'" 2>&1)
  sleep 1
  if [ "$n" = 2 ] && $C logs --no-color proxy 2>&1 | grep "runs in every organization's database" | grep -q "db=guardapp"; then
    echo "PASS  guardapp (mode log): the same kind of statement runs and the proxy logs a warning"
  else
    fail "guardapp log mode: '$n'"
  fi
  sleep 3 # the recording
  run_check check_goapp_refused.log -db "host=cloud user=postgres password=pw dbname=goapp sslmode=disable" -spec /specs/goapp.json \
    -workload /workload/workload.json -proxy-config /etc/lokadb.json >/dev/null
  if [ "$CHECK_RC" = 3 ] && reported check_goapp_refused.log query_refused && grep -q "title LIKE" check_goapp_refused.log; then
    echo "PASS  lokadb check reports the refused statements as blockers (exit 3)"
  else
    fail "check after refusals: exit $CHECK_RC"
  fi
  reported check_goapp_refused.log query_forbidden && grep -q 'documents__refs' check_goapp_refused.log \
    && echo "PASS  lokadb check reports the query on documents__refs as a blocker (query_forbidden)" || fail "check: query_forbidden not reported"

  # block 11: the refused statements were not in the baseline made before them: -ci fails on them
  run_ci check_goapp_ci_new.log -db "host=cloud user=postgres password=pw dbname=goapp sslmode=disable" -spec /specs/goapp.json \
    -workload /workload/workload.json -proxy-config /etc/lokadb.json -ci -baseline /ci/ci-goapp.json
  if [ "$CHECK_RC" = 6 ] && grep -q "FAILED" check_goapp_ci_new.log && grep -A1 '\[query_refused\]' check_goapp_ci_new.log | grep -q "title LIKE" \
    && grep -q '"allow"' check_goapp_ci_new.log && ! grep -q 'no longer found' check_goapp_ci_new.log; then
    echo "PASS  check -ci fails (exit 6) on the new refused pattern (title LIKE) with a hint, nothing accepted went stale"
  else
    fail "check -ci on the new refused patterns: exit $CHECK_RC"; grep -v "^ Container" check_goapp_ci_new.log | head -30
  fi
  run_ci check_goapp_ci_json.log -db "host=cloud user=postgres password=pw dbname=goapp sslmode=disable" -spec /specs/goapp.json \
    -workload /workload/workload.json -proxy-config /etc/lokadb.json -ci -baseline /ci/ci-goapp.json -json
  if grep -q '"schema_version": 1' check_goapp_ci_json.log && grep -q '"key": "query_refused:fp:[0-9a-f]\{16\}"' check_goapp_ci_json.log; then
    echo "PASS  check -ci -json: schema_version 1, the new items keyed by pg_query fingerprint"
  else
    fail "check -ci -json: no schema_version or fingerprint keys"
  fi
fi

if part core; then
log "certificate renewal (certificates live 2 minutes in this stand)"
lokadb agents -cp "$CP" | head -8
# a part runs shorter than a certificate lives: wait for its first renewal
[ -n "${E2E_ONLY:-}" ] && for _ in $(seq 1 36); do [ "$(lokadb agents -cp "$CP" | grep -c " renew ")" -ge 1 ] && break; sleep 5; done
renewed=$(lokadb agents -cp "$CP" | grep -c " renew ")
if [ "$renewed" -ge 1 ] && wait_tunnel; then
  echo "PASS  the agent renewed its certificate $renewed time(s) on its own; the tunnel stayed up"
else
  fail "no renewal seen"
fi

log "revocation"
lokadb token -cp "$CP" -org 1 -ttl 10m >secrets/token-pending # issued, not used yet
lokadb revoke -cp "$CP" -org 1
revoked_err=""
# Until the relay refuses for lack of the agent: an attempt while it closes the session may
# fail otherwise (the connection cut), and a renewal right before the revocation is refused too.
for _ in $(seq 1 20); do
  revoked_err=$($C exec -T cloud psql "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" 2>&1)     && revoked_err="" # still answered
  echo "$revoked_err" | grep -q "not connected" && break
  sleep 0.5
done
if echo "$revoked_err" | grep -q "not connected"; then
  echo "PASS  the relay dropped the revoked agent's live session: $(echo "$revoked_err" | grep -o 'FATAL.*')"
else
  fail "the revoked agent is still connected"
fi
$C restart agent >/dev/null
sleep 5
if $C exec -T cloud psql "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" >/dev/null 2>&1; then
  fail "the agent reconnected with a revoked certificate"
else
  echo "PASS  restarted with its revoked certificate, the agent is refused ($($C logs --no-color relay 2>&1 | grep -o 'agent rejected: certificate revoked or not issued by enrollment' | tail -1))"
fi
if enroll_only token-pending; then fail "a token issued before revocation still enrolled"; else echo "PASS  revocation burned the organization's unused tokens"; fi

log "re-enrollment with a new token"
lokadb token -cp "$CP" -org 1 -ttl 10m >secrets/token
$C restart agent >/dev/null
wait_tunnel && echo "PASS  a new token brought organization 1 back (new key, new certificate)" || fail "re-enrollment"
lokadb agents -cp "$CP" | head -4
fi # part core

if part goapp; then
# Block 8, 1.5: a new organization joins the running relay while organization 1 works
# through it (the proxy load and one long-lived connection); nobody is disconnected.
log "a new organization (3) joins the running relay under load"
docker rm -f "$P-agent3" >/dev/null 2>&1
relay_id=$($C ps -q relay)
relay_started=$(docker inspect -f '{{.State.StartedAt}} {{.RestartCount}}' "$relay_id")
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
lokadb token -cp "$CP" -org 3 -ttl 10m >secrets/token-org3
# S8: organization 3's database offers TLS (the image's snakeoil certificate, on a reload)
for q in "ALTER SYSTEM SET ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'"   "ALTER SYSTEM SET ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'" "ALTER SYSTEM SET ssl = on" "SELECT pg_reload_conf()"; do
  cust_sql postgres "$q" >/dev/null || fail "customer TLS: $q"
done
if enroll_only token-org3 >/dev/null; then
  fail "a token enrolled for an organization the relay does not serve"
elif $C logs --no-color --since "$since" relay 2>&1 | grep -q 'organization not served by this relay, token kept.*org=3'; then
  echo "PASS  before its endpoint exists, organization 3 is refused and its token is kept (the same token enrolls below)"
else
  fail "enrollment for an organization the relay does not serve: no 'token kept' in the relay log"
fi
$C run --rm -T loadgen -dsn "host=proxy port=6432 dbname=$db user=app password=app sslmode=disable" \
  -docs "$docs" -versions "$vers" -duration 30s >load_neworg.log 2>&1 &
load=$!
# one connection of organization 1 kept open through the whole change
{ for _ in $(seq 1 30); do echo "SELECT pg_sleep(1);"; done; echo "SELECT 'still connected';"; } |
  $C exec -T cloud psql "host=relay port=15001 dbname=postgres user=postgres password=pw sslmode=disable" -tA -v ON_ERROR_STOP=1 -f - >long_org1.log 2>&1 &
long=$!
sleep 5
lokadb org-endpoint -cp "$CP" -org 3 -listen :15003 -tls require | grep -v "^ Container"
$C run -d --no-deps --name "$P-agent3" agent agent -relay relay:7443 -ca /ca/ca.crt -state /tmp/state3 \
  -token-file /secrets/token-org3 -db customer:5432 >/dev/null
org3=""
for _ in $(seq 1 30); do
  org3=$($C exec -T cloud psql "host=relay port=15003 dbname=postgres user=postgres password=pw sslmode=require" -tAc     "SELECT 'org3 ' || inet_server_addr() || ' ssl=' || ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()" 2>/dev/null) && break
  sleep 1
done
case "$org3" in
  "org3 "*) echo "PASS  organization 3 got its relay port and its agent connected with the same token, no relay restart ($org3)" ;;
  *) fail "organization 3 is not reachable through relay:15003"
     echo "--- agent3 ($(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}}' "$P-agent3" 2>&1)):"; docker logs --timestamps "$P-agent3" 2>&1 | tail -30
     echo "--- relay since $since:"; $C logs --no-color --timestamps --since "$since" relay 2>&1 | tail -40 ;;
esac
case "$org3" in
  *"ssl=true") echo "PASS  organization 3 is reached with TLS end to end: the relay forwards only ciphertext (org-endpoint -tls require)" ;;
  *) fail "organization 3 without TLS: $org3" ;;
esac
out=$($C exec -T cloud psql "host=relay port=15003 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" 2>&1)
case "$out" in
  *"accepts only TLS"*) echo "PASS  a connection in clear to organization 3 is refused by the relay: $(echo "$out" | grep -o 'FATAL.*' | head -1)" ;;
  *) fail "clear connection to organization 3: $out" ;;
esac
wait $long && grep -q "still connected" long_org1.log && echo "PASS  organization 1's long-lived connection through the relay survived" \
  || fail "organization 1's long-lived connection broke: $(tail -2 long_org1.log)"
wait $load && echo "PASS  organization 1 and 2 kept writing through the proxy: $(grep '^load:' load_neworg.log)" \
  || fail "writes while organization 3 joined: $(grep -v '^ Container' load_neworg.log | tail -3)"
drops=$($C logs --no-color --since "$since" relay 2>&1 | grep -c 'agent disconnected.*org=1')
[ "$(docker inspect -f '{{.State.StartedAt}} {{.RestartCount}}' "$relay_id")" = "$relay_started" ] && [ "$drops" = 0 ] \
  && echo "PASS  the relay was not restarted and organization 1's agent never disconnected" \
  || fail "relay restarted or organization 1 disconnected ($drops)"
lokadb org-endpoint -cp "$CP" -org 3 -remove | grep -v "^ Container"
gone=""
for _ in $(seq 1 20); do
  $C exec -T cloud psql "host=relay port=15003 dbname=postgres user=postgres password=pw sslmode=disable" -tAc "SELECT 1" >/dev/null 2>&1 || { gone=1; break; }
  sleep 0.5
done
[ -n "$gone" ] && wait_tunnel && echo "PASS  removing organization 3 closed its port; organization 1 still answers" \
  || fail "removing organization 3"
docker rm -f "$P-agent3" >/dev/null 2>&1

# Block 8, S8: TLS between the applications and the proxy. A second proxy, same config plus
# "tls" with require, and a certificate of its own made in its container.
log "TLS between the applications and the proxy (tls.require)"
docker rm -f "$P-proxytls" >/dev/null 2>&1
# its own fixed address (the cloud lets the application's role in from the proxies only)
docker run -d --name "$P-proxytls" --network "${P}_cloudnet" --ip "$PROXYTLS_IP" \
  -v "$(pwd -W 2>/dev/null || pwd)/proxy.json:/etc/lokadb.json:ro" --entrypoint sh "${E2E_IMAGES:-$P}-proxy" -c \
  'lokadb certs -dir /tmp/c -relay-host '"$P-proxytls"' >/dev/null && sed "s#\"listen\": \":6432\",#\"listen\": \":6432\", \"tls\": {\"cert\": \"/tmp/c/relay.crt\", \"key\": \"/tmp/c/relay.key\", \"require\": true},#" /etc/lokadb.json >/tmp/p.json && exec lokadb proxy -config /tmp/p.json' >/dev/null
tls_ok=""
for _ in $(seq 1 30); do
  docker exec "$P-proxytls" cat /tmp/c/ca.crt 2>/dev/null | $C exec -T cloud sh -c 'cat >/tmp/proxy-ca.crt' &&
    tls_ok=$($C exec -T cloud psql "host=$P-proxytls port=6432 dbname=$db user=app password=app sslmode=verify-full sslrootcert=/tmp/proxy-ca.crt" -tAc \
      "SELECT 'org 1 documents: ' || count(*) FROM $docs WHERE organization_id = 1" 2>/dev/null) && break
  sleep 1
done
case "$tls_ok" in
  "org 1 documents: "*) echo "PASS  the application reaches organization 1 through the proxy over TLS (sslmode=verify-full): $tls_ok" ;;
  *) fail "TLS to the proxy: $(docker logs "$P-proxytls" 2>&1 | tail -3)" ;;
esac
out=$($C exec -T cloud psql "host=$P-proxytls port=6432 dbname=$db user=app password=app sslmode=disable" -tAc "SELECT 1" 2>&1)
case "$out" in
  *"accepts only TLS"*) echo "PASS  with tls.require the proxy refuses a client in clear before its password: $(echo "$out" | grep -o 'FATAL.*' | head -1)" ;;
  *) fail "clear connection to a proxy with tls.require: $out" ;;
esac
docker rm -f "$P-proxytls" >/dev/null 2>&1

log "statistics for the cloud service: the proxy's /metrics and the relay's /sessions (tasks/monitoring-plan.md)"
# a GET over bash's /dev/tcp from the cloud database's container (the proxy image has no curl)
http_get() { $C exec -T cloud bash -c "exec 3<>/dev/tcp/$1/$2; printf 'GET $3 HTTP/1.0
Host: x

' >&3; cat <&3" 2>&1; }
met=$(http_get proxy 7447 /metrics)
n=$(echo "$met" | sed -n 's/^lokadb_proxy_statements_total{org="1",path="[a-z]*"} //p' | awk '{s+=$1} END{print s+0}')
c=$(echo "$met" | sed -n 's/^lokadb_proxy_response_seconds_count{org="1"} //p')
if [ "${n:-0}" -gt 0 ] && [ "${c:-0}" -gt 0 ]; then
  echo "PASS  the proxy counts organization 1's statements ($n) and responses ($c) at /metrics"
else fail "proxy /metrics for organization 1: statements=$n responses=$c: $(echo "$met" | head -5)"; fi
if echo "$met" | grep -qiE 'select|insert|documents|slug'; then fail "the proxy's /metrics holds statement text or a table name"
else echo "PASS  /metrics holds no statement text, table or value"; fi
ses=$(http_get relay 7446 /sessions)
if echo "$ses" | grep -q '"version":"[^"]'; then echo "PASS  the relay's /sessions names the agent's version ($(echo "$ses" | grep -o '"version":"[^"]*"' | head -1))"
else fail "the relay's /sessions has no agent version: $ses"; fi
rst=$(http_get relay 7446 /status)
if echo "$rst" | grep -q '"draining":false' && echo "$rst" | grep -q '"orgs":\["1"\]'; then echo "PASS  the relay's /status: not draining, serves organization 1"
else fail "the relay's /status: $rst"; fi
pst=$(http_get proxy 7447 /status)
if echo "$pst" | grep -q '"org":"1"' && echo "$pst" | grep -q '"p95_ms"'; then echo "PASS  the proxy's /status: organization 1 with its response times"
else fail "the proxy's /status: $pst"; fi
fi # part goapp

# ---- block 6: the way back (tasks/rollback.md) ----
for name in $APPS; do
  part "$name" || continue
  db=$name
  docs=documents vers=document_versions
  [ "$name" = django ] && docs=docs_document vers=docs_documentversion
  admin="host=cloud user=postgres password=pw dbname=$db sslmode=disable"
  customer="host=relay port=15001 dbname=cust_$db user=postgres password=pw sslmode=disable"
  # Django keeps release 2's default of documents.status in the ORM: the column is NOT NULL
  # with no default in the database, and loadgen does not name it. The catalog comparison
  # below leaves the status column's lines out.
  [ "$name" = django ] && cloud_sql "$db" "ALTER TABLE $docs ALTER COLUMN status SET DEFAULT 'draft'" >/dev/null
  # the rows as the application has them: the versions' organization column is enable's
  digest="SELECT (SELECT count(*) || '/' || coalesce(sum(hashtext(t::text)::numeric), 0) FROM $docs t) || ' '
    || (SELECT count(*) || '/' || coalesce(sum(hashtext((to_jsonb(v) - 'organization_id')::text)::numeric), 0) FROM $vers v)"
  before=$(cloud_sql "$db" "$digest")

  log "$name: move-back: organization 1 returns to the cloud while both organizations keep writing"
  $C run --rm -T loadgen -dsn "host=proxy port=6432 dbname=$db user=app password=app sslmode=disable" \
    -docs "$docs" -versions "$vers" -duration 600s -until-back 1 >"loadback_$name.log" 2>&1 &
  load=$!
  sleep 5 # the steady write loop is running
  timeout 1200 $C run --rm -T lokadb move-back -db "$admin" -spec "/specs/$name.json" -org 1 -customer "$customer" 2>&1 \
    | tee "moveback_$name.log" | grep -v "^ Container"
  [ "${PIPESTATUS[0]}" = 0 ] || fail "$name move-back"
  wait $load && echo "PASS  $name: every write during move-back is there with its last value ($(grep -o 'writes_paused=[^ ]*' "moveback_$name.log"))" \
    || fail "$name: writes during move-back"
  grep -v "^ Container" "loadback_$name.log" | tail -3
  where=$(cloud_sql "$db" "SELECT (SELECT relkind::text FROM pg_class WHERE relname = '${docs}__o1') || ' ' || (SELECT count(*) FROM lokadb.orgs)
    || ' ' || (SELECT count(*) FROM pg_foreign_server WHERE srvname LIKE 'lokadb\_org%')")
  mark=$(cust_sql "cust_$db" "SELECT left(obj_description('$docs'::regclass, 'pg_class'), 31)")
  [ "$where" = "r 0 0" ] && [ "$mark" = "returned to the cloud by lokadb" ] \
    && echo "PASS  $name: organization 1 is in a local partition again, no server left; its tables stay in its database, marked as returned" \
    || fail "$name: after move-back: partition, orgs, servers '$where', mark '$mark'"

  log "$name: disable while both organizations keep writing"
  $C run --rm -T lokadb disable -db "$admin" -spec "/specs/$name.json" -dry-run 2>&1 | grep -v "^ Container" | grep "dry run"
  $C run --rm -T loadgen -dsn "host=proxy port=6432 dbname=$db user=app password=app sslmode=disable" \
    -docs "$docs" -versions "$vers" -duration 90s >"loaddisable_$name.log" 2>&1 &
  load=$!
  sleep 5
  timeout 600 $C run --rm -T lokadb disable -db "$admin" -spec "/specs/$name.json" 2>&1 | tee "disable_$name.log" | grep -v "^ Container"
  [ "${PIPESTATUS[0]}" = 0 ] || fail "$name disable"
  wait $load && echo "PASS  $name: every write during disable is there ($(grep -o 'paused=[^ ]*' "disable_$name.log" | head -1))" \
    || fail "$name: writes during disable"
  grep -v "^ Container" "loaddisable_$name.log" | tail -3

  after=$(cloud_sql "$db" "$digest")
  [ "$before" = "$after" ] && echo "PASS  $name: the rows are as before move-back and disable ($after)" \
    || fail "$name: rows before '$before', after '$after'"
  catalog "$db" >"catalog_after_$name.txt"
  # release 2 (migrate2, after enable) adds documents.status: the only difference expected
  gone=$(comm -23 "catalog_before_$name.txt" "catalog_after_$name.txt")
  new=$(comm -13 "catalog_before_$name.txt" "catalog_after_$name.txt" | grep -v 'status')
  added=$(comm -13 "catalog_before_$name.txt" "catalog_after_$name.txt" | grep -c 'status')
  [ -z "$gone" ] && [ -z "$new" ] \
    && echo "PASS  $name: the schema is as before enable ($(wc -l <"catalog_after_$name.txt") catalog lines; $added from release 2's status column)" \
    || { fail "$name: schema after disable differs from before enable"; echo "before only:"; echo "$gone" | head -8; echo "after only:"; echo "$new" | head -8; }
  # r2 ran once already (release 2, while enabled): its document of organization A goes first
  cloud_sql "$db" "DELETE FROM $docs WHERE slug = 'a-r2'" >/dev/null
  app "$name" r2 >"r2_disabled_$name.log" 2>&1 && echo "PASS  $name: the application runs on the plain tables (scenario r2)" \
    || { fail "$name r2 after disable"; grep -v "^ Container" "r2_disabled_$name.log" | tail -8; }
done

# ---- block 18: an application whose organizations are numbered by uuid (tasks/uuid-tenant.md) ----
# goapp in its uuid mode (e2e/goapp/uuid.go, pgx, binary uuids): accounts.id uuid, projects of an
# account, tasks under projects. Account A gets number 1 (org-add) and moves; B stays in the cloud.
if part uuidapp && [[ " $APPS " == *" goapp "* ]]; then
  db=uuidapp
  admin="host=cloud user=postgres password=pw dbname=$db sslmode=disable"
  customer="host=relay port=15001 dbname=cust_$db user=postgres password=pw sslmode=disable"
  customer_app="host=relay port=15001 dbname=cust_$db user=lokadb_app password=lokadb_app sslmode=disable"
  publisher="host=agent port=5433 dbname=$db user=postgres password=pw sslmode=disable"
  A=aaaaaaaa-0000-4000-8000-00000000000a
  uapp() { $C run --rm -T -e DATABASE_URL="host=proxy port=6432 dbname=$db user=app password=app sslmode=disable" goapp uuid "$@"; }

  log "uuidapp: an application numbered by uuid (block 18): migration and seed through the proxy, check, enable, org-add"
  uapp migrate >/dev/null && uapp seed || fail "uuidapp: migrate, seed"
  run_check check_uuidapp.log -db "$admin" -spec /specs/uuidapp.json -customer "$customer" -org 1 >/dev/null
  same_discovery uuidapp "$admin" accounts account_id
  if [[ $CHECK_RC =~ ^[02]$ ]] && ! reported check_uuidapp.log tenant_column_type; then
    echo "PASS  uuidapp: check takes the uuid organization column (exit $CHECK_RC, no tenant_column_type)"
  else
    fail "uuidapp: lokadb check exit $CHECK_RC"; grep -E '^  \[' check_uuidapp.log | head -5
  fi
  catalog "$db" >"catalog_before_$db.txt"
  lokadb enable -db "$admin" -spec /specs/uuidapp.json -owner app >/dev/null 2>&1 || fail "uuidapp enable"
  out=$(lokadb move-org -db "$admin" -spec /specs/uuidapp.json -owner app -org 1 -customer "$customer" 2>&1)
  [ $? != 0 ] && grep -q "org-add" <<<"$out" && echo "PASS  uuidapp: move-org of a number org-add has not given refuses and names org-add" \
    || fail "uuidapp: move-org before org-add: $(tail -1 <<<"$out")"
  out=$(lokadb org-add -db "$admin" -tenant "${A^^}" 2>&1)
  [ "$(cloud_sql "$db" "SELECT string_agg(org || '=' || tenant, ',') FROM lokadb.tenants")" = "1=$A" ] \
    && echo "PASS  uuidapp: org-add gave account A number 1 ($(grep -o 'organization 1: .*' <<<"$out"))" || fail "uuidapp: org-add: $out"

  log "uuidapp: move account A (organization 1) into its own database by triggers while both accounts write"
  uapp load -until-moved -tag load-move >"load_$db.log" 2>&1 &
  load=$!
  sleep 3
  timeout 1200 $C run --rm -T lokadb move-org -db "$admin" -spec /specs/uuidapp.json -owner app -org 1 -customer "$customer" \
    -customer-app "$customer_app" -publisher "$publisher" -cp "$CP" -mode trigger 2>&1 | tee "move_$db.log" | grep -v "^ Container" | tail -3
  [ "${PIPESTATUS[0]}" = 0 ] || fail "uuidapp move-org"
  wait $load && echo "PASS  uuidapp: every write of both accounts during the move survived ($(grep '^load:' "load_$db.log"))" \
    || { fail "uuidapp: writes during the move"; grep -v "^ Container" "load_$db.log" | tail -6; }
  at=$(cust_sql "cust_$db" "SELECT (SELECT count(*) FILTER (WHERE account_id = '$A') || '/' || count(*) FILTER (WHERE account_id IS DISTINCT FROM '$A') FROM projects)
    || ' ' || (SELECT count(*) FILTER (WHERE account_id IS DISTINCT FROM '$A') FROM tasks)")
  in=$(cloud_sql "$db" "SELECT (SELECT count(*) FROM projects__cloud WHERE account_id = '$A') || ' ' || (SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname = 'projects__org1')
    || ' ' || (SELECT count(*) FROM projects__none)")
  [[ $at =~ ^[0-9]+/0\ 0$ ]] && [ "${at%%/*}" -gt 20 ] && [ "$in" = "0 FOR VALUES IN ('$A') 1" ] \
    && echo "PASS  uuidapp: account A's projects and tasks live only in its database ($at); the cloud's partition of it by its uuid, the template project of no account in __none ($in)" \
    || fail "uuidapp: placement: in A's database '$at', in the cloud '$in'"

  # merge III: account A on the direct path (its profile and customers_app by its number, the
  # gate's pause lock by its uuid)
  out=$(lokadb org-profile -db "$admin" -tenant "$A" -customer-app "$customer_app" -direct-role lokadb_dml fdw+direct 2>&1)
  [ $? = 0 ] && [ "$(cloud_sql "$db" "SELECT profile FROM lokadb.orgs WHERE org = 1")" = fdw+direct ] \
    && echo "PASS  uuidapp: org-profile -tenant set account A to fdw+direct" || fail "uuidapp: org-profile fdw+direct: $(tail -2 <<<"$out")"
  direct_since=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  log "uuidapp: the application's requests with account A in its own database"
  uapp r1 || fail "uuidapp r1"
  plog=$($C logs --no-color --since "$direct_since" proxy 2>&1 | grep 'msg=route' | grep "db=$db" | grep "to=\"tenant $A\"")
  ndirect=$(grep -c 'path=direct' <<<"$plog")
  [ "${ndirect:-0}" -gt 0 ] && echo "PASS  uuidapp: $ndirect of $(grep -c . <<<"$plog") statements of account A went the direct path (proxy log path=direct)" \
    || fail "uuidapp: no statement of account A went the direct path ($(grep -c . <<<"$plog") routed)"
  # the cooperative mode in a transaction with the profile fdw+direct (pgx prepares set_config while BEGIN is
  # deferred: block 10 keeps lokadb.tenant, DP21; such a transaction runs through the cloud, a direct one
  # starts with SET LOCAL: UT11), then with fdw
  log "uuidapp: the cooperative mode (set_config('lokadb.tenant', <uuid>, true) in its transaction), with the profile fdw+direct and fdw"
  uapp coop || fail "uuidapp: cooperative mode on fdw+direct"
  lokadb org-profile -db "$admin" -tenant "$A" fdw >/dev/null 2>&1 || fail "uuidapp: org-profile back to fdw"
  uapp coop || fail "uuidapp: cooperative mode on fdw"

  # block 11: LokaDB's own statements (the proxy's read of lokadb.tenants on the application's
  # connection, the commands' queries) carry internal/ownsql's mark: neither the recording nor
  # pg_stat_statements gives check a statement of LokaDB's as the application's (a query_forbidden
  # blocker "lokadb.tenants belongs to LokaDB" on a real database)
  log "uuidapp: check of the recorded workload and pg_stat_statements after enable: no statement of LokaDB's own"
  sleep 3
  run_check check_uuidapp_after.log -db "$admin" -spec /specs/uuidapp.json -workload /workload/workload.json -proxy-config /etc/lokadb.json >/dev/null
  pss=$(cloud_sql "$db" "SELECT count(*) FILTER (WHERE query LIKE '%/* lokadb */') || '/' || count(*) FILTER (WHERE query NOT LIKE '%/* lokadb */')
    FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND userid = 'app'::regrole AND query LIKE '%FROM lokadb.tenants%'")
  rec=$(grep -c 'lokadb\.tenants' workload/workload.json)
  # (exit 3 is right: r1's statement that names no account is refused under fanout reject)
  if [[ $CHECK_RC =~ ^[0234]$ ]] && ! reported check_uuidapp_after.log query_forbidden && ! grep -q 'lokadb\.tenants' check_uuidapp_after.log \
    && [[ $pss =~ ^[1-9][0-9]*/0$ ]] && [ "$rec" = 0 ]; then
    echo "PASS  uuidapp: check after enable (exit $CHECK_RC) names no statement of LokaDB's; the proxy's reads of lokadb.tenants under the application's role are marked in pg_stat_statements ($pss marked/not), none in the recording"
  else
    fail "uuidapp: LokaDB's own statements: check exit $CHECK_RC, pg_stat_statements marked/not '$pss', in the recording $rec"
    sed -n '/^Blockers/,/^$/p' check_uuidapp_after.log | head -8
  fi

  log "uuidapp: account A's database goes offline"
  $C stop -t 60 customer >/dev/null
  uapp offline || fail "uuidapp offline"
  $C start customer >/dev/null && wait_healthy customer && wait_tunnel || fail "customer did not come back"

  log "uuidapp: move-back while both accounts write, then disable"
  uapp load -until-back -tag load-back >"loadback_$db.log" 2>&1 &
  load=$!
  sleep 3
  # by its uuid instead of its number (-tenant: a lookup in lokadb.tenants)
  timeout 1200 $C run --rm -T lokadb move-back -db "$admin" -spec /specs/uuidapp.json -tenant "$A" -customer "$customer" 2>&1 \
    | tee "moveback_$db.log" | grep -v "^ Container" | tail -3
  [ "${PIPESTATUS[0]}" = 0 ] || fail "uuidapp move-back"
  wait $load && echo "PASS  uuidapp: every write during move-back survived ($(grep '^load:' "loadback_$db.log"))" \
    || { fail "uuidapp: writes during move-back"; grep -v "^ Container" "loadback_$db.log" | tail -6; }
  back=$(cloud_sql "$db" "SELECT count(*) FILTER (WHERE c.relname = 'projects__o1') || '/' || count(*) || ' ' || (SELECT count(*) FROM lokadb.orgs)
    FROM projects p JOIN pg_class c ON c.oid = p.tableoid WHERE p.account_id = '$A'")
  [[ $back =~ ^([0-9]+)/([0-9]+)\ 0$ ]] && [ "${BASH_REMATCH[1]}" = "${BASH_REMATCH[2]}" ] \
    && echo "PASS  uuidapp: account A is back in its partition of the cloud ($back)" || fail "uuidapp: after move-back: '$back'"
  digest="SELECT (SELECT count(*) || '/' || coalesce(sum(hashtext(t::text)::numeric), 0) FROM projects t) || ' '
    || (SELECT count(*) || '/' || coalesce(sum(hashtext((to_jsonb(v) - 'account_id')::text)::numeric), 0) FROM tasks v)"
  before=$(cloud_sql "$db" "$digest")
  timeout 600 $C run --rm -T lokadb disable -db "$admin" -spec /specs/uuidapp.json 2>&1 | tee "disable_$db.log" | grep -v "^ Container" | tail -2
  [ "${PIPESTATUS[0]}" = 0 ] || fail "uuidapp disable"
  after=$(cloud_sql "$db" "$digest")
  catalog "$db" >"catalog_after_$db.txt"
  diffs=$(comm -3 "catalog_before_$db.txt" "catalog_after_$db.txt")
  [ "$before" = "$after" ] && [ -z "$diffs" ] \
    && echo "PASS  uuidapp: after disable the rows are as before ($after) and the schema is as before enable ($(wc -l <"catalog_after_$db.txt") catalog lines)" \
    || { fail "uuidapp: after disable: rows '$before' / '$after'"; echo "$diffs" | head -8; }
fi

$C logs --no-color proxy >proxy.log 2>&1
$C logs --no-color relay agent >tunnel.log 2>&1
log "result"
if [ $FAILED = 0 ]; then echo "ALL CHECKS PASSED"; else echo "SOME CHECKS FAILED"; fi
exit $FAILED
