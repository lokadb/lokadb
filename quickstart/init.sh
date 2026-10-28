#!/bin/sh
# One shot, in the cloud network: issue the agent's token, wait for the tunnel, then
# check -> enable -> move-org of organization 1, and show where each organization's rows are.
set -e
CP="host=cloud dbname=controlplane user=postgres password=pw sslmode=disable"
ADMIN="host=cloud dbname=demo user=postgres password=pw sslmode=disable"
# the organization's database, reached only through the relay and the agent
CUST_ADMIN="host=relay port=15001 dbname=cust_demo user=postgres password=pw sslmode=disable"
CUST_APP="host=relay port=15001 dbname=cust_demo user=lokadb_app password=lokadb_app sslmode=disable"
Q=/quickstart

step() { printf '\n=== %s\n' "$*"; }

until pg_isready -q -h cloud -U postgres; do sleep 1; done

if [ ! -s /shared/token ]; then
  step "1. one-time enrolment token for organization 1's agent (the registry keeps only its hash)"
  # the relay creates its registry tables on start: retry until it has
  until lokadb token -cp "$CP" -org 1 -ttl 15m >/shared/token.tmp 2>/dev/null; do sleep 1; done
  mv /shared/token.tmp /shared/token
fi

step "2. waiting for the agent to enrol and open the tunnel"
i=0
until psql "$CUST_APP" -tAc 'SELECT 1' >/dev/null 2>&1; do
  i=$((i + 1)); [ $i -gt 180 ] && { echo "the tunnel did not come up in 3 minutes: docker compose logs agent relay"; exit 1; }
  sleep 1
done
echo "the organization's database answers through relay -> agent; the cloud cannot reach it any other way"

if psql "$ADMIN" -tAc "SELECT 1 FROM lokadb.orgs WHERE org = 1" 2>/dev/null | grep -q 1; then
  step "organization 1 is already moved (a previous run): skipping check, enable and move-org"
else
  step "3. lokadb check: what blocks the move, what is not supported, what changes"
  lokadb check -db "$ADMIN" -spec $Q/spec.json -proxy-config $Q/lokadb.json \
    -customer "$CUST_ADMIN" -customer-app "$CUST_APP" -org 1 || true   # exit 2 = needs review

  step "4. lokadb enable: documents and document_versions become partitioned by organization (no row copy)"
  lokadb enable -db "$ADMIN" -spec $Q/spec.json -owner app

  step "5. lokadb move-org: organization 1 moves to its own database while demo keeps writing"
  lokadb move-org -db "$ADMIN" -spec $Q/spec.json -owner app -org 1 \
    -customer "$CUST_ADMIN" -customer-app "$CUST_APP" \
    -proxy-config $Q/lokadb.json -direct-role lokadb_dml
fi

step "6. where the rows are"
psql "$ADMIN" -tA <<SQL
SELECT 'cloud: profile of organization 1 = ' || coalesce(profile, 'fdw') FROM lokadb.orgs WHERE org = 1;
SELECT 'cloud: rows of organization 1 stored in the cloud = ' || count(*) FROM ONLY documents__cloud WHERE organization_id = 1;
SELECT 'cloud: rows of organization 2 stored in the cloud = ' || count(*) FROM ONLY documents__cloud WHERE organization_id = 2;
SQL
psql "$CUST_APP" -tAc "SELECT 'customer database: rows of organization 1 = ' || count(*) FROM documents"
cat <<'TXT'

Done. Try it yourself through the proxy:
  psql "host=localhost port=6432 dbname=demo user=app password=app"
  (or, without psql on the host: docker compose -f quickstart/compose.yml exec cloud psql "host=proxy port=6432 dbname=demo user=app password=app")
  SELECT count(*) FROM documents WHERE organization_id = 1;   -- from the customer database
  SELECT count(*) FROM documents WHERE organization_id = 2;   -- from the cloud
  SELECT count(*) FROM documents;   -- names no organization: refused by the default policy
Stop the agent (docker compose -f quickstart/compose.yml stop agent): organization 1 fails
with a readable error, organization 2 keeps working.
TXT
