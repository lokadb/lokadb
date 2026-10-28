#!/bin/sh
# The application: through the proxy, as any application would, it writes a document and a
# version for organization 1 and for organization 2 every half second and reads them back.
# It never stops, also not while organization 1 moves: watch for errors in its output.
DSN="host=proxy port=6432 dbname=demo user=app password=app sslmode=disable"
until psql "$DSN" -tAc 'SELECT 1' >/dev/null 2>&1; do sleep 1; done
# One statement through the proxy. A write that was waiting while `enable` converted the table can get
# "the row reached the table without its organization ... retry the statement" once (the proxy had not
# yet learnt that the table became lokadb's): the application retries, as it would any transient error.
write() {
  out=$(psql "$DSN" -qtAc "$1" 2>&1); rc=$?
  case "$out" in
    *"retry the statement"*) sleep 0.2; out=$(psql "$DSN" -qtAc "$1" 2>&1); rc=$? ;;
  esac
  printf '%s' "$out"
  return $rc
}
n=0 errors=0
while :; do
  for org in 1 2; do
    id=$(write "INSERT INTO documents (organization_id, title, body) VALUES ($org, 'demo $n', 'written by demo') RETURNING id")
    case "$id" in
      ''|*[!0-9]*) errors=$((errors + 1)); echo "write error (org $org, document): $id" ;;
      *) err=$(write "INSERT INTO document_versions (document_id, content) VALUES ($id, 'v1')")            || { errors=$((errors + 1)); echo "write error (org $org, version of $id): $err"; } ;;
    esac
  done
  n=$((n + 1))
  if [ $((n % 20)) = 0 ]; then
    c1=$(psql "$DSN" -tAc "SELECT count(*) FROM documents WHERE organization_id = 1" 2>&1)
    c2=$(psql "$DSN" -tAc "SELECT count(*) FROM documents WHERE organization_id = 2" 2>&1)
    echo "demo: $n rounds, organization 1 has $c1 documents, organization 2 has $c2, write errors so far: $errors"
  fi
  sleep 0.5
done
