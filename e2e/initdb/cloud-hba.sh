#!/bin/sh
# BACKLOG block 8, item 11: the application's role reaches the cloud database from the proxies
# only (CLOUD_APP_HOSTS, set by run.sh; empty: as the image has it, for bench*.sh's direct
# baselines). Its rules go first: pg_hba takes the first line that matches. No exit: the image
# may source this file.
if [ -n "$CLOUD_APP_HOSTS" ]; then
  hba="$PGDATA/pg_hba.conf"
  {
    for h in $CLOUD_APP_HOSTS; do echo "host all app $h scram-sha-256"; done
    echo "host all app all reject"
    cat "$hba"
  } >"$hba.new" && mv "$hba.new" "$hba"
fi
