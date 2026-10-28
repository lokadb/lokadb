# Postgres of the stands with extensions installed as packages (extensions after a move,
# block 7): PKGS are the PGDG packages of this major version. The extensions are marked trusted
# (a role with CREATE on the database may create them: what managed Postgres lets an
# application's migration do), so a test needs no superuser, which the proxy refuses anyway.
FROM postgres:18
ARG PKGS="postgresql-18-pgvector"
RUN apt-get update && apt-get install -y --no-install-recommends $PKGS && rm -rf /var/lib/apt/lists/* \
    && for f in $(ls /usr/share/postgresql/18/extension/*.control | grep -E '/(vector|postgis)\.control$'); do \
         grep -q '^trusted' $f || echo "trusted = true" >> $f; done
