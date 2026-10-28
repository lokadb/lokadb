# pgbouncer in front of the proxy (check B9.11): the PGDG package of the postgres image.
FROM postgres:18
RUN apt-get update && apt-get install -y --no-install-recommends pgbouncer && rm -rf /var/lib/apt/lists/*
COPY pgbouncer.ini userlist.txt /etc/pgbouncer/
USER postgres
CMD ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
