#!/bin/sh
# Monitoring role for the source database. Runs once, on first initialization of the data volume.
#
# The slot monitor (npm run slot-monitor) uses this role to read pg_replication_slots
# and the current WAL position. It gets no access to table data.
# Same pattern as 02-debezium.sh: password from the environment, passed as a psql variable.

psql -v ON_ERROR_STOP=1 \
     -v mon_user="$MONITOR_POSTGRES_USER" \
     -v mon_password="$MONITOR_POSTGRES_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'

CREATE ROLE :"mon_user" WITH LOGIN PASSWORD :'mon_password';

GRANT CONNECT ON DATABASE :"DBNAME" TO :"mon_user";

-- pg_monitor is Postgres's built-in role for monitoring tools: it can read statistics
-- and replication views and call WAL-position functions, but not read or change data.
GRANT pg_monitor TO :"mon_user";
SQL
