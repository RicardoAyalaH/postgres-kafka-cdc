#!/bin/sh
# CDC setup for the source database. Runs once, on first initialization of the data volume.
#
# Creates a least-privilege role for Debezium and the publication it reads from.
# The password comes from the environment (.env) and is passed to psql as a variable,
# so it is never hard-coded and is quoted safely by psql (:'var' syntax).
#
# Note: no `set -u` here. The postgres entrypoint *sources* .sh files that are not
# executable, and changing shell options would leak into the entrypoint itself.

psql -v ON_ERROR_STOP=1 \
     -v dbz_user="$DEBEZIUM_POSTGRES_USER" \
     -v dbz_password="$DEBEZIUM_POSTGRES_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'

-- LOGIN + REPLICATION: may open a replication connection and create a replication slot.
-- Deliberately not a superuser and not the table owner.
CREATE ROLE :"dbz_user" WITH LOGIN REPLICATION PASSWORD :'dbz_password';

GRANT CONNECT ON DATABASE :"DBNAME" TO :"dbz_user";
GRANT USAGE ON SCHEMA public TO :"dbz_user";

-- Read access only: needed for Debezium's initial snapshot of existing rows.
GRANT SELECT ON orders TO :"dbz_user";

-- The publication is the allow-list of tables whose changes pgoutput will decode.
-- Created here by the owner; Debezium is configured not to create its own.
CREATE PUBLICATION cdc_publication FOR TABLE orders;
SQL
