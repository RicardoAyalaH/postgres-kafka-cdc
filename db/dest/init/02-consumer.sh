#!/bin/sh
# Least-privilege role for the CDC consumer. Runs once, on first initialization of the
# data volume, after 01-schema.sql (init scripts run in alphabetical order).
#
# POSTGRES_USER is a superuser in the postgres image; the consumer should not be.
# Same pattern as the source init scripts: password from the environment, passed as a
# psql variable so it never appears in the SQL text.

psql -v ON_ERROR_STOP=1 \
     -v consumer_user="$CONSUMER_POSTGRES_USER" \
     -v consumer_password="$CONSUMER_POSTGRES_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'

CREATE ROLE :"consumer_user" WITH LOGIN PASSWORD :'consumer_password';

GRANT CONNECT ON DATABASE :"DBNAME" TO :"consumer_user";
-- USAGE lets it see objects in the schema. No CREATE (and since Postgres 15, PUBLIC has
-- no CREATE on public either), so the consumer can't create or alter tables.
GRANT USAGE ON SCHEMA public TO :"consumer_user";

-- SELECT is required, not optional: the upsert's ON CONFLICT ... WHERE and the delete's
-- WHERE read source_lsn and id, and Postgres demands SELECT on columns a statement reads.
-- No DELETE or TRUNCATE: deletes must be soft (UPDATE ... SET deleted = true), and
-- without the privilege a hard delete fails instead of silently losing the marker.
GRANT SELECT, INSERT, UPDATE ON orders TO :"consumer_user";

-- Read access to the view readers use, for the DB tests and verification.
GRANT SELECT ON orders_live TO :"consumer_user";
SQL
