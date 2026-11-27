#!/bin/sh
# Least-privilege role for the CDC consumer. Runs once, on first initialization of the
# data volume, after 01-schema.sql and 02-test-db.sh (init scripts run in alphabetical
# order).
#
# POSTGRES_USER is a superuser in the postgres image; the consumer should not be.
# Same pattern as the source init scripts: password from the environment, passed as a
# psql variable so it never appears in the SQL text.
set -e

# Roles belong to the server, not to one database: created once.
psql -v ON_ERROR_STOP=1 \
     -v consumer_user="$CONSUMER_POSTGRES_USER" \
     -v consumer_password="$CONSUMER_POSTGRES_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE :"consumer_user" WITH LOGIN PASSWORD :'consumer_password';
SQL

# Grants belong to each database: the same ones in the replica and in the test database
# (if there is one), so the tests check exactly the consumer's privileges.
for db in "$POSTGRES_DB" ${DEST_TEST_POSTGRES_DB:+"$DEST_TEST_POSTGRES_DB"}; do
  psql -v ON_ERROR_STOP=1 -v consumer_user="$CONSUMER_POSTGRES_USER" \
       --username "$POSTGRES_USER" --dbname "$db" <<'SQL'
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
done
