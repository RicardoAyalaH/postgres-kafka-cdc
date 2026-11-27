#!/bin/sh
# A second database with the same schema, for `npm run test:db`. The tests run as the
# consumer's role in transactions that are rolled back, so this database stays empty and
# the replica in POSTGRES_DB is never touched. Runs once, on first initialization of the
# data volume, after 01-schema.sql and before 03-consumer.sh (which grants on both).
set -e

if [ -z "$DEST_TEST_POSTGRES_DB" ]; then
  echo "02-test-db.sh: DEST_TEST_POSTGRES_DB not set, no test database created"
  exit 0
fi

psql -v ON_ERROR_STOP=1 -v test_db="$DEST_TEST_POSTGRES_DB" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE DATABASE :"test_db";
SQL

# The same schema file the replica got, so the tests can't drift from it.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DEST_TEST_POSTGRES_DB" \
     -f "$(dirname "$0")/01-schema.sql"
