#!/bin/bash

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Load pgmer2 into both template_database and $POSTGRES_DB
for DB in template_postgis "$POSTGRES_DB"; do
        echo "Loading pgmer2 extension into $DB"
        "${psql[@]}" --dbname="$DB" <<-'EOSQL'
                CREATE EXTENSION IF NOT EXISTS pgmer2;
EOSQL
done
