#!/usr/bin/env bash
# Airflow's metadata database. Separate database in the same Postgres server:
# it is operational state, never analytics data, and it must not end up in the
# logical replication publication.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
CREATE ROLE ${AIRFLOW_METADATA_USER} WITH LOGIN PASSWORD '${AIRFLOW_METADATA_PASSWORD}';
CREATE DATABASE ${AIRFLOW_METADATA_DB} OWNER ${AIRFLOW_METADATA_USER};
EOSQL

echo "[init] airflow metadata database created."
