#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Creates the analytics account from .env, so credentials live in exactly one
# place. Runs before the .sql init files (lexical order) and uses the loopback
# default account, which is the only thing that can reach the server this early.
# -----------------------------------------------------------------------------
set -euo pipefail

clickhouse client -n <<EOSQL
CREATE USER IF NOT EXISTS ${CLICKHOUSE_USER}
    IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PASSWORD}'
    HOST ANY
    SETTINGS PROFILE 'analytics_profile';

GRANT ALL ON *.* TO ${CLICKHOUSE_USER} WITH GRANT OPTION;
EOSQL

echo "[init] ClickHouse user ${CLICKHOUSE_USER} created."
