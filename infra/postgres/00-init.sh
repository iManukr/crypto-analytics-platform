#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Postgres bootstrap. Runs once, as the superuser, on an empty data directory.
#
# Creates:
#   * the application role the ingester writes with (least privilege: DML only)
#   * a dedicated REPLICATION role for Debezium (SELECT only, no DML)
#   * the crypto schema and its three CDC-captured tables
#   * REPLICA IDENTITY FULL, so UPDATE/DELETE events carry a full before-image
#   * the logical replication publication Debezium subscribes to
#
# Names/passwords come from the environment so .env stays the single source of
# truth; nothing here is hardcoded.
# -----------------------------------------------------------------------------
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v app_user="${APP_USER}" \
     -v app_password="'${APP_PASSWORD}'" \
     -v dbz_user="${DBZ_USER}" \
     -v dbz_password="'${DBZ_PASSWORD}'" <<'EOSQL'

-- ============================================================ roles ==========
-- Application role: writes the raw landed data. No superuser, no DDL.
CREATE ROLE :app_user WITH LOGIN PASSWORD :app_password;

-- Debezium role: reads the WAL. REPLICATION is the only elevated privilege it
-- gets; it deliberately has no INSERT/UPDATE/DELETE anywhere.
CREATE ROLE :dbz_user WITH LOGIN REPLICATION PASSWORD :dbz_password;

-- =========================================================== schema ==========
CREATE SCHEMA IF NOT EXISTS crypto;

-- Reference dimension. Low volume, occasionally UPDATEd (is_active toggles),
-- which is exactly the shape that exercises the CDC update path.
CREATE TABLE crypto.symbols (
    symbol        varchar(20)  PRIMARY KEY,
    base_asset    varchar(10)  NOT NULL,
    quote_asset   varchar(10)  NOT NULL,
    display_name  text         NOT NULL,
    is_active     boolean      NOT NULL DEFAULT true,
    created_at    timestamptz  NOT NULL DEFAULT now(),
    updated_at    timestamptz  NOT NULL DEFAULT now()
);

-- The high-volume fact table. Insert-heavy; the upsert makes replays idempotent
-- on (symbol, open_time), so the ingester can safely re-send a candle.
CREATE TABLE crypto.market_candles_1m (
    symbol           varchar(20)   NOT NULL REFERENCES crypto.symbols(symbol),
    open_time        timestamptz   NOT NULL,
    close_time       timestamptz   NOT NULL,
    open_price       numeric(20,8) NOT NULL,
    high_price       numeric(20,8) NOT NULL,
    low_price        numeric(20,8) NOT NULL,
    close_price      numeric(20,8) NOT NULL,
    volume           numeric(30,8) NOT NULL,
    quote_volume     numeric(30,8) NOT NULL,
    trade_count      integer       NOT NULL,
    taker_buy_base   numeric(30,8) NOT NULL,
    taker_buy_quote  numeric(30,8) NOT NULL,
    source           varchar(20)   NOT NULL,
    ingested_at      timestamptz   NOT NULL DEFAULT now(),
    updated_at       timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, open_time)
);

-- Freshness probes and the batch reconciliation window both scan on recency.
CREATE INDEX idx_candles_ingested_at ON crypto.market_candles_1m (ingested_at DESC);
CREATE INDEX idx_candles_open_time   ON crypto.market_candles_1m (open_time DESC);

-- Current-value FX table: one row per currency pair, rewritten in place. This is
-- the UPDATE-heavy counterpart to the append-heavy candles table, and it is what
-- proves the ReplacingMergeTree dedup in ClickHouse actually works.
CREATE TABLE crypto.fx_rates (
    base        varchar(10)   NOT NULL,
    quote       varchar(10)   NOT NULL,
    rate        numeric(20,8) NOT NULL,
    as_of       timestamptz   NOT NULL,
    source      text          NOT NULL,
    updated_at  timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (base, quote)
);

-- Quarantine for rows that fail validation at ingest time. Deliberately NOT in
-- the CDC publication: rejects are an operational concern, not analytics data.
CREATE TABLE crypto.ingest_rejects (
    id           bigserial     PRIMARY KEY,
    source       varchar(32)   NOT NULL,
    symbol       varchar(20),
    reason       text          NOT NULL,
    payload      jsonb         NOT NULL,
    rejected_at  timestamptz   NOT NULL DEFAULT now()
);

-- ================================================== replica identity =========
-- Postgres' default (REPLICA IDENTITY DEFAULT) only puts the PK columns in the
-- WAL for UPDATE/DELETE. FULL logs the entire old row, which is what lets the
-- ClickHouse side reconstruct a delete without having to look anything up.
-- The cost is a larger WAL; acceptable at this volume and revisited in
-- docs/SCALING.md for when it is not.
ALTER TABLE crypto.symbols           REPLICA IDENTITY FULL;
ALTER TABLE crypto.market_candles_1m REPLICA IDENTITY FULL;
ALTER TABLE crypto.fx_rates          REPLICA IDENTITY FULL;

-- ====================================================== publication ==========
-- Created explicitly (rather than letting Debezium autocreate it) so the set of
-- captured tables is version-controlled and the connector role needs no DDL
-- rights. The connector is configured with publication.autocreate.mode=disabled.
CREATE PUBLICATION dbz_publication
    FOR TABLE crypto.symbols, crypto.market_candles_1m, crypto.fx_rates;

-- =========================================================== grants ==========
GRANT USAGE ON SCHEMA crypto TO :app_user, :dbz_user;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA crypto TO :app_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA crypto TO :app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crypto
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :app_user;

-- Debezium reads only. It needs SELECT for the initial consistent snapshot.
GRANT SELECT ON ALL TABLES IN SCHEMA crypto TO :dbz_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crypto GRANT SELECT ON TABLES TO :dbz_user;

-- pg_monitor is the built-in role for exactly this: reading pg_stat_*,
-- pg_replication_slots and the other monitoring views WITHOUT superuser. The
-- Prometheus postgres_exporter and the pipeline exporter both connect as the
-- application role, so neither needs a superuser credential lying around in an
-- environment variable. Replication-slot lag is only visible with this grant.
GRANT pg_monitor TO :app_user;

-- ===================================================== seed the dim ==========
INSERT INTO crypto.symbols (symbol, base_asset, quote_asset, display_name) VALUES
    ('ETHUSDT', 'ETH', 'USDT', 'Ethereum / Tether'),
    ('BTCUSDT', 'BTC', 'USDT', 'Bitcoin / Tether')
ON CONFLICT (symbol) DO NOTHING;

EOSQL

echo "[init] crypto schema, roles, publication and seed data created."
