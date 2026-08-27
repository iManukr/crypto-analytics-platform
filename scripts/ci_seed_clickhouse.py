#!/usr/bin/env python3
"""Seed a bare ClickHouse with the pipeline schema and deterministic fixtures.

Purpose: let CI test the *transformation layer* - every dbt model and every dbt
test - without booting Kafka, Debezium and Postgres. That job then runs in about
a minute instead of eight, so a pull request that breaks a mart is caught almost
immediately rather than at the end of the full end-to-end job.

The key property is that the DDL is **not duplicated here**. This script
executes the same files in infra/clickhouse/init/ that the real container runs,
skipping only the two that require a live broker:

    03-kafka-engines.sql      Kafka engine tables - no broker in this job
    04-materialized-views.sql - the views that read those Kafka tables

Everything else, including the ReplacingMergeTree definitions, the ORDER BY and
PARTITION BY clauses and the codecs, is byte-identical to production. If the
schema drifts, this job fails - which is the point. A CI fixture with its own
hand-maintained copy of the schema tests a warehouse that does not exist.

Fixtures come from the same deterministic replay generator the pipeline uses, so
the rows are realistic (continuous prices, valid OHLC relationships) and
reproducible run to run.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingestion.sources.base import MINUTE_MS  # noqa: E402
from ingestion.sources.replay import ReplaySource  # noqa: E402

CH_URL = f"http://{os.environ.get('CLICKHOUSE_HOST', 'localhost')}:{os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')}/"
CH_AUTH = (os.environ.get("CLICKHOUSE_USER", "default"), os.environ.get("CLICKHOUSE_PASSWORD", ""))

INIT_DIR = ROOT / "infra" / "clickhouse" / "init"
# The only files skipped, and the only reason to skip them.
REQUIRES_BROKER = {"03-kafka-engines.sql", "04-materialized-views.sql"}

SYMBOLS = [s.strip() for s in os.environ.get("SYMBOLS", "ETHUSDT,BTCUSDT").split(",") if s.strip()]
MINUTES = int(os.environ.get("CI_FIXTURE_MINUTES", "240"))


def execute(sql: str) -> str:
    response = requests.post(CH_URL, auth=CH_AUTH, data=sql.encode(), timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"ClickHouse rejected:\n{sql[:400]}\n---\n{response.text[:600]}")
    return response.text


def statements(sql_text: str) -> list[str]:
    """Split a file into statements, stripping SQL comments first.

    Comments are removed before splitting because several of them contain
    apostrophes and punctuation that would otherwise confuse a naive split.
    """
    without_comments = re.sub(r"^\s*--.*$", "", sql_text, flags=re.MULTILINE)
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def apply_schema() -> None:
    applied = 0
    for path in sorted(INIT_DIR.glob("*.sql")):
        if path.name in REQUIRES_BROKER:
            print(f"  skip  {path.name} (needs a live Kafka broker)")
            continue
        print(f"  apply {path.name}")
        for statement in statements(path.read_text(encoding="utf-8")):
            execute(statement)
            applied += 1
    print(f"schema applied: {applied} statements")


def seed_candles() -> int:
    """Insert fixture candles directly into the CDC landing table.

    Written in the shape the materialized view would have produced, including
    the CDC metadata columns, so staging's dedup and lag calculations exercise
    real values rather than nulls.
    """
    source = ReplaySource()
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start_ms = int((end - timedelta(minutes=MINUTES)).timestamp() * 1000)

    rows: list[str] = []
    for symbol_index, symbol in enumerate(SYMBOLS):
        for index in range(MINUTES):
            open_ms = start_ms + index * MINUTE_MS
            candle = source.candle_for(symbol, open_ms)
            # A monotonically increasing fake LSN, mirroring how Postgres would
            # version these. Deliberately varied per row so a dedup bug in
            # staging cannot pass by accident on a constant version.
            lsn = 1_000_000 + index * 10 + symbol_index
            source_ts = open_ms + 60_000
            rows.append(
                "\t".join(
                    [
                        candle.symbol,
                        candle.open_time.strftime("%Y-%m-%d %H:%M:%S.000"),
                        candle.close_time.strftime("%Y-%m-%d %H:%M:%S.000"),
                        str(candle.open_price),
                        str(candle.high_price),
                        str(candle.low_price),
                        str(candle.close_price),
                        str(candle.volume),
                        str(candle.quote_volume),
                        str(candle.trade_count),
                        str(candle.taker_buy_base),
                        str(candle.taker_buy_quote),
                        candle.source,
                        candle.open_time.strftime("%Y-%m-%d %H:%M:%S.000"),
                        "c",
                        str(lsn),
                        str(source_ts),
                        "0",
                        str(index),
                        # Arrived ~1.2s after the Postgres commit, so the
                        # cdc_lag_seconds column holds a plausible value and its
                        # range test is meaningful.
                        datetime.fromtimestamp((source_ts + 1200) / 1000, tz=UTC).strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        )[:-3],
                    ]
                )
            )

    columns = (
        "symbol, open_time, close_time, open_price, high_price, low_price, close_price, "
        "volume, quote_volume, trade_count, taker_buy_base, taker_buy_quote, source, "
        "ingested_at, _op, _lsn, _source_ts_ms, _kafka_partition, _kafka_offset, _cdc_arrived_at"
    )
    execute(
        f"INSERT INTO raw.market_candles_1m ({columns}) FORMAT TabSeparated\n" + "\n".join(rows)
    )
    return len(rows)


def seed_duplicates() -> int:
    """Deliberately re-insert some rows with a HIGHER version.

    This is the fixture that makes the staging tests meaningful. Debezium is
    at-least-once, so duplicates are the normal case, not an exotic one. If
    staging's FINAL dedup were removed, these rows would double the count and
    the `unique_combination` test would fail - which is exactly what should
    happen. Without this fixture the dedup test passes trivially on data that
    never had a duplicate in it.
    """
    execute(
        """
        INSERT INTO raw.market_candles_1m
        SELECT
            symbol, open_time, close_time, open_price, high_price, low_price, close_price,
            volume, quote_volume, trade_count, taker_buy_base, taker_buy_quote, source,
            ingested_at, 'u' AS _op, _lsn + 500000 AS _lsn, _source_ts_ms + 1000,
            _kafka_partition, _kafka_offset + 100000, _cdc_arrived_at
        FROM raw.market_candles_1m
        ORDER BY open_time DESC
        LIMIT 50
        """
    )
    return 50


def seed_reference() -> None:
    execute(
        """
        INSERT INTO raw.symbols
            (symbol, base_asset, quote_asset, display_name, is_active,
             created_at, updated_at, _op, _lsn, _source_ts_ms, _kafka_partition, _kafka_offset)
        VALUES
            ('ETHUSDT','ETH','USDT','Ethereum / Tether',1,now(),now(),'r',1,1,0,0),
            ('BTCUSDT','BTC','USDT','Bitcoin / Tether',1,now(),now(),'r',2,2,0,1)
        """
    )

    # Three FX observations at different as_of values, so the ASOF join in
    # fct_candles_1m has real history to resolve against rather than one row.
    execute(
        """
        INSERT INTO raw.fx_rates
            (base, quote, rate, as_of, source, updated_at,
             _op, _lsn, _source_ts_ms, _kafka_partition, _kafka_offset)
        VALUES
            ('USD','KES',128.90, now() - toIntervalDay(3),'ci-fixture',now(),'c',10,10,0,0),
            ('USD','KES',129.15, now() - toIntervalDay(2),'ci-fixture',now(),'u',11,11,0,1),
            ('USD','KES',129.34, now() - toIntervalDay(1),'ci-fixture',now(),'u',12,12,0,2)
        """
    )


def main() -> int:
    print(f"seeding ClickHouse at {CH_URL}")
    apply_schema()

    seed_reference()
    print("reference data: 2 symbols, 3 FX observations")

    count = seed_candles()
    print(f"candles: {count} rows across {len(SYMBOLS)} symbol(s), {MINUTES} minutes each")

    dupes = seed_duplicates()
    print(f"duplicates: {dupes} replayed rows with a higher LSN (exercises the staging dedup)")

    total = execute("SELECT count() FROM raw.market_candles_1m").strip()
    deduped = execute("SELECT count() FROM raw.market_candles_1m FINAL WHERE _op != 'd'").strip()
    print(f"\nraw.market_candles_1m: {total} rows, {deduped} after dedup")
    if int(total) <= int(deduped):
        print("ERROR: the duplicate fixture did not take effect; the dedup test would be vacuous")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
