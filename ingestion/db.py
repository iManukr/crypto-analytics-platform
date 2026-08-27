"""PostgreSQL writer.

Two properties matter more than anything else here, because everything
downstream inherits them:

**Idempotence.** Every write is an upsert keyed on the natural key. The
ingester re-sends candles on startup and after gaps, and a container restart
mid-cycle re-sends whatever was in flight. If those writes were not idempotent,
the recovery mechanism would itself be a source of duplicates - and duplicates
in the OLTP layer become duplicate CDC events, which become double-counted
volume in the marts.

**Bounded work per statement.** Rows go in with ``execute_values`` in pages, not
one round trip per row. At 2 symbols x 180 backfill minutes that is the
difference between one statement and 360.

A note on the upsert's WHERE clause: the update only fires when a value actually
changed. An unchanged re-send therefore produces *no* WAL record, which means it
produces no CDC event, which means the replay of a backfill does not flood Kafka
with no-op updates. This is the single cheapest optimisation in the pipeline.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime

import psycopg2
from psycopg2.extensions import connection as Connection
from psycopg2.extras import Json, execute_values

from ingestion.config import PostgresConfig
from ingestion.models import Candle, FxRate

log = logging.getLogger(__name__)

# Column order here is the contract that Candle.as_row() writes against. The two
# must stay in step; the round-trip is covered by the integration suite.
UPSERT_CANDLES = """
INSERT INTO crypto.market_candles_1m (
    symbol, open_time, close_time, open_price, high_price, low_price, close_price,
    volume, quote_volume, trade_count, taker_buy_base, taker_buy_quote, source
)
VALUES %s
ON CONFLICT (symbol, open_time) DO UPDATE SET
    close_time      = EXCLUDED.close_time,
    open_price      = EXCLUDED.open_price,
    high_price      = EXCLUDED.high_price,
    low_price       = EXCLUDED.low_price,
    close_price     = EXCLUDED.close_price,
    volume          = EXCLUDED.volume,
    quote_volume    = EXCLUDED.quote_volume,
    trade_count     = EXCLUDED.trade_count,
    taker_buy_base  = EXCLUDED.taker_buy_base,
    taker_buy_quote = EXCLUDED.taker_buy_quote,
    source          = EXCLUDED.source,
    updated_at      = now()
WHERE (crypto.market_candles_1m.close_price,
       crypto.market_candles_1m.high_price,
       crypto.market_candles_1m.low_price,
       crypto.market_candles_1m.volume,
       crypto.market_candles_1m.trade_count)
   IS DISTINCT FROM
      (EXCLUDED.close_price,
       EXCLUDED.high_price,
       EXCLUDED.low_price,
       EXCLUDED.volume,
       EXCLUDED.trade_count)
"""

UPSERT_FX = """
INSERT INTO crypto.fx_rates (base, quote, rate, as_of, source)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (base, quote) DO UPDATE SET
    rate       = EXCLUDED.rate,
    as_of      = EXCLUDED.as_of,
    source     = EXCLUDED.source,
    updated_at = now()
WHERE crypto.fx_rates.as_of < EXCLUDED.as_of
"""


class Database:
    """Thin, explicit wrapper over a single psycopg2 connection.

    A pool would be premature: this service is single-threaded by design, so a
    pool would add a failure mode (stale connections) without adding throughput.
    Reconnection is handled by letting the connection drop and rebuilding it,
    which is simpler to reason about than trying to heal one.
    """

    def __init__(self, config: PostgresConfig, connect_timeout: int = 10) -> None:
        self._config = config
        self._connect_timeout = connect_timeout
        self._conn: Connection | None = None

    # ------------------------------------------------------------------ #
    def connect(self) -> Connection:
        if self._conn is None or self._conn.closed:
            log.info("connecting to %s", self._config)
            self._conn = psycopg2.connect(
                self._config.dsn,
                connect_timeout=self._connect_timeout,
                application_name="crypto-ingestion",
            )
            self._conn.autocommit = False
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    @contextmanager
    def cursor(self) -> Iterator:
        """Transactional cursor. Commits on success, rolls back on any error.

        The connection is dropped on failure so the next call reconnects: a
        connection that raised mid-transaction is not reliably reusable, and
        pretending otherwise produces "current transaction is aborted" storms.
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except psycopg2.Error:
                self.close()
            raise

    # ------------------------------------------------------------------ #
    def upsert_candles(self, candles: Sequence[Candle], page_size: int = 500) -> int:
        """Upsert candles, returning the number of rows actually written.

        The returned count is rows *changed*, not rows offered: an unchanged
        re-send reports 0, which is exactly what the "rows written" metric
        should show for a no-op backfill.
        """
        if not candles:
            return 0

        rows = [candle.as_row() for candle in candles]
        with self.cursor() as cur:
            execute_values(cur, UPSERT_CANDLES, rows, page_size=page_size)
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def upsert_fx_rate(self, rate: FxRate) -> int:
        """Upsert an FX rate. The guard keeps a stale provider read from
        overwriting a newer one, which matters because the two free providers
        publish on different schedules."""
        with self.cursor() as cur:
            cur.execute(
                UPSERT_FX,
                (rate.base, rate.quote, rate.rate, rate.as_of, rate.source),
            )
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def record_rejects(self, rejects: Iterable[tuple[str, str | None, str, dict]]) -> int:
        """Quarantine rows that failed validation.

        Tuples are (source, symbol, reason, payload). Writing the payload keeps
        the failure diagnosable: "23 rows rejected" is an alert, "23 rows
        rejected and here is one of them" is a fix.
        """
        rows = [(src, sym, reason, Json(payload)) for src, sym, reason, payload in rejects]
        if not rows:
            return 0
        with self.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO crypto.ingest_rejects (source, symbol, reason, payload) VALUES %s",
                rows,
            )
            return len(rows)

    # ------------------------------------------------------------------ #
    def latest_open_time(self, symbol: str) -> datetime | None:
        """Newest candle held for a symbol, used to size the gap to heal."""
        with self.cursor() as cur:
            cur.execute(
                "SELECT max(open_time) FROM crypto.market_candles_1m WHERE symbol = %s",
                (symbol,),
            )
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    def ensure_symbol(self, symbol: str, base_asset: str, quote_asset: str) -> None:
        """Make sure the FK target exists before candles reference it.

        Also the reason the symbols dimension gets CDC traffic at all: a new
        pair appearing in SYMBOLS produces an INSERT event, which is the
        cheapest way to see the dimension flow end to end.
        """
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crypto.symbols (symbol, base_asset, quote_asset, display_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
                """,
                (symbol, base_asset, quote_asset, f"{base_asset} / {quote_asset}"),
            )

    def health(self) -> dict:
        """Snapshot used by the readiness probe and the integration tests."""
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)::bigint,
                       max(open_time),
                       max(ingested_at)
                FROM crypto.market_candles_1m
                """
            )
            count, newest, ingested = cur.fetchone()
        return {
            "candle_count": count,
            "newest_open_time": newest.isoformat() if newest else None,
            "last_ingested_at": ingested.isoformat() if ingested else None,
            "checked_at": datetime.now(UTC).isoformat(),
        }


def split_symbol(symbol: str) -> tuple[str, str]:
    """Best-effort split of a Binance pair into (base, quote).

    Binance concatenates without a separator, so this walks a list of known
    quote assets longest-first. Unknown shapes fall back to a 3-character quote
    rather than raising: getting the dimension label slightly wrong is far less
    harmful than refusing to ingest the pair at all.
    """
    for quote in ("USDT", "USDC", "FDUSD", "TUSD", "BUSD", "USD", "EUR", "BTC", "ETH", "BNB"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol[:-3], symbol[-3:]


def dumps(payload: object) -> str:
    """JSON with a deterministic key order, so reject payloads diff cleanly."""
    return json.dumps(payload, sort_keys=True, default=str)
