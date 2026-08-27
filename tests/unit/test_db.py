"""Database-writer tests.

Driven against a fake connection rather than a live Postgres: the behaviour worth
pinning here is transaction handling and the shape of the statements, and neither
needs a server. The statements themselves are exercised against a real database
by the integration suite.

The transaction rules matter because getting them wrong produces the two worst
failure modes in a long-running ingester: a connection stuck in an aborted
transaction that fails every subsequent write, and a partial batch committed as
if it were whole.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import psycopg2
import pytest

from ingestion.config import PostgresConfig
from ingestion.db import UPSERT_CANDLES, UPSERT_FX, Database, dumps, split_symbol
from ingestion.models import Candle, FxRate


class FakeCursor:
    def __init__(self, conn, rowcount=1, fetch=None):
        self._conn = conn
        self.rowcount = rowcount
        self.executed: list[tuple] = []
        self._fetch = fetch

    @property
    def connection(self):
        # psycopg2.extras.execute_values reads cur.connection.encoding to encode
        # the statement, so the fake has to expose the same link a real one does.
        return self._conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self._conn.fail_on_execute:
            raise psycopg2.OperationalError("connection lost")

    def fetchone(self):
        return self._fetch

    def mogrify(self, template, args):
        return str((template, args)).encode()


class FakeConnection:
    encoding = "UTF8"

    def __init__(self, rowcount=1, fetch=None, fail_on_execute=False):
        self.closed = False
        self.autocommit = False
        self.commits = 0
        self.rollbacks = 0
        self.fail_on_execute = fail_on_execute
        self._cursor = FakeCursor(self, rowcount=rowcount, fetch=fetch)

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def make_db(conn: FakeConnection) -> Database:
    db = Database(PostgresConfig("localhost", 5432, "crypto", "crypto_app", "pw"))
    db._conn = conn
    return db


CANDLE = Candle(
    symbol="ETHUSDT",
    open_time=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
    close_time=datetime(2026, 8, 26, 12, 0, 59, tzinfo=UTC),
    open_price=Decimal("1865.20"),
    high_price=Decimal("1870.00"),
    low_price=Decimal("1860.00"),
    close_price=Decimal("1868.50"),
    volume=Decimal("120.50"),
    quote_volume=Decimal("225000.00"),
    trade_count=350,
    taker_buy_base=Decimal("60.25"),
    taker_buy_quote=Decimal("112500.00"),
    source="binance",
)


# --------------------------------------------------------------------------- #
class TestTransactionHandling:
    def test_a_successful_block_commits_once(self):
        conn = FakeConnection()
        db = make_db(conn)
        with db.cursor() as cur:
            cur.execute("SELECT 1")
        assert (conn.commits, conn.rollbacks) == (1, 0)

    def test_a_failing_block_rolls_back_and_does_not_commit(self):
        conn = FakeConnection()
        db = make_db(conn)
        with pytest.raises(ValueError), db.cursor():
            raise ValueError("business logic blew up")
        assert conn.commits == 0
        assert conn.rollbacks == 1

    def test_the_error_propagates_rather_than_being_swallowed(self):
        """A write that silently fails is indistinguishable from a gap in the
        source data, and the gap-healing logic would never fix it."""
        conn = FakeConnection(fail_on_execute=True)
        db = make_db(conn)
        with pytest.raises(psycopg2.OperationalError), db.cursor() as cur:
            cur.execute("INSERT INTO whatever VALUES (1)")

    def test_a_connection_that_cannot_roll_back_is_discarded(self):
        """Reusing a connection that raised mid-transaction produces endless
        'current transaction is aborted' errors. Dropping it forces a reconnect."""

        class Unrollbackable(FakeConnection):
            def rollback(self):
                raise psycopg2.InterfaceError("connection already closed")

        conn = Unrollbackable()
        db = make_db(conn)
        # The original failure is what propagates; the rollback failure is a
        # cleanup detail and must not mask the actual diagnosis.
        with pytest.raises(RuntimeError, match="boom"), db.cursor():
            raise RuntimeError("boom")
        assert db._conn is None, "the poisoned connection should have been dropped"


# --------------------------------------------------------------------------- #
class TestWrites:
    def test_an_empty_batch_does_no_work(self):
        conn = FakeConnection()
        db = make_db(conn)
        assert db.upsert_candles([]) == 0
        assert conn.commits == 0, "an empty batch should not open a transaction"

    def test_empty_rejects_do_no_work(self):
        conn = FakeConnection()
        assert make_db(conn).record_rejects([]) == 0
        assert conn.commits == 0

    def test_a_negative_rowcount_is_reported_as_zero(self):
        """psycopg2 returns -1 when a statement reports no row count. Passing
        that straight into a Prometheus counter would decrement it, and counters
        may only go up."""
        conn = FakeConnection(rowcount=-1)
        assert make_db(conn).upsert_candles([CANDLE]) == 0

    def test_fx_upsert_passes_the_natural_key_and_the_value(self):
        conn = FakeConnection(rowcount=1)
        db = make_db(conn)
        rate = FxRate("USD", "KES", Decimal("129.34"), datetime(2026, 8, 26, tzinfo=UTC), "test")

        assert db.upsert_fx_rate(rate) == 1
        _sql, params = conn._cursor.executed[0]
        assert params == ("USD", "KES", Decimal("129.34"), rate.as_of, "test")

    def test_latest_open_time_normalises_a_missing_row_to_none(self):
        conn = FakeConnection(fetch=(None,))
        assert make_db(conn).latest_open_time("ETHUSDT") is None

    def test_latest_open_time_returns_the_stored_timestamp(self):
        newest = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        conn = FakeConnection(fetch=(newest,))
        assert make_db(conn).latest_open_time("ETHUSDT") == newest


# --------------------------------------------------------------------------- #
class TestStatementSemantics:
    def test_the_candle_upsert_is_idempotent_on_the_natural_key(self):
        assert "ON CONFLICT (symbol, open_time) DO UPDATE" in UPSERT_CANDLES

    def test_the_candle_upsert_skips_writes_that_change_nothing(self):
        """The IS DISTINCT FROM guard is the cheapest optimisation in the whole
        pipeline: an unchanged re-send produces no WAL record, so a replayed
        backfill does not flood Kafka with no-op CDC updates."""
        assert "IS DISTINCT FROM" in UPSERT_CANDLES

    def test_the_fx_upsert_refuses_to_go_backwards(self):
        """The two free providers publish on different schedules, so a stale
        read must never overwrite a newer rate."""
        assert "WHERE crypto.fx_rates.as_of < EXCLUDED.as_of" in UPSERT_FX

    def test_updated_at_is_refreshed_on_every_real_change(self):
        assert "updated_at      = now()" in UPSERT_CANDLES
        assert "updated_at = now()" in UPSERT_FX


# --------------------------------------------------------------------------- #
class TestHelpers:
    @pytest.mark.parametrize(
        ("symbol", "expected"),
        [
            ("ETHUSDT", ("ETH", "USDT")),
            ("BTCUSDT", ("BTC", "USDT")),
            ("ETHBTC", ("ETH", "BTC")),
            ("EURUSD", ("EUR", "USD")),
        ],
    )
    def test_known_quote_assets_split_longest_first(self, symbol, expected):
        assert split_symbol(symbol) == expected

    def test_an_unknown_pair_degrades_instead_of_raising(self):
        """Getting a dimension label slightly wrong is far less harmful than
        refusing to ingest the pair at all."""
        assert split_symbol("FOOBAR") == ("FOO", "BAR")

    def test_dumps_is_deterministic_so_reject_payloads_diff_cleanly(self):
        assert dumps({"b": 2, "a": 1}) == dumps({"a": 1, "b": 2}) == '{"a": 1, "b": 2}'

    def test_dumps_handles_values_json_cannot_serialise(self):
        assert "1865.20" in dumps({"price": Decimal("1865.20")})
