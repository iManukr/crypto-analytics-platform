"""Service-loop tests: gap healing, degradation, and configuration handling.

The gap-healing tests are the important ones. The recovery story for this
pipeline is "restart it and it works out what it missed", which is only true if
``pending_window`` derives the window from the database rather than from
remembered state.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from ingestion.config import Config, PostgresConfig
from ingestion.db import split_symbol
from ingestion.service import IngestionService, pending_window

NOW = datetime(2026, 8, 26, 12, 30, 45, tzinfo=UTC)
# last_closed_minute(NOW) is 12:29, so the exclusive end is always 12:30.
EXPECTED_END = datetime(2026, 8, 26, 12, 30, tzinfo=UTC)


class StubDatabase:
    """Only the one method pending_window actually uses."""

    def __init__(self, newest: datetime | None = None):
        self.newest = newest
        self.queried: list[str] = []

    def latest_open_time(self, symbol: str):
        self.queried.append(symbol)
        return self.newest


# --------------------------------------------------------------------------- #
class TestPendingWindow:
    def test_empty_database_requests_the_full_backfill_budget(self):
        start, end = pending_window(StubDatabase(None), "ETHUSDT", 180, now=NOW)
        assert end == EXPECTED_END
        assert start == EXPECTED_END - timedelta(minutes=180)

    def test_window_resumes_from_the_minute_after_the_newest_row(self):
        newest = datetime(2026, 8, 26, 12, 20, tzinfo=UTC)
        start, end = pending_window(StubDatabase(newest), "ETHUSDT", 180, now=NOW)
        assert start == newest + timedelta(minutes=1)
        assert end == EXPECTED_END

    def test_a_fully_caught_up_database_asks_for_nothing(self):
        """Start >= end means no fetch, so a caught-up loop makes no API call."""
        newest = datetime(2026, 8, 26, 12, 29, tzinfo=UTC)
        start, end = pending_window(StubDatabase(newest), "ETHUSDT", 180, now=NOW)
        assert start >= end

    def test_a_gap_longer_than_the_budget_is_clamped(self):
        """A months-old database must not ask the API for months of history in
        one go. The budget caps a single run; repeated runs walk further back."""
        newest = datetime(2026, 1, 1, tzinfo=UTC)
        start, end = pending_window(StubDatabase(newest), "ETHUSDT", 60, now=NOW)
        assert start == EXPECTED_END - timedelta(minutes=60)
        assert (end - start) == timedelta(minutes=60)

    def test_naive_timestamps_from_the_driver_are_treated_as_utc(self):
        """psycopg2 can hand back a naive datetime depending on column type.
        Treating that as local time would silently shift the whole window."""
        naive = datetime(2026, 8, 26, 12, 20)
        start, _ = pending_window(StubDatabase(naive), "ETHUSDT", 180, now=NOW)
        assert start == datetime(2026, 8, 26, 12, 21, tzinfo=UTC)

    def test_zero_budget_does_not_produce_an_inverted_window(self):
        start, end = pending_window(StubDatabase(None), "ETHUSDT", 0, now=NOW)
        assert start < end


# --------------------------------------------------------------------------- #
class TestSymbolSplitting:
    @pytest.mark.parametrize(
        "symbol,expected",
        [
            ("ETHUSDT", ("ETH", "USDT")),
            ("BTCUSDT", ("BTC", "USDT")),
            ("ETHBTC", ("ETH", "BTC")),
            ("BNBFDUSD", ("BNB", "FDUSD")),
            ("EURUSD", ("EUR", "USD")),
        ],
    )
    def test_known_quote_assets_split_correctly(self, symbol, expected):
        assert split_symbol(symbol) == expected

    def test_an_unknown_shape_degrades_rather_than_raising(self):
        """A slightly wrong dimension label is far less harmful than refusing
        to ingest the pair at all."""
        base, quote = split_symbol("WEIRDPAIR")
        assert base and quote
        assert base + quote == "WEIRDPAIR"


# --------------------------------------------------------------------------- #
class TestDegradation:
    def build(self, **overrides) -> IngestionService:
        config = Config(
            source="binance",
            binance_hosts=["https://a.example"],
            symbols=["ETHUSDT"],
            fallback_after_failures=overrides.get("fallback_after_failures", 3),
            postgres=PostgresConfig("localhost", 5432, "crypto", "u", "p"),
        )
        return IngestionService(config)

    def test_falls_back_to_replay_after_the_configured_failure_budget(self):
        service = self.build()
        assert service.active_source.name == "binance"

        service._note_failure()
        service._note_failure()
        assert service.active_source.name == "binance", "must not degrade early"

        service._note_failure()
        assert service.active_source.name == "replay"

    def test_recovery_returns_to_the_live_source(self):
        service = self.build()
        for _ in range(3):
            service._note_failure()
        assert service.active_source.name == "replay"

        service._note_success()
        assert service.active_source.name == "binance"

    def test_fallback_can_be_disabled(self):
        """With the budget at 0 the service stays broken and visible rather
        than quietly emitting synthetic data."""
        service = self.build(fallback_after_failures=0)
        for _ in range(50):
            service._note_failure()
        assert service.active_source.name == "binance"


# --------------------------------------------------------------------------- #
class TestConfig:
    def test_parses_the_documented_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            config = Config.from_env()
        assert config.source == "binance"
        assert config.symbols == ["ETHUSDT"]
        config.validate()

    def test_reads_and_normalises_the_environment(self):
        env = {
            "INGEST_SOURCE": "REPLAY",
            "SYMBOLS": " ethusdt , btcusdt ,, ",
            "BACKFILL_MINUTES": "45",
            "FX_QUOTE": "kes",
        }
        with patch.dict(os.environ, env, clear=True):
            config = Config.from_env()
        assert config.source == "replay"
        assert config.symbols == ["ETHUSDT", "BTCUSDT"], "blank entries must be dropped"
        assert config.backfill_minutes == 45
        assert config.fx_quote == "KES"

    def test_an_empty_value_falls_back_to_the_default(self):
        """`SYMBOLS=` in a .env file is a common way to accidentally blank a
        setting. Treating empty as unset avoids an empty symbol list."""
        with patch.dict(os.environ, {"SYMBOLS": ""}, clear=True):
            assert Config.from_env().symbols == ["ETHUSDT"]

    def test_a_non_integer_fails_loudly_at_startup(self):
        with patch.dict(os.environ, {"BACKFILL_MINUTES": "soon"}, clear=True):
            with pytest.raises(ValueError, match="must be an integer"):
                Config.from_env()

    def test_an_unknown_source_is_rejected_by_validate(self):
        with pytest.raises(ValueError, match="INGEST_SOURCE"):
            Config(source="kraken").validate()

    def test_the_password_never_appears_in_a_repr(self):
        """Config objects get logged and end up in tracebacks."""
        config = PostgresConfig("h", 5432, "db", "user", "hunter2")
        assert "hunter2" not in repr(config)
        assert "hunter2" in config.dsn, "the DSN itself still needs the real value"


# --------------------------------------------------------------------------- #
class TestInsertContract:
    """The INSERT column list and Candle.as_row() are a positional contract.

    If they drift, psycopg2 does not raise - it happily writes the values into
    the wrong columns, and a high_price ends up in volume. Nothing downstream
    would flag it as a type error, so it has to be checked here.
    """

    def test_column_count_matches_the_row_tuple(self):
        import re

        from ingestion.db import UPSERT_CANDLES
        from tests.unit.test_models import candle

        columns = re.search(r"market_candles_1m \(([^)]*)\)", UPSERT_CANDLES).group(1)
        names = [c.strip() for c in columns.replace("\n", " ").split(",") if c.strip()]
        assert len(names) == len(candle().as_row())

    def test_column_order_matches_the_row_tuple(self):
        import re

        from ingestion.db import UPSERT_CANDLES
        from tests.unit.test_models import candle

        columns = re.search(r"market_candles_1m \(([^)]*)\)", UPSERT_CANDLES).group(1)
        names = [c.strip() for c in columns.replace("\n", " ").split(",") if c.strip()]
        row = candle().as_row()
        # Compare the two distinctive values that would be silently swapped.
        assert row[names.index("symbol")] == "ETHUSDT"
        assert row[names.index("trade_count")] == 350
        assert row[names.index("source")] == "binance"

    def test_no_stray_comment_leaked_into_the_sql(self):
        """Guards against a lint directive being inserted inside the string."""
        from ingestion.db import UPSERT_CANDLES

        assert "noqa" not in UPSERT_CANDLES
        assert "#" not in UPSERT_CANDLES
