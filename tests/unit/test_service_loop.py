"""Tests for the ingestion loop itself.

`test_service.py` covers the pure decisions - which window to fetch, when to
degrade. This file covers the parts that actually move data and handle failure:
one cycle over the symbols, the FX refresh timer, startup waiting, and the
structured-log format that everything else is diagnosed through.

Everything is driven with stubs. The point is the control flow, and the real
Postgres/HTTP paths are covered by the integration suite.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from ingestion.config import Config, PostgresConfig
from ingestion.fx import FxError
from ingestion.models import Candle, FxRate
from ingestion.service import (
    IngestionService,
    JsonFormatter,
    ingest_window,
    refresh_fx,
)
from ingestion.sources.base import CandleSource, SourceError


# --------------------------------------------------------------------------- #
# Stubs                                                                        #
# --------------------------------------------------------------------------- #
def make_candle(open_time: datetime) -> Candle:
    return Candle(
        symbol="ETHUSDT",
        open_time=open_time,
        close_time=open_time + timedelta(seconds=59),
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


class StubDatabase:
    def __init__(self, newest=None, written=0, connect_failures=0):
        self.newest = newest
        self.written = written
        self.upserted: list[list[Candle]] = []
        self.fx_writes: list[FxRate] = []
        self.symbols: list[tuple] = []
        self.connect_attempts = 0
        self._connect_failures = connect_failures
        self.closed = False

    def connect(self):
        self.connect_attempts += 1
        if self.connect_attempts <= self._connect_failures:
            raise OSError("connection refused")
        return object()

    def latest_open_time(self, _symbol):
        return self.newest

    def upsert_candles(self, candles):
        self.upserted.append(list(candles))
        return self.written

    def upsert_fx_rate(self, rate):
        self.fx_writes.append(rate)
        return 1

    def ensure_symbol(self, symbol, base, quote):
        self.symbols.append((symbol, base, quote))

    def close(self):
        self.closed = True


class StubSource(CandleSource):
    name = "stub"

    def __init__(self, candles=None, error=None):
        self._candles = candles or []
        self._error = error
        self.calls = 0
        self.closed = False

    def fetch(self, symbol, start_ms, end_ms):
        self.calls += 1
        if self._error:
            raise self._error
        return list(self._candles)

    def close(self):
        self.closed = True


def make_config(**overrides) -> Config:
    defaults = {
        "source": "binance",
        "symbols": ["ETHUSDT"],
        "backfill_minutes": 60,
        "fx_refresh_seconds": 900,
        "fallback_after_failures": 2,
        "postgres": PostgresConfig("localhost", 5432, "crypto", "crypto_app", "pw"),
    }
    defaults.update(overrides)
    return Config(**defaults)


def make_service(config: Config, db: StubDatabase, source: StubSource) -> IngestionService:
    service = IngestionService(config)
    service.db = db
    service.source = source
    return service


# --------------------------------------------------------------------------- #
class TestStructuredLogging:
    def make_record(self, **kwargs) -> logging.LogRecord:
        record = logging.LogRecord(
            name="ingestion",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="landed %d candles",
            args=(7,),
            exc_info=None,
        )
        for key, value in kwargs.items():
            setattr(record, key, value)
        return record

    def test_emits_one_json_object_per_line(self):
        payload = json.loads(JsonFormatter().format(self.make_record()))
        assert payload["message"] == "landed 7 candles"
        assert payload["level"] == "INFO"
        assert payload["logger"] == "ingestion"
        assert "ts" in payload

    def test_extra_fields_are_promoted_to_top_level_keys(self):
        """Structured fields are what make 'show me every failure for ETHUSDT'
        a query rather than a regex over free text."""
        record = self.make_record(extra_fields={"symbol": "ETHUSDT", "written": 7})
        payload = json.loads(JsonFormatter().format(record))
        assert payload["symbol"] == "ETHUSDT"
        assert payload["written"] == 7

    def test_exceptions_are_included_rather_than_lost(self):
        try:
            raise ValueError("upstream exploded")
        except ValueError:
            import sys

            record = self.make_record(exc_info=sys.exc_info())
        payload = json.loads(JsonFormatter().format(record))
        assert "upstream exploded" in payload["exception"]

    def test_output_is_a_single_line(self):
        """A multi-line log record breaks every line-oriented log shipper."""
        record = self.make_record(extra_fields={"note": "line one\nline two"})
        assert "\n" not in JsonFormatter().format(record)


# --------------------------------------------------------------------------- #
class TestIngestWindow:
    def test_an_inverted_window_does_no_work(self):
        source = StubSource()
        now = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        assert ingest_window(StubDatabase(), source, "ETHUSDT", now, now) == 0
        assert source.calls == 0, "an empty window should not hit the API at all"

    def test_a_source_returning_nothing_writes_nothing(self):
        db = StubDatabase()
        now = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        assert ingest_window(db, StubSource([]), "ETHUSDT", now, now + timedelta(minutes=5)) == 0
        assert db.upserted == []

    def test_fetched_candles_are_written_and_the_changed_count_returned(self):
        start = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        candles = [make_candle(start + timedelta(minutes=i)) for i in range(3)]
        db = StubDatabase(written=3)

        written = ingest_window(
            db, StubSource(candles), "ETHUSDT", start, start + timedelta(minutes=3)
        )

        assert written == 3
        assert len(db.upserted[0]) == 3

    def test_reports_rows_changed_not_rows_offered(self):
        """An unchanged re-send must report zero. Reporting 3 would make a no-op
        backfill look like real throughput on the dashboard."""
        start = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        candles = [make_candle(start + timedelta(minutes=i)) for i in range(3)]
        db = StubDatabase(written=0)

        assert (
            ingest_window(db, StubSource(candles), "ETHUSDT", start, start + timedelta(minutes=3))
            == 0
        )
        assert len(db.upserted[0]) == 3, "the rows were still offered to the database"


# --------------------------------------------------------------------------- #
class TestFxRefresh:
    def test_a_provider_failure_is_not_fatal(self, monkeypatch):
        """FX is a side quest. Losing it must never stop candle ingestion."""
        monkeypatch.setattr(
            "ingestion.service.fetch_rate",
            lambda *a, **k: (_ for _ in ()).throw(FxError("all providers down")),
        )
        assert refresh_fx(StubDatabase(), make_config()) is False

    def test_a_successful_refresh_is_written(self, monkeypatch):
        rate = FxRate("USD", "KES", Decimal("129.34"), datetime(2026, 8, 26, tzinfo=UTC), "test")
        monkeypatch.setattr("ingestion.service.fetch_rate", lambda *a, **k: rate)

        db = StubDatabase()
        assert refresh_fx(db, make_config()) is True
        assert db.fx_writes == [rate]

    def test_the_refresh_timer_is_respected(self, monkeypatch):
        """The free providers publish daily. Polling them every cycle would burn
        the rate limit to learn nothing."""
        calls = []
        monkeypatch.setattr("ingestion.service.refresh_fx", lambda db, cfg: calls.append(1) or True)

        service = make_service(make_config(fx_refresh_seconds=3600), StubDatabase(), StubSource())
        service.maybe_refresh_fx()
        service.maybe_refresh_fx()
        service.maybe_refresh_fx()

        assert len(calls) == 1, f"expected one refresh inside the interval, got {len(calls)}"


# --------------------------------------------------------------------------- #
class TestCycle:
    def test_a_clean_cycle_writes_and_clears_the_failure_counter(self):
        newest = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=5)
        db = StubDatabase(newest=newest, written=2)
        source = StubSource([make_candle(newest + timedelta(minutes=1))])

        service = make_service(make_config(), db, source)
        service._consecutive_failures = 1

        assert service.cycle() == 2
        assert service._consecutive_failures == 0

    def test_a_source_failure_does_not_raise_out_of_the_cycle(self):
        """One bad symbol must not kill the loop for every other symbol."""
        service = make_service(
            make_config(symbols=["ETHUSDT", "BTCUSDT"]),
            StubDatabase(),
            StubSource(error=SourceError("all hosts failed")),
        )
        assert service.cycle() == 0

    def test_repeated_total_failure_degrades_to_the_replay_source(self):
        service = make_service(
            make_config(fallback_after_failures=2),
            StubDatabase(),
            StubSource(error=SourceError("geo-blocked")),
        )

        service.cycle()
        assert service.active_source.name == "stub", "one failure is not yet a degradation"

        service.cycle()
        assert service.active_source.name == "replay"

    def test_a_database_failure_is_counted_but_contained(self):
        class ExplodingDatabase(StubDatabase):
            def latest_open_time(self, _symbol):
                raise RuntimeError("relation does not exist")

        service = make_service(make_config(), ExplodingDatabase(), StubSource())
        assert service.cycle() == 0


# --------------------------------------------------------------------------- #
class TestStartup:
    def test_waits_for_a_database_that_is_not_ready_yet(self, monkeypatch):
        """Compose starts containers concurrently; Postgres accepting connections
        lags the container being 'up'. Crashing on the first refusal would put the
        service into a restart loop."""
        monkeypatch.setattr("ingestion.service.time.sleep", lambda _s: None)
        db = StubDatabase(connect_failures=3)

        make_service(make_config(), db, StubSource()).wait_for_database(timeout=60)

        assert db.connect_attempts == 4

    def test_gives_up_with_a_clear_error_rather_than_hanging(self, monkeypatch):
        monkeypatch.setattr("ingestion.service.time.sleep", lambda _s: None)
        clock = iter([0.0, 1.0, 2.0, 100.0, 200.0, 300.0])
        monkeypatch.setattr("ingestion.service.time.time", lambda: next(clock, 1000.0))

        service = make_service(make_config(), StubDatabase(connect_failures=99), StubSource())
        with pytest.raises(RuntimeError, match="did not become reachable"):
            service.wait_for_database(timeout=10)

    def test_a_stop_signal_is_recorded_rather_than_killing_the_process(self):
        """SIGTERM finishes the current cycle, so a container shutdown cannot
        leave a half-written batch."""
        service = make_service(make_config(), StubDatabase(), StubSource())
        assert service._stopping is False
        service.request_stop(15, None)
        assert service._stopping is True
