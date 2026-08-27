"""Source-layer tests: resilience behaviour and generator determinism.

The Binance tests use a fake session rather than a live call. That is the point:
a test that hits the real API tests the network, is slow, and goes red for
reasons that have nothing to do with the code under review.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import requests

from ingestion.config import Config
from ingestion.models import validate_candle
from ingestion.sources import build_source
from ingestion.sources.base import MINUTE_MS, last_closed_minute, minute_range
from ingestion.sources.binance import BinanceSource, SourceError
from ingestion.sources.replay import ReplaySource

OPEN_MS = 1_756_200_000_000


def make_kline(open_ms: int, close: str = "1868.50") -> list:
    return [
        open_ms,
        "1865.20",
        "1870.00",
        "1860.00",
        close,
        "120.50",
        open_ms + 59_999,
        "225000.00",
        350,
        "60.25",
        "112500.00",
    ]


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else []
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    """Replays a scripted sequence of responses and records the requests made."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[tuple[str, dict]] = []
        self.headers = {}

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params or {}))
        item = self._responses.pop(0) if self._responses else FakeResponse(200, [])
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        pass


# --------------------------------------------------------------------------- #
class TestTimeHelpers:
    def test_last_closed_minute_never_returns_the_in_flight_minute(self):
        """A bar for minute m is only complete once m+1 has started. Ingesting
        the current minute produces a row whose volume changes after we wrote it."""
        now = datetime(2026, 8, 26, 12, 30, 45, tzinfo=UTC)
        assert last_closed_minute(now) == datetime(2026, 8, 26, 12, 29, tzinfo=UTC)

    def test_last_closed_minute_on_an_exact_boundary(self):
        now = datetime(2026, 8, 26, 12, 30, 0, tzinfo=UTC)
        assert last_closed_minute(now) == datetime(2026, 8, 26, 12, 29, tzinfo=UTC)

    def test_minute_range_is_inclusive_exclusive(self):
        start = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
        got = minute_range(start, start + timedelta(minutes=3))
        assert len(got) == 3
        assert got[0] == start
        assert got[-1] == start + timedelta(minutes=2)


# --------------------------------------------------------------------------- #
class TestBinanceResilience:
    def build(self, responses, hosts=None):
        return BinanceSource(
            hosts=hosts or ["https://a.example", "https://b.example"],
            max_retries=2,
            session=FakeSession(responses),
            sleep=lambda _s: None,  # no real sleeping in tests
        )

    def test_happy_path(self):
        source = self.build([FakeResponse(200, [make_kline(OPEN_MS)])])
        candles = source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + MINUTE_MS)
        assert len(candles) == 1
        assert candles[0].source == "binance"

    def test_geo_block_fails_over_to_the_next_host(self):
        """451 is the geo-block Binance returns in restricted jurisdictions. It
        must move to the next host, not abort the cycle."""
        session = FakeSession(
            [
                FakeResponse(451, text="restricted"),
                FakeResponse(200, [make_kline(OPEN_MS)]),
            ]
        )
        source = BinanceSource(
            hosts=["https://blocked.example", "https://ok.example"],
            max_retries=2,
            session=session,
            sleep=lambda _s: None,
        )
        candles = source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + MINUTE_MS)
        assert len(candles) == 1
        assert session.calls[0][0].startswith("https://blocked.example")
        assert session.calls[1][0].startswith("https://ok.example")

    def test_rate_limit_honours_retry_after(self):
        slept: list[float] = []
        session = FakeSession(
            [
                FakeResponse(429, headers={"Retry-After": "7"}),
                FakeResponse(200, [make_kline(OPEN_MS)]),
            ]
        )
        source = BinanceSource(
            hosts=["https://a.example"],
            max_retries=3,
            session=session,
            sleep=slept.append,
        )
        source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + MINUTE_MS)
        assert 7.0 in slept, f"Retry-After was ignored; slept {slept}"

    def test_timeouts_are_retried(self):
        source = self.build(
            [
                requests.Timeout("timed out"),
                FakeResponse(200, [make_kline(OPEN_MS)]),
            ]
        )
        assert len(source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + MINUTE_MS)) == 1

    def test_exhausting_every_host_raises(self):
        source = self.build([FakeResponse(503) for _ in range(10)])
        with pytest.raises(SourceError, match="host"):
            source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + MINUTE_MS)

    def test_a_client_error_is_not_retried(self):
        """A bad symbol is our bug. Retrying it only burns rate-limit quota."""
        session = FakeSession([FakeResponse(400, text="Invalid symbol")])
        source = BinanceSource(
            hosts=["https://a.example"],
            max_retries=3,
            session=session,
            sleep=lambda _s: None,
        )
        with pytest.raises(SourceError, match="400"):
            source.fetch("BADPAIR", OPEN_MS, OPEN_MS + MINUTE_MS)
        assert len(session.calls) == 1

    def test_a_malformed_bar_does_not_discard_the_whole_page(self):
        good = make_kline(OPEN_MS)
        bad = make_kline(OPEN_MS + MINUTE_MS)
        bad[2] = "1.00"  # high below low
        also_good = make_kline(OPEN_MS + 2 * MINUTE_MS)

        source = self.build([FakeResponse(200, [good, bad, also_good])])
        candles = source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + 3 * MINUTE_MS)
        assert len(candles) == 2, "one bad bar should not sink its two good neighbours"

    def test_pagination_advances_past_the_last_bar_seen(self):
        """Advancing by an assumed page size spins forever on a short page."""
        page1 = [make_kline(OPEN_MS + i * MINUTE_MS) for i in range(3)]
        session = FakeSession([FakeResponse(200, page1), FakeResponse(200, [])])
        source = BinanceSource(
            hosts=["https://a.example"],
            max_retries=1,
            session=session,
            sleep=lambda _s: None,
        )
        candles = source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + 3 * MINUTE_MS)
        assert len(candles) == 3
        assert len(session.calls) == 1, "a short page means the window is done"

    def test_bars_at_or_after_the_end_bound_are_dropped(self):
        page = [make_kline(OPEN_MS), make_kline(OPEN_MS + MINUTE_MS)]
        source = self.build([FakeResponse(200, page)])
        candles = source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + MINUTE_MS)
        assert [c.open_time.timestamp() * 1000 for c in candles] == [OPEN_MS]

    def test_results_are_sorted_by_open_time(self):
        page = [make_kline(OPEN_MS + 2 * MINUTE_MS), make_kline(OPEN_MS)]
        source = self.build([FakeResponse(200, page)])
        candles = source.fetch("ETHUSDT", OPEN_MS, OPEN_MS + 3 * MINUTE_MS)
        assert candles == sorted(candles, key=lambda c: c.open_time)


# --------------------------------------------------------------------------- #
class TestReplayDeterminism:
    def test_the_same_minute_always_yields_the_same_candle(self):
        """Determinism is what makes the idempotent upsert genuinely a no-op on
        a re-run, and what lets CI assert on an exact value."""
        a = ReplaySource().candle_for("ETHUSDT", OPEN_MS)
        b = ReplaySource().candle_for("ETHUSDT", OPEN_MS)
        assert a == b

    def test_different_symbols_diverge(self):
        eth = ReplaySource().candle_for("ETHUSDT", OPEN_MS)
        btc = ReplaySource().candle_for("BTCUSDT", OPEN_MS)
        assert eth.close_price != btc.close_price

    def test_generated_candles_satisfy_every_production_invariant(self):
        source = ReplaySource()
        for i in range(500):
            validate_candle(
                source.candle_for("ETHUSDT", OPEN_MS + i * MINUTE_MS),
                now=datetime.now(UTC) + timedelta(days=3650),
            )

    def test_bars_are_contiguous(self):
        """Each bar's close is the next bar's open, so returns are continuous
        and the moving averages in the ML mart are not full of artificial gaps."""
        source = ReplaySource()
        first = source.candle_for("ETHUSDT", OPEN_MS)
        second = source.candle_for("ETHUSDT", OPEN_MS + MINUTE_MS)
        assert first.close_price == second.open_price

    def test_every_row_is_labelled_as_synthetic(self):
        """Provenance flows to the marts, so nobody mistakes this for market data."""
        assert ReplaySource().candle_for("ETHUSDT", OPEN_MS).source == "replay"

    def test_fetch_snaps_to_the_minute_grid(self):
        candles = ReplaySource().fetch("ETHUSDT", OPEN_MS + 31_000, OPEN_MS + 3 * MINUTE_MS)
        assert all(c.open_time.second == 0 for c in candles)


# --------------------------------------------------------------------------- #
class TestSourceRegistry:
    def test_builds_known_sources(self):
        config = Config()
        assert build_source("replay", config).name == "replay"
        assert build_source("binance", config).name == "binance"

    def test_an_unknown_source_raises_rather_than_defaulting(self):
        """A typo in INGEST_SOURCE must stop the service, not quietly change
        where the data comes from."""
        with pytest.raises(ValueError, match="unknown source"):
            build_source("bnance", Config())
