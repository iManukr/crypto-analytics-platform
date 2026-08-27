"""Deterministic offline candle generator.

Purpose, stated plainly: this is **not** real market data and is never presented
as such. Every row it produces is stamped ``source = 'replay'``, which flows all
the way through to the marts, so any consumer can filter it out and the Grafana
panels show which source a number came from.

It exists for two reasons:

* **CI must be hermetic.** The end-to-end integration job asserts that rows move
  Postgres -> Kafka -> ClickHouse -> staging -> marts. Making that assertion
  depend on a third-party API being reachable from a GitHub runner turns a data
  pipeline test into a network test, and produces flaky red builds that people
  learn to ignore.
* **A reviewer behind a geo-block still sees the pipeline work.** Binance blocks
  several jurisdictions outright. Degrading to this source beats showing an
  empty dashboard and being unable to tell whether the pipeline or the network
  is at fault - and the ``ingest_active_source`` metric makes the degradation
  loud rather than silent.

Determinism is the design constraint: the candle for a given (symbol, minute) is
a pure function of those two values. No accumulated state, no RNG seeded once at
startup. That means re-running a backfill produces byte-identical rows, so the
idempotent upsert really is a no-op, and a test can assert on an exact value.
"""

from __future__ import annotations

import hashlib
import math
import struct
from datetime import UTC, datetime
from decimal import Decimal

from ingestion.models import Candle, validate_candle
from ingestion.sources.base import MINUTE_MS, CandleSource

# Plausible starting levels, so a dashboard built against replay data looks
# like the real thing rather than like noise around 1.0.
BASE_PRICES: dict[str, float] = {
    "ETHUSDT": 1865.0,
    "BTCUSDT": 61500.0,
}
DEFAULT_BASE_PRICE = 100.0


def _unit_noise(*parts: object) -> float:
    """A stable float in [-1, 1) derived from the arguments.

    ``hash()`` is deliberately avoided: Python salts string hashing per process,
    so it would produce different candles on every restart and break the whole
    point of this module.
    """
    digest = hashlib.sha256(":".join(str(p) for p in parts).encode()).digest()
    (value,) = struct.unpack(">Q", digest[:8])
    return (value / 2**63) - 1.0


def _price_at(symbol: str, minute_index: int) -> float:
    """A smooth, stateless price curve.

    Superposed sinusoids of co-prime-ish periods (a day, ~1.6h, ~13min) give
    something with visible trend, cycle and chop, which exercises the moving
    averages and volatility features in the ML mart. A small hashed term stops
    it from being perfectly periodic.
    """
    base = BASE_PRICES.get(symbol, DEFAULT_BASE_PRICE)
    daily = 0.020 * math.sin(2 * math.pi * minute_index / 1440)
    hourly = 0.008 * math.sin(2 * math.pi * minute_index / 97)
    chop = 0.003 * math.sin(2 * math.pi * minute_index / 13)
    jitter = 0.0015 * _unit_noise(symbol, minute_index)
    return base * math.exp(daily + hourly + chop + jitter)


def _q(value: float, places: str = "0.00000001") -> Decimal:
    return Decimal(str(value)).quantize(Decimal(places))


class ReplaySource(CandleSource):
    name = "replay"

    def fetch(self, symbol: str, start_ms: int, end_ms: int) -> list[Candle]:
        candles: list[Candle] = []
        # Snap to the minute grid so a caller passing an arbitrary millisecond
        # still gets aligned bars.
        cursor = (start_ms // MINUTE_MS) * MINUTE_MS

        while cursor < end_ms:
            candles.append(self.candle_for(symbol, cursor))
            cursor += MINUTE_MS

        return candles

    # ------------------------------------------------------------------ #
    def candle_for(self, symbol: str, open_ms: int) -> Candle:
        """The candle for one minute. Pure function of (symbol, open_ms)."""
        minute_index = open_ms // MINUTE_MS

        open_price = _price_at(symbol, minute_index)
        close_price = _price_at(symbol, minute_index + 1)

        # Intrabar range: at least the open/close spread, widened by a hashed
        # amount so high/low are never degenerate.
        spread = abs(close_price - open_price)
        wick = open_price * 0.0006 * abs(_unit_noise("wick", symbol, minute_index))
        high_price = max(open_price, close_price) + spread * 0.35 + wick
        low_price = min(open_price, close_price) - spread * 0.35 - wick
        low_price = max(low_price, 0.01)  # never let the invariant price > 0 break

        volume = 40.0 + 35.0 * abs(_unit_noise("vol", symbol, minute_index))
        # Taker-buy share stays inside (0.2, 0.8): it is a fraction of total
        # volume by definition, and the validator enforces <= volume.
        taker_share = 0.5 + 0.3 * _unit_noise("taker", symbol, minute_index)
        taker_share = min(max(taker_share, 0.2), 0.8)
        taker_base = volume * taker_share

        mid = (high_price + low_price) / 2
        trade_count = 200 + int(300 * abs(_unit_noise("trades", symbol, minute_index)))

        candle = Candle(
            symbol=symbol,
            open_time=datetime.fromtimestamp(open_ms / 1000, tz=UTC),
            close_time=datetime.fromtimestamp((open_ms + MINUTE_MS - 1) / 1000, tz=UTC),
            open_price=_q(open_price),
            high_price=_q(high_price),
            low_price=_q(low_price),
            close_price=_q(close_price),
            volume=_q(volume),
            quote_volume=_q(volume * mid),
            trade_count=trade_count,
            taker_buy_base=_q(taker_base),
            taker_buy_quote=_q(taker_base * mid),
            source=self.name,
        )
        # Self-check: the generator is held to exactly the same invariants as
        # the live source. If a formula change here would produce an impossible
        # bar, it fails at the generator rather than three layers downstream.
        validate_candle(candle)
        return candle
