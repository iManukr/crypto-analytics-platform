"""Validation gate tests.

These matter more than most unit tests here, because this is the only place in
the pipeline where a bad row can still be stopped cheaply. Once a value is in
Postgres it becomes a CDC event, then a ClickHouse row, then part of a moving
average - and by then the cost of the mistake is a backfill, not a rejection.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from ingestion.models import (
    Candle,
    FxRate,
    ValidationError,
    parse_binance_kline,
    validate_candle,
    validate_fx,
)

OPEN_MS = 1_756_200_000_000  # a minute-aligned epoch millisecond


def kline(**overrides) -> list:
    """A well-formed Binance kline array, with targeted field overrides."""
    base = [
        OPEN_MS,  # 0 open time
        "1865.20",  # 1 open
        "1870.00",  # 2 high
        "1860.00",  # 3 low
        "1868.50",  # 4 close
        "120.50",  # 5 volume
        OPEN_MS + 59_999,  # 6 close time
        "225000.00",  # 7 quote volume
        350,  # 8 trade count
        "60.25",  # 9 taker buy base
        "112500.00",  # 10 taker buy quote
    ]
    for index, value in overrides.items():
        base[int(index)] = value
    return base


def candle(**overrides) -> Candle:
    defaults = {
        "symbol": "ETHUSDT",
        "open_time": datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
        "close_time": datetime(2026, 8, 26, 12, 0, 59, tzinfo=UTC),
        "open_price": Decimal("1865.20"),
        "high_price": Decimal("1870.00"),
        "low_price": Decimal("1860.00"),
        "close_price": Decimal("1868.50"),
        "volume": Decimal("120.50"),
        "quote_volume": Decimal("225000.00"),
        "trade_count": 350,
        "taker_buy_base": Decimal("60.25"),
        "taker_buy_quote": Decimal("112500.00"),
        "source": "binance",
    }
    defaults.update(overrides)
    return Candle(**defaults)


NOW = datetime(2026, 8, 26, 12, 30, tzinfo=UTC)


# --------------------------------------------------------------------------- #
class TestParsing:
    def test_parses_a_well_formed_kline(self):
        result = parse_binance_kline("ETHUSDT", kline())
        assert result.symbol == "ETHUSDT"
        assert result.close_price == Decimal("1868.50")
        assert result.trade_count == 350
        assert result.source == "binance"
        assert result.open_time.tzinfo is UTC

    def test_prices_keep_full_decimal_precision(self):
        """Float parsing would silently round an 8dp price. Decimal must not."""
        result = parse_binance_kline("ETHUSDT", kline(**{"4": "1868.12345678"}))
        assert result.close_price == Decimal("1868.12345678")
        assert str(result.close_price) == "1868.12345678"

    def test_rejects_a_short_array(self):
        with pytest.raises(ValidationError, match="expected at least 11"):
            parse_binance_kline("ETHUSDT", kline()[:5])

    def test_rejects_a_non_array(self):
        with pytest.raises(ValidationError, match="expected a kline array"):
            parse_binance_kline("ETHUSDT", {"open": 1})

    def test_rejects_a_non_numeric_price(self):
        with pytest.raises(ValidationError, match="not a number"):
            parse_binance_kline("ETHUSDT", kline(**{"4": "not-a-price"}))

    def test_rejects_nan(self):
        with pytest.raises(ValidationError, match="not finite"):
            parse_binance_kline("ETHUSDT", kline(**{"4": "NaN"}))


# --------------------------------------------------------------------------- #
class TestCandleInvariants:
    def test_a_good_candle_passes(self):
        validate_candle(candle(), now=NOW)

    @pytest.mark.parametrize("field", ["open_price", "high_price", "low_price", "close_price"])
    def test_prices_must_be_positive(self, field):
        with pytest.raises(ValidationError, match="must be > 0"):
            validate_candle(candle(**{field: Decimal("0")}), now=NOW)

    def test_negative_volume_is_rejected(self):
        with pytest.raises(ValidationError, match="volume must be >= 0"):
            validate_candle(candle(volume=Decimal("-1")), now=NOW)

    def test_high_below_low_is_rejected(self):
        with pytest.raises(ValidationError, match="below low_price"):
            validate_candle(candle(high_price=Decimal("1800"), low_price=Decimal("1900")), now=NOW)

    def test_high_below_the_body_is_rejected(self):
        """A high that is not the maximum makes every derived range wrong."""
        with pytest.raises(ValidationError, match="high_price is below open/close"):
            validate_candle(candle(high_price=Decimal("1866.00")), now=NOW)

    def test_low_above_the_body_is_rejected(self):
        with pytest.raises(ValidationError, match="low_price is above open/close"):
            validate_candle(candle(low_price=Decimal("1867.00")), now=NOW)

    def test_taker_volume_cannot_exceed_total_volume(self):
        with pytest.raises(ValidationError, match="exceeds volume"):
            validate_candle(candle(taker_buy_base=Decimal("999")), now=NOW)

    def test_misaligned_open_time_is_rejected(self):
        """An off-grid timestamp creates a second bucket for the same minute."""
        with pytest.raises(ValidationError, match="not minute-aligned"):
            validate_candle(
                candle(open_time=datetime(2026, 8, 26, 12, 0, 30, tzinfo=UTC)),
                now=NOW,
            )

    def test_a_bar_spanning_more_than_a_minute_is_rejected(self):
        with pytest.raises(ValidationError, match="expected at most one minute"):
            validate_candle(candle(close_time=datetime(2026, 8, 26, 12, 5, tzinfo=UTC)), now=NOW)

    def test_close_time_before_open_time_is_rejected(self):
        with pytest.raises(ValidationError, match="close_time must be after"):
            validate_candle(candle(close_time=datetime(2026, 8, 26, 11, 59, tzinfo=UTC)), now=NOW)

    def test_future_timestamps_are_rejected(self):
        """A future-dated row pins the freshness metric at zero forever, so the
        staleness alert can never fire again. Silent monitoring failure."""
        future = NOW + timedelta(minutes=30)
        with pytest.raises(ValidationError, match="in the future"):
            validate_candle(
                candle(open_time=future, close_time=future + timedelta(seconds=59)), now=NOW
            )

    def test_small_clock_skew_is_tolerated(self):
        """Our clock and the exchange's will not agree exactly. One minute of
        tolerance absorbs that without letting nonsense through. The bar must
        still be minute-aligned - skew shifts which minute is current, it does
        not make a bar start at :30."""
        skewed = NOW + timedelta(minutes=1)
        validate_candle(
            candle(open_time=skewed, close_time=skewed + timedelta(seconds=59)), now=NOW
        )


# --------------------------------------------------------------------------- #
class TestFxValidation:
    def rate(self, **overrides) -> FxRate:
        defaults = {
            "base": "USD",
            "quote": "KES",
            "rate": Decimal("129.34"),
            "as_of": NOW,
            "source": "test",
        }
        defaults.update(overrides)
        return FxRate(**defaults)

    def test_a_plausible_rate_passes(self):
        validate_fx(self.rate(), now=NOW)

    def test_non_positive_rate_is_rejected(self):
        with pytest.raises(ValidationError, match="must be > 0"):
            validate_fx(self.rate(rate=Decimal("0")), now=NOW)

    def test_implausibly_large_rate_is_rejected(self):
        """A decimal-point slip at the provider would otherwise reprice every
        converted figure in the mart."""
        with pytest.raises(ValidationError, match="implausibly large"):
            validate_fx(self.rate(rate=Decimal("99999999")), now=NOW)

    def test_future_as_of_is_rejected(self):
        with pytest.raises(ValidationError, match="in the future"):
            validate_fx(self.rate(as_of=NOW + timedelta(days=1)), now=NOW)
