"""Domain model and the validation gate that sits in front of the database.

This is the pipeline's first data-quality checkpoint. Anything that fails here
never reaches Postgres, and therefore never reaches Kafka, ClickHouse or the
marts - it goes to ``crypto.ingest_rejects`` with the reason attached, so a bad
upstream response is visible as a metric rather than as a mysterious gap.

Validation is written as plain functions over a frozen dataclass rather than a
schema library, so each rule is independently unit-testable and the failure
message says what a human needs to know.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

MINUTE = timedelta(minutes=1)

# How far ahead of "now" a candle may legitimately be stamped. Clock skew
# between our host and the exchange is real; a full minute of tolerance absorbs
# it without letting genuinely nonsensical future data through.
FUTURE_TOLERANCE = timedelta(minutes=1)


class ValidationError(ValueError):
    """Raised when a payload cannot become a trustworthy Candle."""


@dataclass(frozen=True)
class Candle:
    """One closed OHLCV bar for a symbol at 1-minute grain."""

    symbol: str
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_base: Decimal
    taker_buy_quote: Decimal
    source: str

    def as_row(self) -> tuple:
        """Positional tuple matching the INSERT column order in db.py."""
        return (
            self.symbol,
            self.open_time,
            self.close_time,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume,
            self.quote_volume,
            self.trade_count,
            self.taker_buy_base,
            self.taker_buy_quote,
            self.source,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            k: (
                v.isoformat()
                if isinstance(v, datetime)
                else str(v)
                if isinstance(v, Decimal)
                else v
            )
            for k, v in asdict(self).items()
        }


@dataclass(frozen=True)
class FxRate:
    base: str
    quote: str
    rate: Decimal
    as_of: datetime
    source: str


# --------------------------------------------------------------------------- #
# Parsing                                                                      #
# --------------------------------------------------------------------------- #
def _decimal(value: Any, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError(f"{field}: {value!r} is not a number") from exc
    if not parsed.is_finite():
        raise ValidationError(f"{field}: {value!r} is not finite")
    return parsed


def _ms_to_utc(value: Any, field: str) -> datetime:
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    except (TypeError, ValueError, OSError, OverflowError) as exc:
        raise ValidationError(f"{field}: {value!r} is not a millisecond timestamp") from exc


def parse_binance_kline(symbol: str, kline: list, source: str = "binance") -> Candle:
    """Turn one Binance ``/api/v3/klines`` array into a validated Candle.

    Binance returns positional arrays, which are compact but unforgiving: a
    change in field order would silently shift every value. The length check
    plus the OHLC consistency rules in ``validate_candle`` are what catch that.
    """
    if not isinstance(kline, (list, tuple)):
        raise ValidationError(f"expected a kline array, got {type(kline).__name__}")
    if len(kline) < 11:
        raise ValidationError(f"kline array has {len(kline)} fields, expected at least 11")

    candle = Candle(
        symbol=symbol,
        open_time=_ms_to_utc(kline[0], "open_time"),
        close_time=_ms_to_utc(kline[6], "close_time"),
        open_price=_decimal(kline[1], "open_price"),
        high_price=_decimal(kline[2], "high_price"),
        low_price=_decimal(kline[3], "low_price"),
        close_price=_decimal(kline[4], "close_price"),
        volume=_decimal(kline[5], "volume"),
        quote_volume=_decimal(kline[7], "quote_volume"),
        trade_count=int(kline[8]),
        taker_buy_base=_decimal(kline[9], "taker_buy_base"),
        taker_buy_quote=_decimal(kline[10], "taker_buy_quote"),
        source=source,
    )
    validate_candle(candle)
    return candle


# --------------------------------------------------------------------------- #
# Validation rules                                                             #
# --------------------------------------------------------------------------- #
def validate_candle(candle: Candle, now: datetime | None = None) -> None:
    """Assert every invariant a 1-minute OHLCV bar must satisfy.

    Each rule exists because violating it would produce a plausible-looking but
    wrong number somewhere downstream:

    * non-positive prices break log-returns in the ML feature model,
    * high < low means the bar is internally inconsistent and any range or ATR
      derived from it is garbage,
    * a misaligned open_time silently creates a second bucket for the same
      minute and double-counts volume in the 5m rollup,
    * a future-stamped bar poisons the freshness metric, which is the signal we
      page on.
    """
    now = now or datetime.now(UTC)

    if not candle.symbol or len(candle.symbol) > 20:
        raise ValidationError(f"symbol {candle.symbol!r} is empty or longer than 20 chars")

    for name in ("open_price", "high_price", "low_price", "close_price"):
        value: Decimal = getattr(candle, name)
        if value <= 0:
            raise ValidationError(f"{name} must be > 0, got {value}")

    for name in ("volume", "quote_volume", "taker_buy_base", "taker_buy_quote"):
        value = getattr(candle, name)
        if value < 0:
            raise ValidationError(f"{name} must be >= 0, got {value}")

    if candle.trade_count < 0:
        raise ValidationError(f"trade_count must be >= 0, got {candle.trade_count}")

    if candle.high_price < candle.low_price:
        raise ValidationError(
            f"high_price {candle.high_price} is below low_price {candle.low_price}"
        )
    if candle.high_price < max(candle.open_price, candle.close_price):
        raise ValidationError("high_price is below open/close")
    if candle.low_price > min(candle.open_price, candle.close_price):
        raise ValidationError("low_price is above open/close")

    # Taker buy volume is a subset of total volume, so it can never exceed it.
    if candle.taker_buy_base > candle.volume:
        raise ValidationError(
            f"taker_buy_base {candle.taker_buy_base} exceeds volume {candle.volume}"
        )

    if candle.open_time.second != 0 or candle.open_time.microsecond != 0:
        raise ValidationError(f"open_time {candle.open_time.isoformat()} is not minute-aligned")

    if candle.close_time <= candle.open_time:
        raise ValidationError("close_time must be after open_time")
    if candle.close_time - candle.open_time > MINUTE:
        raise ValidationError(
            f"bar spans {candle.close_time - candle.open_time}, expected at most one minute"
        )

    if candle.open_time > now + FUTURE_TOLERANCE:
        raise ValidationError(f"open_time {candle.open_time.isoformat()} is in the future")


def validate_fx(rate: FxRate, now: datetime | None = None) -> None:
    now = now or datetime.now(UTC)
    if rate.rate <= 0:
        raise ValidationError(f"fx rate must be > 0, got {rate.rate}")
    # A currency pair moving by more than 100x is a provider bug, not a market
    # event. Bound it so a malformed response cannot reprice the whole mart.
    if rate.rate > Decimal("1000000"):
        raise ValidationError(f"fx rate {rate.rate} is implausibly large")
    if rate.as_of > now + FUTURE_TOLERANCE:
        raise ValidationError(f"fx as_of {rate.as_of.isoformat()} is in the future")
    if not rate.base or not rate.quote:
        raise ValidationError("fx base and quote must both be set")
