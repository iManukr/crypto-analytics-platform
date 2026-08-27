"""Source abstraction.

Every source answers the same question - "give me the closed 1-minute candles
for this symbol in this time window" - so the service loop, the backfill logic
and the tests are all written once against this interface rather than against
Binance specifically.

That is also what makes the offline ``replay`` source a drop-in: CI and
network-restricted environments exercise the identical code path, with only the
bytes at the very edge differing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta

from ingestion.models import Candle

MINUTE_MS = 60_000


class SourceError(RuntimeError):
    """Raised when a source cannot satisfy a request after its own retries."""


class CandleSource(ABC):
    """A provider of closed 1-minute OHLCV bars."""

    #: Stable identifier used as a Prometheus label. Keep it low-cardinality.
    name: str = "base"

    @abstractmethod
    def fetch(self, symbol: str, start_ms: int, end_ms: int) -> list[Candle]:
        """Return closed candles with ``start_ms <= open_time < end_ms``.

        Implementations must:
          * return candles sorted by ``open_time`` ascending,
          * never return the still-forming current minute,
          * raise :class:`SourceError` rather than returning a partial result
            silently, so the caller can decide whether to retry or degrade.
        """

    def close(self) -> None:  # noqa: B027 - pragma: no cover
        # Concrete no-op on purpose: most sources hold nothing to release,
        # and making this abstract would force every one of them to write an
        # empty override.
        """Release any held resources (HTTP sessions, sockets)."""


def last_closed_minute(now: datetime | None = None) -> datetime:
    """The most recent minute that has definitely finished.

    A bar for minute *m* is only complete once *m+1* has started, so the newest
    trustworthy bar is always the previous minute, never the current one.
    Ingesting the in-flight minute is the classic way to end up with a candle
    whose volume changes after you have already written it.
    """
    now = now or datetime.now(UTC)
    floored = now.replace(second=0, microsecond=0)
    return floored - timedelta(minutes=1)


def to_ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def minute_range(start: datetime, end: datetime) -> list[datetime]:
    """Inclusive-exclusive list of minute boundaries, used for gap detection."""
    out: list[datetime] = []
    cursor = start.replace(second=0, microsecond=0)
    while cursor < end:
        out.append(cursor)
        cursor += timedelta(minutes=1)
    return out
