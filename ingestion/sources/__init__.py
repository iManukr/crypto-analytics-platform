"""Source registry.

Sources are resolved by name so the choice is a configuration value rather than
an import, and so a test can substitute a fake without touching the service
loop.
"""

from __future__ import annotations

from ingestion.config import Config
from ingestion.sources.base import CandleSource, SourceError, last_closed_minute, to_ms
from ingestion.sources.binance import BinanceSource
from ingestion.sources.replay import ReplaySource

__all__ = [
    "BinanceSource",
    "CandleSource",
    "ReplaySource",
    "SourceError",
    "build_source",
    "last_closed_minute",
    "to_ms",
]


def build_source(name: str, config: Config) -> CandleSource:
    """Instantiate a source by name.

    Raises ValueError rather than falling back to a default: a typo in
    INGEST_SOURCE should stop the service at startup, not quietly change where
    the data comes from.
    """
    if name == "binance":
        return BinanceSource(
            hosts=config.binance_hosts,
            timeout=config.http_timeout_seconds,
            max_retries=config.max_retries,
        )
    if name == "replay":
        return ReplaySource()
    raise ValueError(f"unknown source {name!r}; expected one of: binance, replay")
