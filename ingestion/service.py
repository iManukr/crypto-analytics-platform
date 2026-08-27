"""The ingestion loop.

Shape of the thing:

    startup -> wait for Postgres -> ensure dimension rows -> heal history
            -> [ fetch -> validate -> upsert -> publish metrics ] every N seconds

Two behaviours are worth calling out because they are what make this survive a
laptop lid closing, which is the realistic failure mode:

**Gap healing.** The window to fetch is derived from what is *actually in the
database* (``max(open_time)``), not from a cursor held in memory. So an outage
of any length is healed by simply starting up again: the service asks Postgres
what it is missing and goes and gets it. No state file to corrupt, no offset to
reset.

**Degradation is loud.** When the live API fails repeatedly the service switches
to the offline replay source rather than going silent, but it sets
``ingest_active_source{source="replay"} = 1`` when it does. Silent degradation
is worse than an outage, because nobody investigates a dashboard that is still
drawing a line.

The module is importable: ``run_backfill`` is what the Airflow DAG calls, so the
orchestrated batch path and the streaming path share one implementation rather
than drifting apart.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime, timedelta

from ingestion import metrics
from ingestion.config import Config
from ingestion.db import Database, split_symbol
from ingestion.fx import FxError, fetch_rate
from ingestion.models import Candle
from ingestion.sources import SourceError, build_source, last_closed_minute, to_ms
from ingestion.sources.base import CandleSource

log = logging.getLogger("ingestion")


# --------------------------------------------------------------------------- #
# Logging                                                                      #
# --------------------------------------------------------------------------- #
class JsonFormatter(logging.Formatter):
    """One JSON object per line.

    Container logs get scraped, not read by a human at a terminal. Structured
    output means "show me every database error in the last hour" is a query
    rather than a regex.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key, value in getattr(record, "extra_fields", {}).items():
            payload[key] = value
        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # psycopg2 and urllib3 are chatty at DEBUG and say nothing useful at INFO.
    logging.getLogger("urllib3").setLevel(logging.WARNING)


# --------------------------------------------------------------------------- #
# Core operations                                                              #
# --------------------------------------------------------------------------- #
def _write_candles(db: Database, candles: list[Candle], symbol: str) -> int:
    written = db.upsert_candles(candles)
    metrics.ROWS_WRITTEN.labels(table="market_candles_1m", symbol=symbol).inc(written)
    return written


def ingest_window(
    db: Database,
    source: CandleSource,
    symbol: str,
    start: datetime,
    end: datetime,
) -> int:
    """Fetch and land ``[start, end)`` for one symbol. Returns rows changed."""
    if start >= end:
        return 0

    candles = source.fetch(symbol, to_ms(start), to_ms(end))
    if not candles:
        log.info(
            "no candles returned for %s in window",
            symbol,
            extra={
                "extra_fields": {
                    "symbol": symbol,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                }
            },
        )
        return 0

    written = _write_candles(db, candles, symbol)
    log.info(
        "landed %d/%d candles for %s",
        written,
        len(candles),
        symbol,
        extra={
            "extra_fields": {
                "symbol": symbol,
                "fetched": len(candles),
                "written": written,
                "source": source.name,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
            }
        },
    )
    return written


def pending_window(
    db: Database, symbol: str, max_backfill_minutes: int, now: datetime | None = None
) -> tuple[datetime, datetime]:
    """Work out what is missing for a symbol, bounded by the backfill budget.

    Bounding matters: an empty database or a very old one would otherwise ask
    for years of history on first start and hammer the API. The budget caps a
    single run; repeated runs walk further back if that is what is wanted.
    """
    end = last_closed_minute(now) + timedelta(minutes=1)  # exclusive
    newest = db.latest_open_time(symbol)

    if newest is None:
        start = end - timedelta(minutes=max_backfill_minutes or 1)
    else:
        if newest.tzinfo is None:
            newest = newest.replace(tzinfo=UTC)
        start = newest + timedelta(minutes=1)
        earliest_allowed = end - timedelta(minutes=max_backfill_minutes or 1)
        if start < earliest_allowed:
            log.warning(
                "gap for %s exceeds the backfill budget; healing the most recent %d minutes",
                symbol,
                max_backfill_minutes,
                extra={"extra_fields": {"symbol": symbol, "oldest_missing": start.isoformat()}},
            )
            start = earliest_allowed

    gap_minutes = max(0, int((end - start).total_seconds() // 60))
    metrics.BACKFILL_GAP.labels(symbol=symbol).set(gap_minutes)
    return start, end


def refresh_fx(database: Database, config: Config) -> bool:
    """Refresh the FX pair. Returns True when a rate was actually written.

    False is the normal case most of the time: the free providers publish daily,
    so the upsert guard suppresses the write until the rate genuinely moves.
    """
    try:
        rate = fetch_rate(config.fx_base, config.fx_quote, timeout=config.http_timeout_seconds)
    except FxError as exc:
        metrics.ERRORS.labels(kind="api").inc()
        log.warning("fx refresh failed: %s", exc)
        return False

    changed = database.upsert_fx_rate(rate)
    metrics.ROWS_WRITTEN.labels(table="fx_rates", symbol=f"{rate.base}{rate.quote}").inc(changed)
    return bool(changed)


def run_backfill(config: Config, minutes: int | None = None) -> dict:
    """Batch reconciliation entry point. Imported and called by the Airflow DAG.

    Deliberately the same code the streaming loop runs, so the scheduled catch-up
    cannot develop different semantics from the continuous path.
    """
    config.validate()
    budget = minutes if minutes is not None else config.backfill_minutes
    db = Database(config.postgres)
    source = build_source(config.source, config)
    summary: dict[str, int] = {}

    try:
        for symbol in config.symbols:
            base, quote = split_symbol(symbol)
            db.ensure_symbol(symbol, base, quote)
            start, end = pending_window(db, symbol, budget)
            summary[symbol] = ingest_window(db, source, symbol, start, end)
    finally:
        source.close()
        db.close()

    return summary


# --------------------------------------------------------------------------- #
# Service                                                                      #
# --------------------------------------------------------------------------- #
class IngestionService:
    def __init__(self, config: Config) -> None:
        config.validate()
        self.config = config
        self.db = Database(config.postgres)
        self.source = build_source(config.source, config)
        self._fallback: CandleSource | None = None
        self._consecutive_failures = 0
        self._last_fx_refresh = 0.0
        self._stopping = False

    # -------------------------------------------------------------- lifecycle
    def request_stop(self, signum, _frame) -> None:
        log.info("received signal %s; finishing the current cycle then exiting", signum)
        self._stopping = True

    def wait_for_database(self, timeout: int = 120) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.db.connect()
                log.info("database is reachable")
                return
            except Exception as exc:  # any connect failure here is retryable
                log.info("waiting for the database: %s", exc)
                time.sleep(3)
        raise RuntimeError(f"database did not become reachable within {timeout}s")

    # ------------------------------------------------------------------ source
    @property
    def active_source(self) -> CandleSource:
        return self._fallback or self.source

    def _note_failure(self) -> None:
        self._consecutive_failures += 1
        metrics.CONSECUTIVE_FAILURES.set(self._consecutive_failures)

        budget = self.config.fallback_after_failures
        if budget and self._consecutive_failures >= budget and self._fallback is None:
            log.error(
                "live source failed %d times in a row; degrading to the offline replay source. "
                "Rows produced from here are stamped source='replay' and are NOT market data.",
                self._consecutive_failures,
                extra={"extra_fields": {"degraded": True}},
            )
            self._fallback = build_source("replay", self.config)
            metrics.set_active_source("replay")

    def _note_success(self) -> None:
        if self._fallback is not None:
            log.info("live source recovered; leaving the replay fallback")
            self._fallback.close()
            self._fallback = None
            metrics.set_active_source(self.config.source)
        self._consecutive_failures = 0
        metrics.CONSECUTIVE_FAILURES.set(0)

    # ------------------------------------------------------------------- cycle
    def cycle(self) -> int:
        """One pass over every symbol. Returns total rows written."""
        total = 0
        failures = 0

        for symbol in self.config.symbols:
            try:
                start, end = pending_window(self.db, symbol, self.config.backfill_minutes)
                total += ingest_window(self.db, self.active_source, symbol, start, end)

                newest = self.db.latest_open_time(symbol)
                if newest:
                    if newest.tzinfo is None:
                        newest = newest.replace(tzinfo=UTC)
                    lag = (datetime.now(UTC) - newest).total_seconds()
                    metrics.SOURCE_LAG.labels(symbol=symbol).set(lag)
            except SourceError as exc:
                failures += 1
                metrics.ERRORS.labels(kind="api").inc()
                log.error("source failure for %s: %s", symbol, exc)
            except Exception as exc:  # a bad symbol must not kill the loop
                failures += 1
                metrics.ERRORS.labels(kind="database").inc()
                log.exception("failed to ingest %s: %s", symbol, exc)

        if failures == 0:
            self._note_success()
            metrics.CYCLES.labels(outcome="success").inc()
        elif failures < len(self.config.symbols):
            metrics.CYCLES.labels(outcome="partial").inc()
        else:
            self._note_failure()
            metrics.CYCLES.labels(outcome="failed").inc()

        if total:
            metrics.LAST_SUCCESS.set(time.time())
        return total

    def maybe_refresh_fx(self) -> None:
        if time.time() - self._last_fx_refresh < self.config.fx_refresh_seconds:
            return
        self._last_fx_refresh = time.time()
        try:
            refresh_fx(self.db, self.config)
        except Exception as exc:  # FX is a side quest, never fatal
            metrics.ERRORS.labels(kind="unexpected").inc()
            log.warning("fx refresh raised: %s", exc)

    # -------------------------------------------------------------------- run
    def run(self) -> int:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)

        metrics.serve(self.config.metrics_port)
        metrics.set_active_source(self.config.source)
        metrics.UP.set(1)
        log.info(
            "ingestion starting",
            extra={
                "extra_fields": {
                    "source": self.config.source,
                    "symbols": self.config.symbols,
                    "interval_seconds": self.config.interval_seconds,
                    "backfill_minutes": self.config.backfill_minutes,
                    "metrics_port": self.config.metrics_port,
                }
            },
        )

        self.wait_for_database()
        for symbol in self.config.symbols:
            base, quote = split_symbol(symbol)
            self.db.ensure_symbol(symbol, base, quote)

        try:
            while not self._stopping:
                started = time.monotonic()
                self.cycle()
                self.maybe_refresh_fx()

                # Sleep the remainder of the interval in short slices so a
                # SIGTERM is honoured within a second rather than after a full
                # cycle interval. Container shutdowns have a 10s grace period.
                elapsed = time.monotonic() - started
                remaining = max(0.0, self.config.interval_seconds - elapsed)
                while remaining > 0 and not self._stopping:
                    nap = min(1.0, remaining)
                    time.sleep(nap)
                    remaining -= nap
        finally:
            metrics.UP.set(0)
            self.active_source.close()
            self.source.close()
            self.db.close()
            log.info("ingestion stopped cleanly")

        return 0


def main() -> int:
    config = Config.from_env()
    configure_logging(config.log_level)

    if os.environ.get("INGEST_ONCE", "").lower() in {"1", "true", "yes"}:
        # One-shot mode, used by the CI smoke test and by `make ingest-once`.
        summary = run_backfill(config)
        log.info("one-shot ingestion complete", extra={"extra_fields": {"written": summary}})
        return 0

    return IngestionService(config).run()


if __name__ == "__main__":
    sys.exit(main())
