"""Binance public REST source.

Endpoint: ``GET /api/v3/klines`` - public, keyless, no authentication of any
kind. Documented at https://developers.binance.com/docs/binance-spot-api-docs.

Three things this handles that a naive ``requests.get`` loop does not:

1. **Host failover.** ``api.binance.com`` is geo-restricted in a number of
   jurisdictions, and CI runners frequently sit in one of them. The host list is
   walked on failure, so a 451 from one endpoint tries the next rather than
   ending the cycle.
2. **Rate limits.** Binance answers 429 (and 418 once it has started banning)
   with a ``Retry-After``. Honouring it is the difference between backing off and
   escalating into an IP ban.
3. **The 1000-candle cap.** A backfill window longer than 1000 minutes has to be
   paginated; the loop advances by the last returned ``open_time`` rather than by
   an assumed page size, so a short page cannot cause an infinite loop.
"""

from __future__ import annotations

import logging
import random
import time

import requests

from ingestion import metrics
from ingestion.models import Candle, ValidationError, parse_binance_kline
from ingestion.sources.base import MINUTE_MS, CandleSource, SourceError

log = logging.getLogger(__name__)

MAX_LIMIT = 1000
# Status codes worth trying the next host for. 451 is the geo-block, 403 is the
# WAF, 5xx is Binance having a bad day.
FAILOVER_STATUSES = {403, 418, 451, 500, 502, 503, 504}


class BinanceSource(CandleSource):
    name = "binance"

    def __init__(
        self,
        hosts: list[str],
        timeout: int = 15,
        max_retries: int = 4,
        session: requests.Session | None = None,
        sleep=time.sleep,
    ) -> None:
        if not hosts:
            raise ValueError("BinanceSource requires at least one host")
        self._hosts = list(hosts)
        self._timeout = timeout
        self._max_retries = max_retries
        self._sleep = sleep
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": "crypto-analytics-platform/1.0"})
        # Index of the host that last worked, so a healthy host is not
        # re-discovered from scratch on every call.
        self._preferred = 0

    # ------------------------------------------------------------------ #
    def fetch(self, symbol: str, start_ms: int, end_ms: int) -> list[Candle]:
        candles: list[Candle] = []
        cursor = start_ms

        while cursor < end_ms:
            page = self._fetch_page(symbol, cursor, end_ms)
            if not page:
                break

            for kline in page:
                open_ms = int(kline[0])
                if open_ms >= end_ms:
                    continue  # still-forming minute; base.last_closed_minute excludes it
                try:
                    candles.append(parse_binance_kline(symbol, kline, source=self.name))
                except ValidationError as exc:
                    # A single malformed bar must not sink the page. Count it,
                    # log it, keep the good rows; the caller quarantines it.
                    metrics.ROWS_REJECTED.labels(
                        table="market_candles_1m", reason="source_validation"
                    ).inc()
                    log.warning("dropping malformed kline for %s: %s", symbol, exc)

            # Advance past the last bar we actually saw. Trusting the page size
            # instead would spin forever whenever Binance returns a short page.
            last_open = int(page[-1][0])
            next_cursor = last_open + MINUTE_MS
            if next_cursor <= cursor:
                break
            cursor = next_cursor

            if len(page) < MAX_LIMIT:
                break

        candles.sort(key=lambda c: c.open_time)
        return candles

    # ------------------------------------------------------------------ #
    def _fetch_page(self, symbol: str, start_ms: int, end_ms: int) -> list:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms - 1,  # endTime is inclusive on Binance
            "limit": MAX_LIMIT,
        }

        attempts = self._max_retries * len(self._hosts)
        last_error: Exception | None = None

        for attempt in range(attempts):
            host = self._hosts[(self._preferred + attempt) % len(self._hosts)]
            url = f"{host}/api/v3/klines"
            started = time.perf_counter()

            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
                metrics.API_LATENCY.labels(source=self.name).observe(time.perf_counter() - started)

                if response.status_code == 200:
                    metrics.API_REQUESTS.labels(source=self.name, outcome="success").inc()
                    self._preferred = (self._preferred + attempt) % len(self._hosts)
                    payload = response.json()
                    if not isinstance(payload, list):
                        raise SourceError(f"expected a JSON array from {url}, got {type(payload)}")
                    return payload

                metrics.API_REQUESTS.labels(source=self.name, outcome="http_error").inc()
                last_error = SourceError(f"HTTP {response.status_code} from {url}")

                if response.status_code in (429, 418):
                    # Explicit rate limit. Respect Retry-After when present -
                    # guessing here is how a 429 becomes an IP ban.
                    delay = float(response.headers.get("Retry-After", self._backoff(attempt)))
                    log.warning("rate limited by %s, sleeping %.1fs", host, delay)
                    self._sleep(delay)
                    continue

                if response.status_code in FAILOVER_STATUSES:
                    log.warning(
                        "%s returned %s; failing over to the next host",
                        host,
                        response.status_code,
                    )
                    continue

                # 4xx that is not a rate limit or a block is our bug (bad symbol,
                # bad params). Retrying it just burns quota.
                raise SourceError(f"HTTP {response.status_code} from {url}: {response.text[:200]}")

            except requests.Timeout as exc:
                metrics.API_REQUESTS.labels(source=self.name, outcome="timeout").inc()
                last_error = exc
                log.warning("timeout from %s (attempt %d/%d)", host, attempt + 1, attempts)
            except requests.RequestException as exc:
                metrics.API_REQUESTS.labels(source=self.name, outcome="transport_error").inc()
                last_error = exc
                log.warning("transport error from %s: %s", host, exc)

            self._sleep(self._backoff(attempt))

        raise SourceError(f"all {len(self._hosts)} Binance host(s) failed: {last_error}")

    @staticmethod
    def _backoff(attempt: int) -> float:
        """Exponential backoff with full jitter, capped at 30s.

        Full jitter (rather than a fixed multiplier) matters when several
        symbols retry at once: without it they synchronise and hit the API in a
        thundering herd at exactly the same moments.
        """
        return random.uniform(0, min(30.0, 0.5 * (2**attempt)))  # noqa: S311 - jitter, not crypto

    def close(self) -> None:
        self._session.close()
