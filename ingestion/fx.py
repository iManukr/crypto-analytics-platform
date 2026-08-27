"""FX rate fetcher.

Why there is an FX leg at all: it gives the pipeline a second, structurally
different table. Candles are append-only and high-volume; ``crypto.fx_rates``
is a current-value table that is *rewritten in place*. That makes it the only
part of the stack that exercises the CDC UPDATE path end to end, and therefore
the only thing that actually proves the ReplacingMergeTree version column in
ClickHouse is doing its job rather than just being configured.

Providers are both free and keyless, tried in order:

  1. https://open.er-api.com/v6/latest/{base}
  2. https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/{base}.json

Both publish roughly once a day. That is a real limitation and it is stated in
the docs rather than papered over: the KES figure in the marts moves
minute-to-minute because *ETH* moves, while the FX leg steps daily. Swapping in
a paid tick-level feed is a change to this module and nothing else.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

import requests

from ingestion import metrics
from ingestion.models import FxRate, ValidationError, validate_fx

log = logging.getLogger(__name__)

SOURCE_NAME = "fx"


class FxError(RuntimeError):
    """Raised when no provider could supply a usable rate."""


def _er_api(session: requests.Session, base: str, quote: str, timeout: int) -> FxRate:
    response = session.get(f"https://open.er-api.com/v6/latest/{base}", timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if payload.get("result") != "success":
        raise FxError(f"er-api returned result={payload.get('result')!r}")

    rate = payload.get("rates", {}).get(quote)
    if rate is None:
        raise FxError(f"er-api has no {base}->{quote} rate")

    # Use the provider's own publish time, not ours. Storing our fetch time
    # would make every poll look like a new rate and destroy the "has this
    # actually moved" signal that the upsert guard depends on.
    published = payload.get("time_last_update_unix")
    as_of = datetime.fromtimestamp(int(published), tz=UTC) if published else datetime.now(UTC)
    return FxRate(base, quote, Decimal(str(rate)), as_of, "open.er-api.com")


def _fawaz(session: requests.Session, base: str, quote: str, timeout: int) -> FxRate:
    url = (
        "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/"
        f"{base.lower()}.json"
    )
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    rates = payload.get(base.lower(), {})
    rate = rates.get(quote.lower())
    if rate is None:
        raise FxError(f"fawazahmed0 has no {base}->{quote} rate")

    date = payload.get("date")
    as_of = datetime.fromisoformat(date).replace(tzinfo=UTC) if date else datetime.now(UTC)
    return FxRate(base, quote, Decimal(str(rate)), as_of, "fawazahmed0/currency-api")


PROVIDERS = (_er_api, _fawaz)


def fetch_rate(
    base: str,
    quote: str,
    timeout: int = 15,
    session: requests.Session | None = None,
) -> FxRate:
    """Fetch a validated rate, trying each provider in turn.

    Returns the first rate that passes validation. A provider that answers with
    a structurally valid but implausible number (a decimal-point slip, say) is
    treated as a failure and the next provider is tried, because a bad rate
    silently reprices every KES figure in the mart.
    """
    session = session or requests.Session()
    errors: list[str] = []

    for provider in PROVIDERS:
        name = provider.__name__.lstrip("_")
        try:
            rate = provider(session, base, quote, timeout)
            validate_fx(rate)
            metrics.API_REQUESTS.labels(source=SOURCE_NAME, outcome="success").inc()
            log.info("fx %s->%s = %s (as_of %s, via %s)", base, quote, rate.rate, rate.as_of, name)
            return rate
        except ValidationError as exc:
            metrics.ROWS_REJECTED.labels(table="fx_rates", reason="validation").inc()
            errors.append(f"{name}: rejected by validation ({exc})")
        except requests.RequestException as exc:
            metrics.API_REQUESTS.labels(source=SOURCE_NAME, outcome="transport_error").inc()
            errors.append(f"{name}: {exc}")
        except (FxError, ValueError, KeyError) as exc:
            metrics.API_REQUESTS.labels(source=SOURCE_NAME, outcome="http_error").inc()
            errors.append(f"{name}: {exc}")

    raise FxError(f"no FX provider could supply {base}->{quote}: {'; '.join(errors)}")
