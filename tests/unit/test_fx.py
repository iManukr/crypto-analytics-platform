"""FX fetcher tests.

The FX leg is small but it is the only thing that exercises the CDC *update*
path, and a bad rate silently reprices every converted figure in the marts. So
what matters here is not the happy path - it is that a broken provider is
detected and skipped rather than trusted.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
import requests

from ingestion import fx
from ingestion.fx import FxError, fetch_rate

PUBLISHED_UNIX = 1_756_166_400  # 2026-08-26T00:00:00Z


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    """Answers by URL substring, so a test can break one provider and not the other."""

    def __init__(self, by_host: dict):
        self._by_host = by_host
        self.urls: list[str] = []

    def get(self, url, timeout=None):
        self.urls.append(url)
        for fragment, response in self._by_host.items():
            if fragment in url:
                if isinstance(response, Exception):
                    raise response
                return response
        raise requests.ConnectionError(f"no stub for {url}")


ER_API_OK = FakeResponse(
    {
        "result": "success",
        "time_last_update_unix": PUBLISHED_UNIX,
        "rates": {"KES": 129.34, "EUR": 0.92},
    }
)

FAWAZ_OK = FakeResponse({"date": "2026-08-26", "usd": {"kes": 128.90}})


class TestPrimaryProvider:
    def test_returns_a_validated_rate(self):
        session = FakeSession({"er-api.com": ER_API_OK})
        rate = fetch_rate("USD", "KES", session=session)

        assert rate.rate == Decimal("129.34")
        assert rate.base == "USD"
        assert rate.quote == "KES"
        assert rate.source == "open.er-api.com"

    def test_uses_the_providers_publish_time_not_our_fetch_time(self):
        """as_of must be when the PROVIDER published.

        Stamping our own fetch time would make every poll look like a new rate,
        which defeats the upsert guard and destroys the ability to tell whether
        the rate actually moved.
        """
        session = FakeSession({"er-api.com": ER_API_OK})
        rate = fetch_rate("USD", "KES", session=session)
        assert rate.as_of == datetime.fromtimestamp(PUBLISHED_UNIX, tz=UTC)

    def test_a_result_other_than_success_is_treated_as_a_failure(self):
        session = FakeSession(
            {
                "er-api.com": FakeResponse({"result": "error", "error-type": "unsupported-code"}),
                "fawazahmed0": FAWAZ_OK,
            }
        )
        rate = fetch_rate("USD", "KES", session=session)
        assert rate.source == "fawazahmed0/currency-api"

    def test_a_missing_currency_pair_falls_through(self):
        session = FakeSession(
            {
                "er-api.com": FakeResponse(
                    {"result": "success", "time_last_update_unix": PUBLISHED_UNIX, "rates": {}}
                ),
                "fawazahmed0": FAWAZ_OK,
            }
        )
        assert fetch_rate("USD", "KES", session=session).rate == Decimal("128.90")


class TestFallbackChain:
    def test_a_transport_error_moves_to_the_next_provider(self):
        session = FakeSession(
            {
                "er-api.com": requests.ConnectionError("DNS failure"),
                "fawazahmed0": FAWAZ_OK,
            }
        )
        rate = fetch_rate("USD", "KES", session=session)
        assert rate.source == "fawazahmed0/currency-api"
        assert len(session.urls) == 2

    def test_an_http_error_moves_to_the_next_provider(self):
        session = FakeSession(
            {"er-api.com": FakeResponse({}, status_code=503), "fawazahmed0": FAWAZ_OK}
        )
        assert fetch_rate("USD", "KES", session=session).rate == Decimal("128.90")

    def test_an_implausible_rate_is_rejected_and_the_next_provider_tried(self):
        """A structurally valid but nonsensical number is the dangerous case:
        nothing errors, and the bad rate silently reprices the whole mart."""
        session = FakeSession(
            {
                "er-api.com": FakeResponse(
                    {
                        "result": "success",
                        "time_last_update_unix": PUBLISHED_UNIX,
                        "rates": {"KES": 99_999_999},
                    }
                ),
                "fawazahmed0": FAWAZ_OK,
            }
        )
        rate = fetch_rate("USD", "KES", session=session)
        assert rate.source == "fawazahmed0/currency-api"

    def test_a_negative_rate_is_rejected(self):
        session = FakeSession(
            {
                "er-api.com": FakeResponse(
                    {
                        "result": "success",
                        "time_last_update_unix": PUBLISHED_UNIX,
                        "rates": {"KES": -5},
                    }
                ),
                "fawazahmed0": FAWAZ_OK,
            }
        )
        assert fetch_rate("USD", "KES", session=session).rate > 0

    def test_every_provider_failing_raises_with_all_the_reasons(self):
        session = FakeSession(
            {
                "er-api.com": requests.ConnectionError("down"),
                "fawazahmed0": FakeResponse({}, status_code=500),
            }
        )
        with pytest.raises(FxError) as excinfo:
            fetch_rate("USD", "KES", session=session)

        message = str(excinfo.value)
        assert "er_api" in message and "fawaz" in message, (
            f"the error must name every provider tried, or diagnosing it means guessing: {message}"
        )

    def test_providers_are_tried_in_the_declared_order(self):
        assert [p.__name__ for p in fx.PROVIDERS] == ["_er_api", "_fawaz"]
