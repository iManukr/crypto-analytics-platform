"""Tests for the Debezium connector registration helper.

The substitution test is not ceremony. If a placeholder silently survives
substitution, the connector registers with the literal string
``${DEBEZIUM_PASSWORD}`` as its password and fails several minutes later with an
authentication error that points nowhere near the actual cause.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from register_connector import DOC_KEY_PREFIX, load_payload, strip_doc_keys, substitute

TEMPLATE = Path(__file__).resolve().parents[2] / "infra" / "connect" / "debezium-postgres.json"

COMPLETE_ENV = {
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "crypto",
    "DEBEZIUM_USER": "debezium",
    "DEBEZIUM_PASSWORD": "s3cret",
    "CDC_TOPIC_PREFIX": "cdc",
}


class TestSubstitution:
    def test_resolves_placeholders(self):
        got = substitute({"a": "${FOO}/x", "b": "static"}, {"FOO": "bar"})
        assert got == {"a": "bar/x", "b": "static"}

    def test_missing_variables_raise_rather_than_passing_through(self):
        with pytest.raises(KeyError, match="MISSING"):
            substitute({"a": "${MISSING}"}, {})

    def test_reports_every_missing_variable_at_once(self):
        """One round trip per missing variable is a miserable way to configure
        a connector."""
        with pytest.raises(KeyError) as exc:
            substitute({"a": "${ONE}", "b": "${TWO}"}, {})
        assert "ONE" in str(exc.value) and "TWO" in str(exc.value)

    def test_non_string_values_pass_through_untouched(self):
        assert substitute({"n": 5, "b": True}, {}) == {"n": 5, "b": True}


class TestDocKeys:
    def test_documentation_keys_are_stripped(self):
        got = strip_doc_keys({"__doc_why": "because", "real.setting": "1"})
        assert got == {"real.setting": "1"}

    def test_the_committed_template_still_carries_its_rationale(self):
        """The stripping only makes sense if the annotations are actually there;
        this fails if someone 'cleans up' the template."""
        raw = json.loads(TEMPLATE.read_text(encoding="utf-8"))
        doc_keys = [k for k in raw["config"] if k.startswith(DOC_KEY_PREFIX)]
        assert len(doc_keys) >= 5


class TestRealTemplate:
    def test_renders_with_a_complete_environment(self):
        name, config = load_payload(str(TEMPLATE), COMPLETE_ENV)
        assert name == "crypto-oltp-cdc"
        assert "${" not in json.dumps(config), "an unresolved placeholder survived"
        assert not any(k.startswith(DOC_KEY_PREFIX) for k in config)

    def test_carries_the_settings_the_design_depends_on(self):
        """These are load-bearing choices, documented in docs/DESIGN.md. A
        change to any of them should require changing this test too."""
        _, config = load_payload(str(TEMPLATE), COMPLETE_ENV)

        # Silently skipping a record corrupts a CDC replica permanently.
        assert config["errors.tolerance"] == "none"
        # Without a heartbeat, a quiet captured table lets WAL accumulate until
        # the source database fills its disk.
        assert int(config["heartbeat.interval.ms"]) > 0
        # A float round-trip would not preserve an 8dp price.
        assert config["decimal.handling.mode"] == "string"
        # The publication is version-controlled in the DB init script.
        assert config["publication.autocreate.mode"] == "disabled"
        # Without a snapshot, a fresh ClickHouse only ever sees new changes.
        assert config["snapshot.mode"] == "initial"
        # The ClickHouse Kafka tables subscribe to exactly these topic names.
        assert config["topic.prefix"] == "cdc"

    def test_captures_exactly_the_intended_tables(self):
        _, config = load_payload(str(TEMPLATE), COMPLETE_ENV)
        captured = set(config["table.include.list"].split(","))
        assert captured == {
            "crypto.symbols",
            "crypto.market_candles_1m",
            "crypto.fx_rates",
        }
        # The quarantine table is operational state, not analytics data, and
        # replicating it would put malformed payloads into the warehouse.
        assert "crypto.ingest_rejects" not in captured
