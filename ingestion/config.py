"""Environment-driven configuration for the ingestion service.

One frozen dataclass, built once at startup, passed explicitly everywhere else.
No module reads ``os.environ`` outside this file, which is what makes the rest
of the package testable without monkeypatching the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    return value if value != "" else default


def _int(name: str, default: int) -> int:
    raw = _env(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _csv(name: str, default: str) -> list[str]:
    return [item.strip() for item in _env(name, default).split(",") if item.strip()]


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    def __repr__(self) -> str:  # keep the password out of logs and tracebacks
        return (
            f"PostgresConfig(host={self.host!r}, port={self.port}, "
            f"database={self.database!r}, user={self.user!r}, password='***')"
        )


@dataclass(frozen=True)
class Config:
    # ---- source ----------------------------------------------------------
    source: str = "binance"
    binance_hosts: list[str] = field(default_factory=lambda: ["https://api.binance.com"])
    symbols: list[str] = field(default_factory=lambda: ["ETHUSDT"])
    interval_seconds: int = 20
    backfill_minutes: int = 180
    # After this many consecutive live-API failures, degrade to the replay
    # source rather than emitting nothing. 0 disables the fallback entirely.
    fallback_after_failures: int = 3
    http_timeout_seconds: int = 15
    max_retries: int = 4

    # ---- fx --------------------------------------------------------------
    fx_base: str = "USD"
    fx_quote: str = "KES"
    fx_refresh_seconds: int = 900

    # ---- sinks / ops -----------------------------------------------------
    postgres: PostgresConfig = field(
        default_factory=lambda: PostgresConfig("localhost", 5432, "crypto", "crypto_app", "")
    )
    metrics_port: int = 8000
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            source=_env("INGEST_SOURCE", "binance").lower(),
            binance_hosts=_csv("BINANCE_HOSTS", "https://api.binance.com"),
            symbols=[s.upper() for s in _csv("SYMBOLS", "ETHUSDT")],
            interval_seconds=_int("INGEST_INTERVAL_SECONDS", 20),
            backfill_minutes=_int("BACKFILL_MINUTES", 180),
            fallback_after_failures=_int("INGEST_FALLBACK_AFTER_FAILURES", 3),
            http_timeout_seconds=_int("INGEST_HTTP_TIMEOUT_SECONDS", 15),
            max_retries=_int("INGEST_MAX_RETRIES", 4),
            fx_base=_env("FX_BASE", "USD").upper(),
            fx_quote=_env("FX_QUOTE", "KES").upper(),
            fx_refresh_seconds=_int("FX_REFRESH_SECONDS", 900),
            postgres=PostgresConfig(
                host=_env("POSTGRES_HOST", "postgres"),
                port=_int("POSTGRES_PORT", 5432),
                database=_env("POSTGRES_DB", "crypto"),
                user=_env("POSTGRES_USER", "crypto_app"),
                password=_env("POSTGRES_PASSWORD", ""),
            ),
            metrics_port=_int("INGESTION_METRICS_PORT", 8000),
            log_level=_env("LOG_LEVEL", "INFO").upper(),
        )

    def validate(self) -> None:
        if self.source not in {"binance", "replay"}:
            raise ValueError(f"INGEST_SOURCE must be 'binance' or 'replay', got {self.source!r}")
        if not self.symbols:
            raise ValueError("SYMBOLS resolved to an empty list")
        if self.interval_seconds < 1:
            raise ValueError("INGEST_INTERVAL_SECONDS must be >= 1")
        if self.backfill_minutes < 0:
            raise ValueError("BACKFILL_MINUTES must be >= 0")
        if self.source == "binance" and not self.binance_hosts:
            raise ValueError("BINANCE_HOSTS resolved to an empty list")
