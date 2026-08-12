#!/usr/bin/env python3
"""
Aiven Kafka -> Aiven Postgres consumer  (kafka-python version).

Uses the SAME connection recipe as your working producer/consumer:
SASL_SSL + SCRAM-SHA-256 + ca.pem. Writes each candle into Postgres,
committing Postgres FIRST and the Kafka offset SECOND (safe on crash,
idempotent upsert on (symbol, open_time)).

Message shape on the topic:
  {"open_time": 1786257060000, "open": "1917.11", "high": "1917.50",
   "low": "1917.11", "close": "1917.50", "volume": "11.3246",
   "close_time": 1786257119999, "quote_volume": "21712.46",
   "trades": 337, "taker_base": "10.62", "taker_quote": "20361.60", "ignore": "0"}
The payload has no symbol, so we supply it from SYMBOL (default ETHUSDT).

Run:
    pip install kafka-python psycopg2-binary python-dotenv
    python consumer.py
"""

import os, json, base64, time, threading
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "ethereum_prices")
KAFKA_GROUP     = os.environ.get("KAFKA_GROUP", "ethereum_prices-pg-consumer")
KAFKA_USER      = os.environ["KAFKA_USER"]
KAFKA_PASSWORD  = os.environ["KAFKA_PASSWORD"]
KAFKA_CA        = os.environ.get("KAFKA_CA", "ca.pem")
SYMBOL          = os.environ.get("SYMBOL", "ETHUSDT")
PG_DSN          = os.environ["PG_DSN"]

# FX leg: quote the USD price in a local currency (default KES).
FX_BASE         = os.environ.get("FX_BASE", "USD")
FX_QUOTE        = os.environ.get("FX_QUOTE", "KES")
FX_REFRESH_SEC  = int(os.environ.get("FX_REFRESH_SEC", "900"))   # 15 min

CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS crypto;
CREATE TABLE IF NOT EXISTS crypto.market_candles_1m (
    symbol           varchar(20)  NOT NULL,
    open_time        timestamptz  NOT NULL,
    close_time       timestamptz,
    open_price       numeric(20,8),
    high_price       numeric(20,8),
    low_price        numeric(20,8),
    close_price      numeric(20,8) NOT NULL,
    volume           numeric(30,8),
    quote_volume     numeric(30,8),
    trade_count      integer,
    taker_buy_base   numeric(30,8),
    taker_buy_quote  numeric(30,8),
    PRIMARY KEY (symbol, open_time)
);
CREATE TABLE IF NOT EXISTS crypto.fx_rates (
    base        varchar(10)   NOT NULL,
    quote       varchar(10)   NOT NULL,
    rate        numeric(20,8) NOT NULL,
    as_of       timestamptz   NOT NULL,   -- when the PROVIDER published it
    fetched_at  timestamptz   NOT NULL DEFAULT now(),
    source      text,
    PRIMARY KEY (base, quote, as_of)      -- re-fetching the same daily rate is a no-op
);
"""

COLS = ("symbol","open_time","close_time","open_price","high_price","low_price",
        "close_price","volume","quote_volume","trade_count","taker_buy_base","taker_buy_quote")

UPSERT_SQL = f"""
INSERT INTO crypto.market_candles_1m ({",".join(COLS)})
VALUES %s
ON CONFLICT (symbol, open_time) DO UPDATE SET
    close_time=EXCLUDED.close_time, open_price=EXCLUDED.open_price,
    high_price=EXCLUDED.high_price, low_price=EXCLUDED.low_price,
    close_price=EXCLUDED.close_price, volume=EXCLUDED.volume,
    quote_volume=EXCLUDED.quote_volume, trade_count=EXCLUDED.trade_count,
    taker_buy_base=EXCLUDED.taker_buy_base, taker_buy_quote=EXCLUDED.taker_buy_quote;
"""


def ms(v):
    return datetime.fromtimestamp(int(v)/1000, tz=timezone.utc) if v is not None else None


def parse_message(m: dict):
    if "close" not in m or "open_time" not in m:
        return None
    return (
        SYMBOL, ms(m["open_time"]), ms(m.get("close_time")),
        m.get("open"), m.get("high"), m.get("low"), m["close"],
        m.get("volume"), m.get("quote_volume"),
        int(m["trades"]) if m.get("trades") is not None else None,
        m.get("taker_base"), m.get("taker_quote"),
    )


def decode_value(raw: bytes) -> dict:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return json.loads(base64.b64decode(raw).decode("utf-8"))


def connect_pg():
    """Open a Postgres connection and ensure the schema/table exist."""
    pg = psycopg2.connect(PG_DSN)
    pg.autocommit = False
    with pg.cursor() as cur:
        cur.execute(CREATE_SQL)
    pg.commit()
    return pg


def write_row(pg, row):
    """Upsert one row; return the (possibly reconnected) connection.
    Retries on a dropped connection, and RAISES if the row still could not be
    written — so the caller never commits a Kafka offset for an unwritten row."""
    last_err = None
    for attempt in (1, 2, 3):
        try:
            with pg.cursor() as cur:
                execute_values(cur, UPSERT_SQL, [row])
            pg.commit()                 # Postgres first
            return pg
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            last_err = e
            print(f"  Postgres connection lost ({e}); reconnecting… ({attempt}/3)")
            try:
                pg.close()
            except Exception:
                pass
            time.sleep(3)
            try:
                pg = connect_pg()       # fresh connection, then retry the write
            except psycopg2.Error as e2:
                last_err = e2           # still down; next attempt retries
                print(f"  reconnect failed ({e2})")
    raise RuntimeError(f"could not write row after 3 attempts: {last_err}")


FX_UPSERT_SQL = """
INSERT INTO crypto.fx_rates (base, quote, rate, as_of, source)
VALUES %s
ON CONFLICT (base, quote, as_of) DO UPDATE SET
    rate = EXCLUDED.rate, fetched_at = now(), source = EXCLUDED.source;
"""


def fetch_fx_rate():
    """Latest FX_BASE->FX_QUOTE rate from a keyless provider, with a fallback.
    Returns (rate, as_of, source). NOTE: both providers publish once per DAY —
    there is no free tick-by-tick FX feed."""
    b, q = FX_BASE.upper(), FX_QUOTE.upper()
    try:
        r = requests.get(f"https://open.er-api.com/v6/latest/{b}", timeout=15)
        r.raise_for_status()
        d = r.json()
        return (float(d["rates"][q]),
                datetime.fromtimestamp(d["time_last_update_unix"], tz=timezone.utc),
                "open.er-api.com")
    except Exception as e:
        print(f"  FX primary failed ({e}); trying fallback…")

    r = requests.get(
        f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/{b.lower()}.json",
        timeout=15)
    r.raise_for_status()
    d = r.json()
    return (float(d[b.lower()][q.lower()]),
            datetime.strptime(d["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc),
            "fawazahmed0/currency-api")


def fx_updater():
    """Daemon thread: keeps crypto.fx_rates topped up on its OWN timer, so the
    rate stays fresh even when no Kafka messages are arriving. Uses a separate
    Postgres connection — psycopg2 connections are not shared across threads."""
    pg = None
    while True:
        try:
            if pg is None or pg.closed:
                pg = connect_pg()
            rate, as_of, source = fetch_fx_rate()
            with pg.cursor() as cur:
                execute_values(cur, FX_UPSERT_SQL,
                               [(FX_BASE.upper(), FX_QUOTE.upper(), rate, as_of, source)])
            pg.commit()
            print(f"  FX {FX_BASE}->{FX_QUOTE} = {rate}  "
                  f"(published {as_of:%Y-%m-%d %H:%M} UTC via {source})")
        except Exception as e:
            print(f"  FX update failed ({e}); retrying in {FX_REFRESH_SEC}s")
            try:
                if pg is not None:
                    pg.close()
            except Exception:
                pass
            pg = None
        time.sleep(FX_REFRESH_SEC)


def make_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_cafile=KAFKA_CA,
        value_deserializer=lambda b: b,
        consumer_timeout_ms=-1,          # block forever waiting for messages
        max_poll_records=100,            # smaller batches => more headroom before
                                         # max_poll_interval_ms (5 min) is exceeded
    )


def run(pg, state):
    """One consumer session. Returns the (possibly reconnected) Postgres
    connection when the group kicks us out, so the caller can rejoin."""
    consumer = make_consumer()
    print(f"Listening on '{KAFKA_TOPIC}' as {SYMBOL} -> crypto.market_candles_1m … Ctrl-C to stop")
    try:
        for msg in consumer:
            try:
                value = decode_value(msg.value)
            except Exception as e:
                print("skip bad message:", e)
                consumer.commit()
                continue

            row = parse_message(value)
            if row is None:
                consumer.commit()
                continue

            pg = write_row(pg, row)      # raises if the write did not land
            consumer.commit()            # commit Kafka offset only after DB success
            state["written"] += 1
            if state["written"] % 10 == 0:
                print(f"  wrote {state['written']}  latest {row[1]}  close={row[6]}")
    finally:
        try:
            consumer.close()             # never let close() mask the real error
        except Exception:
            pass
    return pg


def main():
    pg = connect_pg()
    state = {"written": 0}
    # daemon=True so Ctrl-C still exits immediately
    threading.Thread(target=fx_updater, name="fx-updater", daemon=True).start()
    try:
        while True:
            # Any failure here (group rebalance, dropped TLS session, Postgres
            # outage) restarts the session instead of killing the pipeline.
            # Uncommitted offsets are simply redelivered; the upsert is idempotent.
            try:
                pg = run(pg, state)
            except Exception as e:
                print(f"  session error ({type(e).__name__}: {e}); restarting in 5s…")
                time.sleep(5)
    except KeyboardInterrupt:
        print(f"\nStopping. Wrote {state['written']} rows this session.")
    finally:
        try:
            pg.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()