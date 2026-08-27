#!/usr/bin/env python3
"""
Live ETH producer: fetches CLOSED 1-minute klines from Binance and pushes them
to Aiven Kafka in the same JSON shape your consumer expects. Uses the
SASL_SSL/SCRAM recipe you confirmed works.

Gap handling (why this is not just a poll loop):
  * on startup it republishes the last BACKFILL_MINUTES of candles, so a restart
    heals the hole left by whatever downtime preceded it;
  * during the run it watches for gaps (laptop sleep, network drop) and
    backfills them automatically before sending the current candle.
Re-sending is harmless: the consumer upserts on (symbol, open_time).

Run:
    pip install kafka-python requests python-dotenv
    python producer.py

    # one-off catch-up after a long outage (e.g. 5 days):
    BACKFILL_MINUTES=7200 python producer.py

Leave it running in its own terminal, alongside consumer.py.
"""

import os, json, time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "ethereum_prices")
KAFKA_USER      = os.environ["KAFKA_USER"]
KAFKA_PASSWORD  = os.environ["KAFKA_PASSWORD"]
KAFKA_CA        = os.environ.get("KAFKA_CA", "ca.pem")
SYMBOL          = os.environ.get("SYMBOL", "ETHUSDT")

# How far back to republish on startup. 0 disables. Binance caps a single
# request at 1000 candles, so longer windows are paginated automatically.
BACKFILL_MINUTES = int(os.environ.get("BACKFILL_MINUTES", "180"))

BINANCE   = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000
MINUTE_MS = 60_000


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def to_record(k):
    """Binance kline array -> your topic's JSON shape."""
    return {
        "open_time":    k[0],
        "open":         k[1],
        "high":         k[2],
        "low":          k[3],
        "close":        k[4],
        "volume":       k[5],
        "close_time":   k[6],
        "quote_volume": k[7],
        "trades":       k[8],
        "taker_base":   k[9],
        "taker_quote":  k[10],
        "ignore":       k[11],
    }


def fetch_klines(start_ms=None, limit=MAX_LIMIT):
    params = {"symbol": SYMBOL, "interval": "1m", "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    r = requests.get(BINANCE, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def latest_closed_kline():
    # ask for the last 2 klines; [-2] is the most recent CLOSED one
    # ([-1] is the still-forming current minute)
    return to_record(fetch_klines(limit=2)[-2])


def current_minute_ms():
    """Start of the minute now forming. Every candle opening before this is closed."""
    now_ms = int(time.time() * 1000)
    return now_ms - (now_ms % MINUTE_MS)


def backfill(producer, since_ms, until_ms):
    """Publish every CLOSED candle with open_time in [since_ms, until_ms).
    Paginates over Binance's 1000-candle cap. Returns how many were sent."""
    if since_ms >= until_ms:
        return 0

    sent, cursor = 0, since_ms
    while cursor < until_ms:
        batch = fetch_klines(start_ms=cursor, limit=MAX_LIMIT)
        if not batch:
            break

        for k in batch:
            if k[0] >= until_ms:        # never send the still-forming candle
                break
            producer.send(KAFKA_TOPIC, to_record(k))
            sent += 1

        last_open = batch[-1][0]
        if last_open < cursor:          # safety: refuse to spin if time goes backwards
            break
        cursor = last_open + MINUTE_MS
        if len(batch) < MAX_LIMIT:      # Binance had nothing more to give
            break
        time.sleep(0.25)                # stay well inside the rate limit

    producer.flush()
    return sent


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_cafile=KAFKA_CA,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Live producer -> topic '{KAFKA_TOPIC}' ({SYMBOL}). Ctrl-C to stop.")

    last_sent = None

    if BACKFILL_MINUTES > 0:
        until = current_minute_ms()
        since = until - BACKFILL_MINUTES * MINUTE_MS
        print(f"  backfilling {BACKFILL_MINUTES} min "
              f"({utc(since):%Y-%m-%d %H:%M} -> {utc(until):%Y-%m-%d %H:%M} UTC)…")
        try:
            n = backfill(producer, since, until)
            last_sent = until - MINUTE_MS
            print(f"  backfill sent {n} candles.")
        except Exception as e:
            print("  backfill failed (continuing live):", e)

    try:
        while True:
            try:
                candle = latest_closed_kline()
                open_time = candle["open_time"]

                # A gap means we were asleep or offline. Heal it before moving on.
                if last_sent is not None and open_time > last_sent + MINUTE_MS:
                    missing = (open_time - last_sent) // MINUTE_MS - 1
                    print(f"  gap of {missing} min detected "
                          f"(since {utc(last_sent):%H:%M} UTC); backfilling…")
                    n = backfill(producer, last_sent + MINUTE_MS, open_time)
                    print(f"  gap backfill sent {n} candles.")

                if open_time != last_sent:                   # only send new candles
                    producer.send(KAFKA_TOPIC, candle)
                    producer.flush()
                    last_sent = open_time
                    print(f"  sent candle open_time={open_time} close={candle['close']}")
            except Exception as e:
                print("fetch/send error (will retry):", e)
            time.sleep(20)   # poll every 20s; the dedupe above avoids duplicates
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
