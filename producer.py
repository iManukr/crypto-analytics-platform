#!/usr/bin/env python3
"""
Live ETH producer: fetches the most recent CLOSED 1-minute kline from Binance
once per minute and pushes it to Aiven Kafka in the same JSON shape your
consumer expects. Uses the SASL_SSL/SCRAM recipe you confirmed works.

Run:
    pip install kafka-python requests python-dotenv
    python producer.py

Leave it running in its own terminal, alongside consumer.py.
"""

import os, json, time
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

BINANCE = "https://api.binance.com/api/v3/klines"


def latest_closed_kline():
    # ask for the last 2 klines; [-2] is the most recent CLOSED one
    # ([-1] is the still-forming current minute)
    r = requests.get(BINANCE, params={"symbol": SYMBOL, "interval": "1m", "limit": 2}, timeout=10)
    r.raise_for_status()
    k = r.json()[-2]
    # Binance kline array -> your topic's JSON shape
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
    try:
        while True:
            try:
                candle = latest_closed_kline()
                if candle["open_time"] != last_sent:          # only send new candles
                    producer.send(KAFKA_TOPIC, candle)
                    producer.flush()
                    last_sent = candle["open_time"]
                    print(f"  sent candle open_time={candle['open_time']} close={candle['close']}")
            except Exception as e:
                print("fetch/send error (will retry):", e)
            time.sleep(20)   # poll every 20s; the dedupe above avoids duplicates
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()