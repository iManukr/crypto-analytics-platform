# Real-Time Ethereum Price Tracker

![Real-time ETH dashboard](assets/dashboard.png)

A streaming data pipeline that ingests live Ethereum (ETH/USD) 1-minute candles
from Binance, moves them through Kafka, stores them in PostgreSQL, and visualises
them live on a Grafana dashboard.

**Status:** Phase 1 (real-time tracking) — complete and streaming.
Phase 2 (price prediction) is planned; see [Roadmap](#roadmap).

---

## Architecture

```
Binance API ──▶ producer.py ──▶ Aiven Kafka ──▶ consumer.py ──▶ Aiven Postgres ──▶ Grafana
 (1m klines)     (poll +          (topic:          (parse +        (market_candles_1m)   (live
                  publish)      ethereum_prices)    upsert)                              panels)
```

- **Producer** fetches the most recent *closed* 1-minute candle from Binance every
  ~20s and publishes it to Kafka (deduplicated, so each candle is sent once).
- **Kafka** (Aiven, SASL_SSL / SCRAM-SHA-256) buffers the stream.
- **Consumer** reads each message, parses it, and upserts it into Postgres.
  It commits **Postgres first, the Kafka offset second**, so a crash can only ever
  cause a harmless re-insert — never data loss. The upsert is idempotent on
  `(symbol, open_time)`.
- **Grafana** reads directly from Postgres and renders live price, volume, 24h
  high/low, and per-minute price change.

---

## Tech stack

| Component   | Choice                                    |
|-------------|-------------------------------------------|
| Source      | Binance public REST API (`/api/v3/klines`)|
| Streaming   | Aiven for Apache Kafka (SASL_SSL, SCRAM-SHA-256) |
| Storage     | Aiven for PostgreSQL (SSL required)       |
| Client lib  | `kafka-python`                            |
| DB driver   | `psycopg2`                                |
| Dashboard   | Grafana (PostgreSQL data source)          |
| Language    | Python 3.13                               |

---

## Project structure

```
Kafka_Project/
├── producer.py            # Binance -> Kafka (live 1m candles)
├── consumer.py            # Kafka -> Postgres (upsert, auto-reconnect)
├── requirements.txt       # Python dependencies
├── dashboard_fixed.json   # Grafana dashboard (import into Grafana)
├── .env                   # secrets & config (NOT committed)
├── .gitignore
└── certificates/          # Aiven Kafka TLS files (NOT committed)
    ├── ca.pem
    ├── service.cert
    └── service.key
```

---

## Database schema

```sql
CREATE SCHEMA IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.market_candles_1m (
    symbol           varchar(20)   NOT NULL,
    open_time        timestamptz   NOT NULL,
    close_time       timestamptz,
    open_price       numeric(20,8),
    high_price       numeric(20,8),
    low_price        numeric(20,8),
    close_price      numeric(20,8)  NOT NULL,
    volume           numeric(30,8),
    quote_volume     numeric(30,8),
    trade_count      integer,
    taker_buy_base   numeric(30,8),
    taker_buy_quote  numeric(30,8),
    PRIMARY KEY (symbol, open_time)   -- makes the upsert idempotent
);
```

The consumer creates this automatically on first run.

---

## Setup

### 1. Clone and install

```bash
pip install -r requirements.txt
```

`requirements.txt`:

```
kafka-python
psycopg2-binary
python-dotenv
requests
```

### 2. Aiven credentials

From the [Aiven console](https://console.aiven.io):

- **Kafka service → Overview:** download `ca.pem`, `service.cert`, `service.key`
  into `certificates/`. Note the bootstrap host and port, the SASL username, and
  the SASL password.
- **Postgres service → Overview:** copy the full **Service URI** (includes
  `?sslmode=require`).

### 3. Configure `.env`

Create a `.env` file in the project root (never commit it):

```dotenv
# Kafka (Aiven)
KAFKA_BOOTSTRAP=kafka-XXXX-yourproject.e.aivencloud.com:PORT
KAFKA_TOPIC=ethereum_prices
KAFKA_USER=avnadmin
KAFKA_PASSWORD=your_kafka_sasl_password
KAFKA_CA=certificates/ca.pem

# Symbol
SYMBOL=ETHUSDT

# Postgres (Aiven) — paste the Service URI from the console
PG_DSN=postgres://avnadmin:PASSWORD@pg-XXXX-yourproject.h.aivencloud.com:PORT/defaultdb?sslmode=require
```

> **Note:** Kafka and Postgres are separate Aiven services with **different**
> hostnames and ports. Copy each from its own Overview page.

---

## Running

Run the producer and consumer in **two separate terminals**, both left running:

```bash
# Terminal 1 — stream Binance -> Kafka
python producer.py

# Terminal 2 — drain Kafka -> Postgres
python consumer.py
```

Expected output:

```
# producer
Live producer -> topic 'ethereum_prices' (ETHUSDT). Ctrl-C to stop.
  sent candle open_time=1786474920000 close=1865.20000000

# consumer
Listening on 'ethereum_prices' as ETHUSDT -> crypto.market_candles_1m … Ctrl-C to stop
  wrote 10  latest 2026-08-11 19:05:00+00:00  close=1864.82000000
```

The producer emits one candle per minute, so short quiet gaps between
`sent candle` lines are normal.

---

## Verifying it's live

```sql
SELECT now()                    AS server_now,
       max(open_time)           AS newest,
       now() - max(open_time)   AS behind_by,
       count(*)                 AS total
FROM crypto.market_candles_1m;
```

You are live when `newest` is within ~1–2 minutes of `now`, `behind_by` stays
small and steady, and `total` climbs on repeat runs. If `behind_by` grows, the
producer has stopped feeding the topic.

---

## Grafana dashboard

Import `dashboard_fixed.json`:

**Dashboards → New → Import → Upload JSON file** → select your Aiven Postgres
data source when prompted.

Panels included:

| Panel                  | Query summary                                  |
|------------------------|------------------------------------------------|
| ETH Price (USD)        | latest `close_price`                            |
| ETH Price (KES)        | latest `close_price × FX rate`                  |
| Live Price (latest)    | most recent candle row                          |
| ETH/USD (time-filtered)| `close_price` over the dashboard time range     |
| Volume                 | `volume` over range                             |
| Price change (%/min)   | minute-over-minute % change (window function)   |
| 24h High / 24h Low     | `max(high_price)` / `min(low_price)` over 24h   |

Dashboard defaults to a 30-minute window with 30s auto-refresh.

**Tips**
- Stat panels (single value) must use **Format: Table**; time-series panels use
  **Format: Time series**.
- Macros like `$__timeFilter(open_time)` only work inside Grafana. In a plain SQL
  client, use `open_time > now() - interval '7 days'` instead.
- If a panel shows *"datasource not found"* after import, open it and re-select
  your Aiven Postgres data source.

---

## Notes & gotchas

- **Two terminals required.** Closing either the producer or consumer freezes the
  live line at that point.
- **Data gaps** appear whenever the producer isn't running. They're harmless for
  live tracking (a continuous backfill is only needed for the prediction phase).
- **Library choice matters.** `kafka-python` connected reliably to Aiven's
  SASL_SSL endpoint on Windows where `confluent-kafka` (librdkafka) repeatedly
  dropped the TLS session. Stick with `kafka-python` here.
- **Binance access.** The producer needs outbound access to `api.binance.com`.
  Some regions restrict it; a data proxy or `api.binance.us` may be required.

---

## Security

- Never commit `.env` or the `certificates/` folder — both are in `.gitignore`.
- Keep credentials out of notebooks, screenshots, and chat logs.
- If a database or Kafka credential is ever exposed, rotate it in the Aiven
  console (Postgres/Kafka service → **Users** → reset password), then update
  `.env`.

`.gitignore`:

```
.env
certificates/
__pycache__/
*.pem
service.*
```

---

## Roadmap

Phase 2 — **price prediction** (planned):

1. Backfill continuous historical candles from Binance's public archive into
   `market_candles_1m`.
2. Engineer leakage-safe features (returns, SMA/EMA, RSI, MACD, ATR, volume
   z-score, taker-buy ratio).
3. Train and honestly evaluate a short-horizon classifier against a
   majority-class baseline (with fees/slippage in a paper-PnL check).
4. Serve live predictions into a `predictions` table and resolve them against
   actual outcomes.
5. Add prediction + rolling-accuracy panels to the Grafana dashboard.

> Reliable crypto price prediction is not a solved problem. Phase 2 is a learning
> exercise in building the pipeline honestly — including measuring how little edge
> a naive model actually has — not a trading signal.

---

## License

For educational / workshop use.