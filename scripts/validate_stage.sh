#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Prove data reached a given stage of the pipeline.
#
#   ./scripts/validate_stage.sh postgres | kafka | clickhouse | marts | all
#
# This is the "how do I verify the data moved through each stage" answer from
# the README, made executable. Each stage prints what it found rather than just
# a pass/fail, because the useful question during an incident is not "is it
# broken" but "how far did the data get".
# -----------------------------------------------------------------------------
set -uo pipefail

COMPOSE="${COMPOSE_CMD:-docker compose}"
CH_URL="http://localhost:${CLICKHOUSE_PUBLISHED_HTTP_PORT:-8123}"
CH_USER="${CLICKHOUSE_USER:-analytics}"
CH_PASS="${CLICKHOUSE_PASSWORD:-changeme_clickhouse}"
PG_USER="${POSTGRES_USER:-crypto_app}"
PG_DB="${POSTGRES_DB:-crypto}"

STAGE="${1:-all}"
rc=0

hdr()  { printf '\n\033[1;36m=== %s ===\033[0m\n' "$*"; }
ok()   { printf '\033[32m  PASS\033[0m %s\n' "$*"; }
bad()  { printf '\033[31m  FAIL\033[0m %s\n' "$*"; rc=1; }
note() { printf '       %s\n' "$*"; }

pg_query() { $COMPOSE exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null; }
ch_query() { curl -sf -u "${CH_USER}:${CH_PASS}" "${CH_URL}/" --data-binary "$1" 2>/dev/null; }

# --------------------------------------------------------------------------- #
validate_postgres() {
  hdr "Stage 1/4  REST API -> PostgreSQL (OLTP)"

  local rows newest age
  rows=$(pg_query "SELECT count(*) FROM crypto.market_candles_1m")
  newest=$(pg_query "SELECT COALESCE(max(open_time)::text, 'none') FROM crypto.market_candles_1m")
  age=$(pg_query "SELECT COALESCE(round(EXTRACT(EPOCH FROM (now() - max(open_time)))), -1)::int FROM crypto.market_candles_1m")

  if [ "${rows:-0}" -gt 0 ]; then
    ok "crypto.market_candles_1m holds ${rows} rows"
    note "newest candle: ${newest} (${age}s old)"
  else
    bad "crypto.market_candles_1m is empty - the ingester is not landing rows"
    note "check: docker compose logs ingestion"
  fi

  local per_symbol
  per_symbol=$(pg_query "SELECT string_agg(symbol || '=' || n, ', ') FROM (SELECT symbol, count(*) n FROM crypto.market_candles_1m GROUP BY symbol ORDER BY symbol) s")
  [ -n "${per_symbol:-}" ] && note "per symbol: ${per_symbol}"

  local sources
  sources=$(pg_query "SELECT string_agg(DISTINCT source, ', ') FROM crypto.market_candles_1m")
  note "data sources present: ${sources:-none}"
  if [[ "${sources:-}" == *replay* ]]; then
    note "NOTE: 'replay' rows are SYNTHETIC, produced by the offline generator."
  fi

  local rejects
  rejects=$(pg_query "SELECT count(*) FROM crypto.ingest_rejects")
  if [ "${rejects:-0}" -gt 0 ]; then
    note "${rejects} row(s) quarantined in crypto.ingest_rejects (validation gate working)"
  fi

  # The CDC precondition. Everything downstream is impossible without it.
  local wal slot
  wal=$(pg_query "SHOW wal_level")
  [ "$wal" = "logical" ] && ok "wal_level = logical" || bad "wal_level = ${wal}, expected logical"

  slot=$(pg_query "SELECT active FROM pg_replication_slots WHERE slot_name = 'dbz_crypto_slot'")
  [ "$slot" = "t" ] && ok "replication slot dbz_crypto_slot is active" \
                    || bad "replication slot is inactive or missing (got '${slot:-none}')"
}

# --------------------------------------------------------------------------- #
validate_kafka() {
  hdr "Stage 2/4  PostgreSQL -> Debezium -> Kafka (CDC)"

  local state
  state=$(curl -sf http://localhost:8083/connectors/crypto-oltp-cdc/status \
          | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4)
  [ "$state" = "RUNNING" ] && ok "Debezium connector is RUNNING" \
                           || bad "Debezium connector state is '${state:-unreachable}'"

  local topics
  topics=$($COMPOSE exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep '^cdc\.' | tr '\n' ' ')
  if [ -n "${topics// }" ]; then
    ok "CDC topics exist: ${topics}"
  else
    bad "no cdc.* topics found - Debezium has not produced anything"
  fi

  for topic in cdc.crypto.market_candles_1m cdc.crypto.fx_rates cdc.crypto.symbols; do
    local offsets
    offsets=$($COMPOSE exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
              --bootstrap-server localhost:29092 --topic "$topic" 2>/dev/null \
              | awk -F: '{s += $3} END {print s+0}')
    if [ "${offsets:-0}" -gt 0 ]; then
      ok "${topic}: ${offsets} message(s)"
    else
      note "${topic}: 0 messages (expected if that table has not changed yet)"
    fi
  done
}

# --------------------------------------------------------------------------- #
validate_clickhouse() {
  hdr "Stage 3/4  Kafka -> ClickHouse (CDC landing)"

  local engines
  engines=$(ch_query "SELECT count() FROM system.tables WHERE database='raw' AND engine='Kafka'")
  [ "${engines:-0}" -ge 3 ] && ok "${engines} Kafka engine tables present" \
                            || bad "expected 3 Kafka engine tables, found ${engines:-0}"

  local raw_rows dedup_rows
  raw_rows=$(ch_query "SELECT count() FROM raw.market_candles_1m")
  dedup_rows=$(ch_query "SELECT count() FROM raw.market_candles_1m FINAL WHERE _op != 'd'")

  if [ "${raw_rows:-0}" -gt 0 ]; then
    ok "raw.market_candles_1m holds ${raw_rows} CDC event(s)"
    note "after dedup (FINAL, tombstones removed): ${dedup_rows}"
  else
    bad "raw.market_candles_1m is empty - CDC is not reaching ClickHouse"
    note "check: docker compose logs connect | tail -50"
  fi

  # The number the whole CDC design is judged on.
  local lag
  lag=$(ch_query "SELECT ifNull(round(quantile(0.95)((toUnixTimestamp64Milli(_cdc_arrived_at) - toInt64(_source_ts_ms)) / 1000), 2), -1) FROM raw.market_candles_1m WHERE _cdc_arrived_at >= now() - toIntervalMinute(15)")
  note "CDC p95 lag, Postgres commit -> ClickHouse visible: ${lag}s (SLA 120s)"

  local ops
  ops=$(ch_query "SELECT arrayStringConcat(groupArray(concat(_op, '=', toString(n))), ', ') FROM (SELECT _op, count() n FROM raw.market_candles_1m GROUP BY _op)")
  note "operations seen: ${ops:-none}   (c=create, u=update, d=delete, r=snapshot)"

  # Row parity: the only signal that detects a silently dropped change event.
  local pg_window ch_window
  pg_window=$(pg_query "SELECT count(*) FROM crypto.market_candles_1m WHERE open_time >= now() - interval '1 hour' AND open_time < now() - interval '2 minutes'")
  ch_window=$(ch_query "SELECT count() FROM raw.market_candles_1m FINAL WHERE _op != 'd' AND open_time >= now() - toIntervalHour(1) AND open_time < now() - toIntervalMinute(2)")
  if [ "${pg_window:-0}" = "${ch_window:-x}" ]; then
    ok "row parity in the settled window: postgres=${pg_window} clickhouse=${ch_window}"
  else
    bad "row parity MISMATCH: postgres=${pg_window} clickhouse=${ch_window}"
    note "a positive difference means change events were lost"
  fi

  local dlq
  dlq=$(ch_query "SELECT count() FROM raw.cdc_dead_letters")
  [ "${dlq:-0}" = "0" ] && ok "dead-letter queue is empty" \
                        || bad "${dlq} unparseable CDC message(s) in raw.cdc_dead_letters"
}

# --------------------------------------------------------------------------- #
validate_marts() {
  hdr "Stage 4/4  ClickHouse raw -> dbt staging -> marts"

  local staged
  staged=$(ch_query "SELECT count() FROM analytics_staging.stg_market_candles" || echo 0)
  if [ "${staged:-0}" -gt 0 ]; then
    ok "analytics_staging.stg_market_candles resolves ${staged} row(s)"
    local distinct
    distinct=$(ch_query "SELECT count() FROM (SELECT DISTINCT symbol, open_time FROM analytics_staging.stg_market_candles)")
    [ "$staged" = "$distinct" ] && ok "staging is fully deduplicated (${distinct} distinct keys)" \
                                || bad "staging has $((staged - distinct)) duplicate key(s) - FINAL is not working"
  else
    bad "staging is empty - run the Airflow DAG (make trigger-dag)"
  fi

  for table in fct_candles_1m agg_candles_5m fct_market_daily ml_features_1m dim_symbol; do
    local n
    n=$(ch_query "SELECT count() FROM analytics_marts.${table}" || echo "")
    if [ -n "$n" ] && [ "$n" -gt 0 ]; then
      ok "analytics_marts.${table}: ${n} row(s)"
    else
      bad "analytics_marts.${table} is empty or does not exist"
    fi
  done

  # The ML contract, checked rather than assumed.
  local unresolved_labelled
  unresolved_labelled=$(ch_query "SELECT count() FROM analytics_marts.ml_features_1m WHERE is_label_resolved = 0 AND (target_direction_up != 0 OR target_log_return_1m != 0)" || echo "")
  if [ "${unresolved_labelled:-0}" = "0" ]; then
    ok "ML labels: no unresolved row carries a non-neutral label"
  else
    bad "${unresolved_labelled} unresolved row(s) carry fabricated labels"
  fi

  local trainable
  trainable=$(ch_query "SELECT count() FROM analytics_marts.ml_features_1m WHERE is_label_resolved = 1 AND has_contiguous_history = 1" || echo "")
  note "training-ready rows (label resolved, history contiguous): ${trainable:-unknown}"

  local failures
  failures=$(ch_query "SELECT countIf(status IN ('fail','error')) FROM analytics_ops.dbt_test_results WHERE invocation_at = (SELECT max(invocation_at) FROM analytics_ops.dbt_test_results)" || echo "")
  if [ -n "${failures:-}" ]; then
    [ "$failures" = "0" ] && ok "dbt tests: all passing in the most recent run" \
                          || bad "dbt tests: ${failures} failing in the most recent run"
  else
    note "no dbt test history yet (the DAG has not published results)"
  fi
}

# --------------------------------------------------------------------------- #
case "$STAGE" in
  postgres)   validate_postgres ;;
  kafka)      validate_kafka ;;
  clickhouse) validate_clickhouse ;;
  marts)      validate_marts ;;
  all)        validate_postgres; validate_kafka; validate_clickhouse; validate_marts ;;
  *)          echo "usage: $0 [postgres|kafka|clickhouse|marts|all]" >&2; exit 2 ;;
esac

echo
if [ "$rc" = "0" ]; then
  printf '\033[1;32mAll checks passed for stage: %s\033[0m\n' "$STAGE"
else
  printf '\033[1;31mOne or more checks failed for stage: %s\033[0m\n' "$STAGE"
fi
exit "$rc"
