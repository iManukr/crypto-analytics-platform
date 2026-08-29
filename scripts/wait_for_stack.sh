#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Block until the stack is genuinely ready.
#
# "Ready" here means more than "the containers started". Compose healthchecks
# cover process liveness; this adds the two things that actually gate the
# pipeline and that a healthcheck cannot see:
#
#   * the Debezium connector has reached RUNNING (a healthy Connect worker with
#     a FAILED connector produces no data at all), and
#   * ClickHouse has finished running its init scripts, so the Kafka engine
#     tables and materialized views exist to receive it.
#
# Used by `make wait`, `make ci`, and the CI end-to-end job. Exits non-zero with
# a diagnostic dump rather than hanging, so a stuck CI job fails in five minutes
# with an explanation instead of at the six-hour job timeout with nothing.
# -----------------------------------------------------------------------------
set -uo pipefail

TIMEOUT="${STACK_WAIT_TIMEOUT:-420}"
COMPOSE="${COMPOSE_CMD:-docker compose}"
CONNECT_URL="${KAFKA_CONNECT_URL_HOST:-http://localhost:8083}"
CH_URL="http://localhost:${CLICKHOUSE_PUBLISHED_HTTP_PORT:-8123}"
CH_USER="${CLICKHOUSE_USER:-analytics}"
CH_PASS="${CLICKHOUSE_PASSWORD:-changeme_clickhouse}"

deadline=$(( $(date +%s) + TIMEOUT ))

say()  { printf '\033[36m[wait]\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m[ ok ]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; }

remaining() { echo $(( deadline - $(date +%s) )); }

# Poll a command until it succeeds, or the global deadline passes.
await() {
  local label="$1"; shift
  say "waiting for ${label} ..."
  while [ "$(remaining)" -gt 0 ]; do
    if "$@" > /dev/null 2>&1; then
      ok "${label}"
      return 0
    fi
    sleep 3
  done
  fail "${label} did not become ready within ${TIMEOUT}s"
  return 1
}

ch_query() {
  curl -sf -u "${CH_USER}:${CH_PASS}" "${CH_URL}/" --data-binary "$1"
}

# --- 1. container healthchecks ----------------------------------------------
containers_healthy() {
  local unhealthy
  # Services with no healthcheck report an empty status; only an explicit
  # "unhealthy"/"starting" should hold us here.
  unhealthy=$($COMPOSE ps --format '{{.Service}} {{.Health}}' 2>/dev/null \
    | awk '$2 == "starting" || $2 == "unhealthy" { print $1 }')
  [ -z "$unhealthy" ]
}
await "container healthchecks" containers_healthy || FAILED=1

# --- 2. ClickHouse init completed -------------------------------------------
clickhouse_ready() {
  local n
  n=$(ch_query "SELECT count() FROM system.tables WHERE database='raw' AND engine='Kafka'") || return 1
  [ "${n:-0}" -ge 3 ]
}
await "ClickHouse init (Kafka engine tables + materialized views)" clickhouse_ready || FAILED=1

# --- 3. Debezium connector RUNNING ------------------------------------------
connector_running() {
  local state
  state=$(curl -sf "${CONNECT_URL}/connectors/crypto-oltp-cdc/status" 2>/dev/null \
    | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4) || return 1
  [ "$state" = "RUNNING" ]
}
await "Debezium connector to reach RUNNING" connector_running || FAILED=1

# --- 4. data actually flowing through CDC -----------------------------------
rows_replicated() {
  local n
  n=$(ch_query "SELECT count() FROM raw.market_candles_1m") || return 1
  [ "${n:-0}" -gt 0 ]
}
await "the first rows to replicate into ClickHouse" rows_replicated || FAILED=1

# --- diagnostics on failure -------------------------------------------------
if [ "${FAILED:-0}" != "0" ]; then
  fail "the stack did not converge. Diagnostics follow."
  echo "----- container status -----"
  $COMPOSE ps
  echo "----- connector status -----"
  curl -s "${CONNECT_URL}/connectors/crypto-oltp-cdc/status" || echo "(connector unreachable)"
  echo
  echo "----- recent logs: connect -----"
  $COMPOSE logs --tail=60 connect 2>&1 || true
  echo "----- recent logs: clickhouse -----"
  $COMPOSE logs --tail=40 clickhouse 2>&1 || true
  echo "----- recent logs: ingestion -----"
  $COMPOSE logs --tail=40 ingestion 2>&1 || true
  exit 1
fi

ok "stack is ready"
