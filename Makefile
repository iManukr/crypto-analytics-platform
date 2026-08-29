# =============================================================================
# Developer entry points. `make help` lists everything.
#
# These wrap the docker compose and dbt invocations you would otherwise have to
# remember, and they are the same commands CI runs - so a green local `make ci`
# means a green pipeline, not a different code path.
# =============================================================================
SHELL := /bin/bash
COMPOSE := docker compose
CI_COMPOSE := docker compose -f docker-compose.yml -f docker-compose.ci.yml
DBT := docker compose exec -T airflow-scheduler /opt/dbt-venv/bin/dbt
DBT_ARGS := --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --no-use-colors
PY := python

.DEFAULT_GOAL := help
.PHONY: help up down clean logs ps env \
        wait validate validate-postgres validate-kafka validate-clickhouse validate-marts \
        dbt-build dbt-test dbt-docs trigger-dag \
        test test-unit test-integration lint format ci \
        psql clickhouse-client urls

## ---------------------------------------------------------------- lifecycle
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

env:  ## Create .env from the template, generating a unique Fernet key
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		key=$$(openssl rand -base64 32 | tr '+/' '-_'); \
		sed -i.bak "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$key|" .env && rm -f .env.bak; \
		echo "created .env with a freshly generated AIRFLOW_FERNET_KEY"; \
		echo "NOTE: passwords are still the changeme_* placeholders - fine locally,"; \
		echo "      replace them before deploying anywhere reachable."; \
	else echo ".env already exists, leaving it alone"; fi

up: env  ## Start the whole stack (the one command a reviewer needs)
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory urls

down:  ## Stop the stack, keeping volumes
	$(COMPOSE) down

clean:  ## Stop and DELETE all data volumes (full reset)
	$(COMPOSE) down -v --remove-orphans

ps:  ## Show container status
	$(COMPOSE) ps

logs:  ## Tail logs (make logs SERVICE=ingestion)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

urls:  ## Print the service URLs
	@echo ""
	@echo "  Airflow      http://localhost:$${AIRFLOW_PUBLISHED_PORT:-8080}   (admin / admin)"
	@echo "  Grafana      http://localhost:$${GRAFANA_PUBLISHED_PORT:-3000}   (admin / admin)"
	@echo "  Prometheus   http://localhost:$${PROMETHEUS_PUBLISHED_PORT:-9090}"
	@echo "  ClickHouse   http://localhost:$${CLICKHOUSE_PUBLISHED_HTTP_PORT:-8123}/play"
	@echo "  Kafka Connect http://localhost:8083/connectors"
	@echo "  Ingestion    http://localhost:$${INGESTION_METRICS_PORT:-8000}/metrics"
	@echo "  DQ exporter  http://localhost:$${EXPORTER_PORT:-9101}/metrics"
	@echo ""

wait:  ## Block until every service reports healthy
	@bash scripts/wait_for_stack.sh

## ------------------------------------------------------------- validation
validate: validate-postgres validate-kafka validate-clickhouse validate-marts  ## Verify data reached every stage
	@echo ""
	@echo "All stages reported data. The pipeline is working end to end."

validate-postgres:  ## Stage 1: rows landed in the OLTP database
	@bash scripts/validate_stage.sh postgres

validate-kafka:  ## Stage 2: CDC events are on the Kafka topics
	@bash scripts/validate_stage.sh kafka

validate-clickhouse:  ## Stage 3: CDC replicated into ClickHouse
	@bash scripts/validate_stage.sh clickhouse

validate-marts:  ## Stage 4: dbt staging and marts are populated
	@bash scripts/validate_stage.sh marts

## -------------------------------------------------------------------- dbt
dbt-build:  ## Run every dbt model and its tests
	$(DBT) build $(DBT_ARGS)

dbt-test:  ## Run the dbt tests only
	$(DBT) test $(DBT_ARGS)

dbt-docs:  ## Generate the dbt documentation site
	$(DBT) docs generate $(DBT_ARGS)

trigger-dag:  ## Trigger the pipeline DAG immediately
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger crypto_analytics_pipeline

## ------------------------------------------------------------------ tests
test: test-unit  ## Alias for the fast suite

test-unit:  ## Fast unit tests, no containers required
	$(PY) -m pytest tests/unit -v --cov --cov-report=term-missing

test-integration:  ## End-to-end tests against a running stack
	$(PY) -m pytest tests/integration -v

lint:  ## Lint and check formatting
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

format:  ## Apply formatting and safe lint fixes
	$(PY) -m ruff check . --fix
	$(PY) -m ruff format .

ci:  ## Reproduce the CI end-to-end job locally
	$(CI_COMPOSE) up -d --build
	bash scripts/wait_for_stack.sh
	$(PY) -m pytest tests/integration -v

## ------------------------------------------------------------------ shells
psql:  ## Open a psql shell on the OLTP database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-crypto_app} -d $${POSTGRES_DB:-crypto}

clickhouse-client:  ## Open a ClickHouse client
	$(COMPOSE) exec clickhouse clickhouse-client \
		--user $${CLICKHOUSE_USER:-analytics} --password $${CLICKHOUSE_PASSWORD:-changeme_clickhouse}
