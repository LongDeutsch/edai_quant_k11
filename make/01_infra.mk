# ==============================================================================
# Module: 01_infra.mk - Infrastructure & Container Lifecycle
# ==============================================================================

DOCKER_COMPOSE_FILE := infra/docker-compose.yml
COMPOSE := docker compose -f $(DOCKER_COMPOSE_FILE)
# Infrastructure Services
STORAGE_SERVICES := minio postgres redis
STREAMING_SERVICES := kafka
INIT_SERVICES := minio-init kafka-init
PROCESSING_SERVICES := schema-registry iceberg-rest flink-jobmanager flink-taskmanager spark-history
ALL_SERVICES := $(STORAGE_SERVICES) $(STREAMING_SERVICES) $(INIT_SERVICES) $(PROCESSING_SERVICES)

# --- [ Infrastructure & Containers ] ---

up: ## Start all infrastructure containers (MinIO, Kafka, Postgres, Redis, Flink, Spark History, Schema Registry, Iceberg REST)
	@echo "==> Starting infrastructure via Docker Compose..."
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Docker daemon is not active. Using local fallback adapters."; \
		exit 0; \
	fi
	@mkdir -p ./.data/spark-events
	@echo "==> 1/3 Starting storage & broker foundation..."
	@$(COMPOSE) up -d $(STORAGE_SERVICES) $(STREAMING_SERVICES)
	@echo "==> 2/3 Provisioning MinIO buckets and Kafka streaming topics..."
	@$(COMPOSE) up $(INIT_SERVICES)
	@echo "==> 3/3 Starting processing engines, governance & UIs..."
	@$(COMPOSE) up -d $(PROCESSING_SERVICES)
	@echo "==> Infrastructure is up. Available endpoints:"
	@echo "    - MinIO Console:        http://localhost:9001 (minioadmin / minioadmin)"
	@echo "    - MinIO S3 API:         http://localhost:9000"
	@echo "    - Kafka Broker:         localhost:9092"
	@echo "    - Schema Registry:      http://localhost:8085"
	@echo "    - Flink Web Dashboard:  http://localhost:8081"
	@echo "    - Iceberg REST Catalog: http://localhost:8181"
	@echo "    - Spark History Server: http://localhost:18080"
	@echo "Run 'make status' to inspect container health."

down: ## Stop running infrastructure containers without deleting volumes
	@echo "==> Stopping infrastructure containers..."
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Docker compose down skipped (daemon inactive)."; \
		exit 0; \
	fi
	@$(COMPOSE) down

status: ## Check health and running status of infrastructure containers
	@echo "==> Infrastructure Service Status:"
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Docker daemon is not running."; \
		exit 0; \
	fi
	@$(COMPOSE) ps

reset: ## Stop containers and remove volumes (clean infrastructure slate)
	@echo "==> Resetting infrastructure and removing container volumes..."
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Reset skipped (daemon inactive)."; \
		exit 0; \
	fi
	@$(COMPOSE) down -v --remove-orphans

logs: ## Tail logs from infrastructure containers (e.g. make logs SERVICE=minio)
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Docker daemon is not running."; \
		exit 1; \
	fi
	@$(COMPOSE) logs -f $(SERVICE)

COMPOSE_TRADING_LITE := docker compose -f infra/docker-compose.trading-lite.yml

trading-lite-up: ## Start lightweight trading & serving stack (Kafka, Postgres, Redis, ClickHouse)
	@echo "==> Starting lightweight trading infrastructure..."
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Docker daemon is not active. Using local fallback adapters."; \
		exit 0; \
	fi
	@$(COMPOSE_TRADING_LITE) up -d kafka postgres redis clickhouse
	@$(COMPOSE_TRADING_LITE) up kafka-init
	@echo "==> Trading Lite is active (Kafka: 9092, Postgres: 5432, Redis: 6379, ClickHouse: 8123)."

trading-lite-down: ## Stop lightweight trading infrastructure
	@echo "==> Stopping lightweight trading infrastructure..."
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Docker compose down skipped (daemon inactive)."; \
		exit 0; \
	fi
	@$(COMPOSE_TRADING_LITE) down

.PHONY: up down status reset logs trading-lite-up trading-lite-down
