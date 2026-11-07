# ==============================================================================
# Module: 01_infra.mk - Infrastructure & Container Lifecycle
# ==============================================================================

DOCKER_COMPOSE_FILE := infra/docker-compose.yml
COMPOSE := docker compose -f $(DOCKER_COMPOSE_FILE)
# Core long-running services. minio-init is a one-shot bucket provisioner.
CORE_SERVICES := minio kafka postgres redis
INIT_SERVICES := minio-init

# --- [ Infrastructure & Containers ] ---

up: ## Start core infrastructure containers (MinIO, Kafka, Postgres, Redis)
	@echo "==> Starting infrastructure via Docker Compose..."
	@if ! docker info >/dev/null 2>&1; then \
		echo "Notice: Docker daemon is not active. Using local fallback adapters."; \
		exit 0; \
	fi
	@$(COMPOSE) up -d $(CORE_SERVICES)
	@echo "==> Provisioning MinIO buckets (minio-init)..."
	@$(COMPOSE) up -d --force-recreate $(INIT_SERVICES)
	@echo "==> Infrastructure is up. Run 'make status' to inspect."

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

.PHONY: up down status reset logs
