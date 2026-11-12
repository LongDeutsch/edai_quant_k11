# ==============================================================================
# Module: 00_env.mk - Environment, Global Parameters & Help System
# ==============================================================================

SHELL := /bin/bash
UV := $(shell command -v uv 2>/dev/null || ([ -x $(HOME)/.local/bin/uv ] && echo $(HOME)/.local/bin/uv))
# Prefer project .venv created by `uv sync` / `make setup`. Override with: make PYTHON=...
VENV_PYTHON := $(CURDIR)/.venv/bin/python
ifeq ($(origin PYTHON),command line)
  # Keep explicit PYTHON=... from the caller.
else ifneq ($(wildcard $(VENV_PYTHON)),)
  PYTHON := $(VENV_PYTHON)
else
  PYTHON := python3
endif
SCALE ?= 0.1
SEED ?= 42
MODE ?= smoke
SPEED ?= 10
OUT_DIR ?= /tmp/lakehouse_source
ENGINE ?= sim
DATASET_ID ?=
# Derived: smoke/sim → SIMULATED; integration|full + real engine → LIVE
ifeq ($(MODE),integration)
  EXECUTION_MODE ?= LIVE
else ifeq ($(MODE),full)
  EXECUTION_MODE ?= LIVE
else
  EXECUTION_MODE ?= SIMULATED
endif
KAFKA_BOOTSTRAP ?= localhost:9092
KAFKA_BOOTSTRAP_SERVERS ?= $(KAFKA_BOOTSTRAP)
SCHEMA_REGISTRY_URL ?= http://localhost:8085
ICEBERG_REST_URL ?= http://localhost:8181
# Contiguous offline daily vs online tick windows (overridable; see make/02_generator.mk)
OFFLINE_START ?= 2022-11-01
OFFLINE_END ?= 2025-10-31
ONLINE_START ?= 2025-11-01
ONLINE_END ?= 2026-11-10

# Export standard variables for downstream sub-shells and scripts
export PYTHONPATH := src
export PYTHON
export SCALE
export SEED
export MODE
export SPEED
export OUT_DIR
export ENGINE
export DATASET_ID
export EXECUTION_MODE
export KAFKA_BOOTSTRAP
export KAFKA_BOOTSTRAP_SERVERS
export SCHEMA_REGISTRY_URL
export ICEBERG_REST_URL
export OFFLINE_START
export OFFLINE_END
export ONLINE_START
export ONLINE_END

# Resolve immutable run root: OUT_DIR/runs/{dataset_id} (falls back to OUT_DIR legacy)
define RESOLVE_DATASET_ROOT
$(PYTHON) -c "from quant_edai.common.dataset_paths import resolve_dataset_root; import os; print(resolve_dataset_root(os.environ['OUT_DIR'], os.environ.get('DATASET_ID') or None))"
endef

# --- [ Environment & Setup ] ---

help: ## Show this help menu with all available targets
	@echo "Quant AI Engineering Platform - Command Center"
	@echo "======================================================================"
	@echo "Usage: make [target] [SCALE=0.1|1.0] [SEED=42] [MODE=smoke|test|full]"
	@awk -F ':.*?## ' ' \
		/^# --- \[ .* \] ---/ { \
			match($$0, /\[ .* \]/); \
			print "\n\033[1;36m" substr($$0, RSTART, RLENGTH) "\033[0m"; \
			next; \
		} \
		/^[a-zA-Z0-9_-]+:.*?## / { \
			printf "  \033[32m%-22s\033[0m %s\n", $$1, $$2; \
		} \
	' $(MAKEFILE_LIST)
	@echo ""

setup: ## Install dependencies into .venv via uv (includes [dev] extras for pytest)
	@echo "==> Setting up environment..."
	@if [ -n "$(UV)" ] && [ -x "$(UV)" ]; then \
		echo "Found uv: $(UV)"; \
		$(UV) sync --extra dev; \
	elif command -v uv >/dev/null 2>&1; then \
		echo "Found uv: $$(command -v uv)"; \
		uv sync --extra dev; \
	else \
		echo "Notice: uv not found, falling back to $(PYTHON) -m pip"; \
		$(PYTHON) -m pip install -e ".[dev]"; \
	fi
	@echo "==> Setup completed. Make targets will use: $$( [ -x "$(VENV_PYTHON)" ] && echo "$(VENV_PYTHON)" || echo "$(PYTHON)" )"

doctor: ## Diagnostic tool to inspect environment, python, and container tools
	@echo "==> Environment Diagnostic:"
	@echo "  OS:             $$(uname -s) $$(uname -r)"
	@echo "  Python:         $(PYTHON) ($$($(PYTHON) --version 2>&1))"
	@echo "  Venv:           $$( [ -x "$(VENV_PYTHON)" ] && echo "$(VENV_PYTHON)" || echo "missing — run: make setup / uv sync --extra dev" )"
	@echo "  Same as uv?:    $$([ "$(PYTHON)" = "$(VENV_PYTHON)" ] && echo YES || echo NO — Make is not using .venv)"
	@echo "  numpy:          $$($(PYTHON) -c 'import numpy; print(numpy.__version__)' 2>/dev/null || echo 'NOT FOUND in selected Python')"
	@echo "  pytest:         $$($(PYTHON) -c 'import pytest; print(pytest.__version__)' 2>/dev/null || echo 'NOT FOUND — run: make setup')"
	@echo "  uv:             $$(if [ -n "$(UV)" ]; then echo "$(UV)"; else echo "Not found (optional)"; fi)"
	@echo "  Docker:         $$(which docker 2>/dev/null || echo 'Not installed')"
	@echo "  Docker Compose: $$(docker compose version 2>/dev/null || echo 'Not available')"
	@echo "  Git:            $$(git --version 2>/dev/null || echo 'Not found')"
	@echo "  SCALE:          $(SCALE) (tick row budget; does not multiply OHLCV volume)"
	@echo "  SEED:           $(SEED)"
	@echo "  MODE:           $(MODE)"
	@echo "  SPEED:          $(SPEED)"
	@echo "  OUT_DIR:        $(OUT_DIR)"
	@echo "  OFFLINE:        $(OFFLINE_START) -> $(OFFLINE_END) (daily OHLCV)"
	@echo "  ONLINE:         $(ONLINE_START) -> $(ONLINE_END) (tick corpus)"
	@echo "  ENGINE:         $(ENGINE) (sim | spark | flink)"
	@echo "  KAFKA_BOOTSTRAP:$(KAFKA_BOOTSTRAP)"

.PHONY: help setup doctor
