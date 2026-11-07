# ==============================================================================
# Module: 00_env.mk - Environment, Global Parameters & Help System
# ==============================================================================

SHELL := /bin/bash
UV := $(shell which uv 2>/dev/null)
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

# Export standard variables for downstream sub-shells and scripts
export PYTHONPATH := src
export PYTHON
export SCALE
export SEED
export MODE
export SPEED
export OUT_DIR

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
	@if [ -n "$(UV)" ]; then \
		echo "Found uv: $(UV)"; \
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
	@echo "  SCALE:          $(SCALE)"
	@echo "  SEED:           $(SEED)"
	@echo "  MODE:           $(MODE)"
	@echo "  SPEED:          $(SPEED)"
	@echo "  OUT_DIR:        $(OUT_DIR)"

.PHONY: help setup doctor
