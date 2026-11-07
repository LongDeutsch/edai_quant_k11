# ==============================================================================
# Quant AI Engineering Platform - Root Makefile (Single Entry Point)
# ==============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Module directory
MAKE_DIR := make

# Load all modular makefiles in deterministic dependency order
MAKE_MODULES := \
    $(MAKE_DIR)/00_env.mk \
    $(MAKE_DIR)/01_infra.mk \
    $(MAKE_DIR)/02_generator.mk \
    $(MAKE_DIR)/03_processing.mk \
    $(MAKE_DIR)/04_orchestration.mk \
    $(MAKE_DIR)/05_mlops.mk \
    $(MAKE_DIR)/06_agents.mk \
    $(MAKE_DIR)/07_quant.mk \
    $(MAKE_DIR)/08_test.mk \
    $(MAKE_DIR)/09_phases.mk

include $(MAKE_MODULES)
