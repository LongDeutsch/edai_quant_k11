# ==============================================================================
# Module: 07_quant.mk - Quant Alpha Evaluation & Paper Trading (Phases 6 & 7)
# ==============================================================================

# --- [ Quant Alpha & Paper Trading ] ---

quant-backtest: ## Run walk-forward alpha factor evaluation vs SPY (Phase 6)
	@echo "Notice: Target '$@' is scheduled for Phase 6."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-6 for technical design."
	@exit 1

paper-trade-replay: ## Replay out-of-sample market data to Kafka (SPEED=$(SPEED)) (Phase 7)
	@echo "Notice: Target '$@' is scheduled for Phase 7."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-7 for technical design."
	@exit 1

paper-trade-simulator: ## Start paper trading simulator with state persistence (Phase 7)
	@echo "Notice: Target '$@' is scheduled for Phase 7."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-7 for technical design."
	@exit 1

.PHONY: quant-backtest paper-trade-replay paper-trade-simulator
