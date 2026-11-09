# ==============================================================================
# Module: 07_quant.mk - Quant Alpha Evaluation & Paper Trading (Phases 5 & 6)
# ==============================================================================

SPEED ?= 20
PROFILE ?= intraday

# --- [ Phase 5: Quant Alpha Backtesting (M1-37) ] ---

quant-backtest: ## Run walk-forward alpha factor evaluation vs SPY (Phase 5)
	@mkdir -p evidence/phase5
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.backtester --output-dir evidence/phase5

# --- [ Phase 6: Real-Time Paper Trading & Intraday Trader (M1-38) ] ---

trader-run: ## Run event-driven intraday trader consuming Kafka market.bars.1m (Phase 6)
	@echo "Notice: Target '$@' runs paper_trading.py in live Kafka streaming mode."
	@echo "Refer to docs/plan/INTRADAY_TRADING_DASHBOARD.md."
	@exit 1

trader-replay: ## Replay market bars with speed multiplier (SPEED=$(SPEED)) into paper trader (Phase 6)
	@echo "Notice: Target '$@' runs market_replay.py feeding paper_trading.py (SPEED=$(SPEED))."
	@echo "Refer to docs/plan/INTRADAY_TRADING_DASHBOARD.md."
	@exit 1

dashboard: ## Launch Streamlit multi-page real-time trading & alpha dashboard on :8501
	@echo "Notice: Target '$@' launches Streamlit app under src/quant_edai/serving/dashboard/app.py."
	@echo "Refer to docs/plan/INTRADAY_TRADING_DASHBOARD.md."
	@exit 1

test-quant: ## Run mathematical and functional tests for quant trading engine & metrics
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_quant_metrics.py tests/unit/test_alpha_synthesis.py tests/unit/test_backtester.py -v

paper-trade-replay: trader-replay ## Alias for trader-replay (Phase 6)
paper-trade-simulator: trader-run ## Alias for trader-run (Phase 6)

.PHONY: quant-backtest trader-run trader-replay dashboard test-quant paper-trade-replay paper-trade-simulator
