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

trader-publish-bronze: ## Publish Bronze raw_market_ohlcv → Kafka market.ticks + market.bars.1m
	@echo "==> Publishing Bronze lakehouse data onto Kafka (trader never reads Parquet)..."
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.bronze_kafka_publisher --kafka-bootstrap $(KAFKA_BOOTSTRAP)

trader-run: ## Live paper trader: Kafka consumer on market.bars.1m only (Phase 6)
	@echo "==> Live trader consuming Kafka topic market.bars.1m (publish Bronze first if empty)..."
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.market_replay --mode live --reset-db --max-bars 300 --kafka-bootstrap $(KAFKA_BOOTSTRAP) --output-dir evidence/phase6

trader-replay: ## Offline synthetic replay only (no Parquet/Kafka) for smoke/unit evidence
	@echo "==> Synthetic replay (offline smoke). For Bronze→Kafka path use: make trader-publish-bronze && make trader-run"
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.market_replay --mode replay --speed $(SPEED) --reset-db --output-dir evidence/phase6

dashboard: ## Launch Streamlit multi-page real-time trading & alpha dashboard on :8501
	@echo "==> Starting Streamlit dashboard at http://localhost:8501 ..."
	PYTHONPATH=src $(PYTHON) -m streamlit run src/quant_edai/serving/dashboard/app.py --server.port 8501

test-quant: ## Run mathematical and functional tests for quant trading engine & metrics
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_quant_metrics.py tests/unit/test_alpha_synthesis.py tests/unit/test_backtester.py tests/unit/test_paper_trading.py -v

paper-trade-replay: trader-replay ## Alias for trader-replay (Phase 6)
paper-trade-simulator: trader-run ## Alias for trader-run (Phase 6)

.PHONY: quant-backtest trader-publish-bronze trader-run trader-replay dashboard test-quant paper-trade-replay paper-trade-simulator
