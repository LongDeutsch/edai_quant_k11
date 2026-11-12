# ==============================================================================
# Module: 07_quant.mk - Quant Alpha Evaluation & Paper Trading (Phases 5 & 6)
# ==============================================================================

SPEED ?= 20
PROFILE ?= intraday

# --- [ Phase 5: Quant Alpha Backtesting (M1-37) ] ---

quant-backtest: ## Run walk-forward alpha factor evaluation vs SPY (Phase 5)
	@mkdir -p evidence/phase5
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.backtester --output-dir evidence/phase5

alpha-seed: ## Seed 10 INTRADAY + 10 DAILY alphas into registry and publish daily T+1 signals
	@echo "==> Seeding 10 alpha strategies x INTRADAY/DAILY into paper DB registry..."
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.alpha_seed --source-dir $(OUT_DIR) --out-file evidence/phase6/alpha_seed_metrics.json

alpha-metrics: ## Refresh alpha metrics_snapshot (Sharpe/MDD/win-rate/PnL) over last 2y features
	@echo "==> Computing alpha metrics from lakehouse/features (booksize=100000)..."
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.alpha_metrics_service --source-dir $(OUT_DIR) --booksize 100000 --out-file evidence/phase6/alpha_metrics_refresh.json

# --- [ Phase 6: Real-Time Paper Trading & Intraday Trader (M1-38) ] ---

trader-publish-bronze: ## Publish Bronze raw_market_ohlcv → Kafka market.ticks + market.bars.1m
	@echo "==> Publishing Bronze lakehouse data onto Kafka (trader never reads Parquet)..."
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.bronze_kafka_publisher --kafka-bootstrap $(KAFKA_BOOTSTRAP)

bars-stream-kafka: ## Run continuous 1m bar feeder into Kafka market.bars.1m (infinite live feed)
	@echo "==> Running Continuous Bar Feeder to Kafka topic market.bars.1m (Press Ctrl+C to stop)..."
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.continuous_bar_feeder --kafka-bootstrap $(KAFKA_BOOTSTRAP) --interval 1.0

alpha-promote-demo: ## Quick-promote candidate alpha to PROMOTED_ACTIVE for live trading demo
	@echo "==> Promoting alpha_composite_wf_m137:v1.0.0 to PROMOTED_ACTIVE in Alpha Registry..."
	PYTHONPATH=src $(PYTHON) -c 'from quant_edai.quant.alpha_registry import AlphaRegistry; from quant_edai.quant.paper_db import PaperDatabase; r = AlphaRegistry(db=PaperDatabase()); r.register_candidate({"alpha_id": "alpha_composite_wf_m137", "version": "v1.0.0", "horizon": "DAILY", "status": "CANDIDATE", "out_of_sample_metrics": {"ic_mean": 0.147, "sharpe": 2.65, "max_drawdown": -0.114}}); r.promote_to_live("alpha_composite_wf_m137", "v1.0.0", pm_user="PM_CLI", reason="Automated demo promotion"); print("Successfully promoted alpha_composite_wf_m137:v1.0.0 to PROMOTED_ACTIVE.")'

trader-run: ## Live paper trader: Kafka consumer on market.bars.1m (alpha-source=store)
	@echo "==> Live trader consuming Kafka market.bars.1m (PROFILE=$(PROFILE), alpha-source=store)..."
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.market_replay --mode live --reset-db --max-bars 300 --profile $(PROFILE) --alpha-source store --kafka-bootstrap $(KAFKA_BOOTSTRAP) --output-dir evidence/phase6

trader-run-store: ## Continuous live trader: Kafka consumer running indefinitely (--max-bars 0)
	@echo "==> Continuous live trader (running until Ctrl+C, PROFILE=$(PROFILE), alpha-source=store)..."
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.market_replay --mode live --max-bars 0 --profile $(PROFILE) --alpha-source store --kafka-bootstrap $(KAFKA_BOOTSTRAP) --output-dir evidence/phase6

trader-live-run: ## Run live closed-loop paper trader on dataset bars with provenance verification
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> Running live paper trader on dataset $$DR..."; \
	PYTHONPATH=src $(PYTHON) -m pytest tests/live/test_live_05_trader_e2e_closed_loop.py -v

trader-replay: ## Offline synthetic replay only (no Parquet/Kafka) for smoke/unit evidence
	@echo "==> Synthetic replay (offline smoke). For Bronze→Kafka path use: make trader-publish-bronze && make trader-run"
	@mkdir -p evidence/phase6
	PYTHONPATH=src $(PYTHON) -m quant_edai.quant.market_replay --mode replay --speed $(SPEED) --reset-db --profile $(PROFILE) --alpha-source inline --output-dir evidence/phase6

dashboard: ## Launch Streamlit multi-page real-time trading & alpha dashboard on :8501
	@echo "==> Starting Streamlit dashboard at http://localhost:8501 ..."
	PYTHONPATH=src $(PYTHON) -m streamlit run src/quant_edai/serving/dashboard/app.py --server.port 8501

ui: dashboard ## Alias for dashboard

test-quant: ## Run mathematical and functional tests for quant trading engine & metrics
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_quant_metrics.py tests/unit/test_alpha_synthesis.py tests/unit/test_alpha_catalog.py tests/unit/test_backtester.py tests/unit/test_paper_trading.py tests/unit/test_alpha_lifecycle.py tests/unit/test_alpha_metrics_service.py tests/unit/test_ohlcv_3y_dataset.py -v

paper-trade-replay: trader-replay ## Alias for trader-replay (Phase 6)
paper-trade-simulator: trader-run ## Alias for trader-run (Phase 6)

.PHONY: quant-backtest alpha-seed alpha-metrics trader-publish-bronze bars-stream-kafka alpha-promote-demo trader-run trader-run-store trader-replay dashboard ui test-quant paper-trade-replay paper-trade-simulator
