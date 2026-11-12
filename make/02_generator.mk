# ==============================================================================
# Module: 02_generator.mk - Synthetic Market Data & Anomaly Generation
# ==============================================================================

# --- [ Data Ingestion & Faults Generator ] ---

# Coursework smoke window (phase1). Use generate-offline / generate-data for 3y offline.
START_DATE ?= 2024-01-02
END_DATE ?= 2024-10-31

# Contiguous offline daily OHLCV vs online tick corpus windows (no overlap).
OFFLINE_START ?= 2022-11-01
OFFLINE_END ?= 2025-10-31
ONLINE_START ?= 2025-11-01
ONLINE_END ?= 2026-11-10

# Optional cap for stream corpus replay (CI/smoke). Empty = full corpus.
STREAM_MAX_TICKS ?=
MODE ?= smoke

generate: ## Run synthetic Bronze generator (immutable runs/{dataset_id}; no gold features)
	@echo "==> Generating Bronze synthetic data (SCALE=$(SCALE), SEED=$(SEED), MODE=$(MODE), $(START_DATE)->$(END_DATE))..."
	$(PYTHON) -m quant_edai.generator.data_generator --mode $(MODE) --scale $(SCALE) --seed $(SEED) --out-dir $(OUT_DIR) --start-date $(START_DATE) --end-date $(END_DATE) $(if $(DATASET_ID),--dataset-id $(DATASET_ID),)
	@echo "==> CURRENT dataset: $$(cat $(OUT_DIR)/CURRENT_DATASET_ID 2>/dev/null || echo unknown)"

generate-ticks: generate ## Alias: Generate Raw Market Ticks (MODE=smoke|test|full)

resample: ## Resample Silver Ticks to Gold 1m and Daily Bars (ENGINE=$(ENGINE))
	@DR=$$($(RESOLVE_DATASET_ROOT)); \
	echo "==> Resampling Silver Ticks -> Gold 1m & Daily Bars (ENGINE=$(ENGINE)) dataset_root=$$DR"; \
	$(PYTHON) -m quant_edai.processing.spark.resample_job --source-dir $$DR/stg_market_ticks --gold-1m-dir $$DR/gold_market_bars_1m --gold-daily-dir $$DR/gold_market_bars_daily --engine $(ENGINE)

generate-offline: ## Offline 3y daily OHLCV Bronze only (OFFLINE_START→OFFLINE_END); no gold features
	@echo "==> Generating offline 3y Bronze OHLCV ($(OFFLINE_START)->$(OFFLINE_END); no gold features) -> $(OUT_DIR)/runs/..."
	$(PYTHON) -m quant_edai.generator.data_generator --mode $(MODE) --scale $(SCALE) --seed $(SEED) --out-dir $(OUT_DIR) --start-date $(OFFLINE_START) --end-date $(OFFLINE_END) $(if $(DATASET_ID),--dataset-id $(DATASET_ID),)

generate-3y: generate-offline ## Alias: offline 3y daily Bronze OHLCV

generate-canonical-4y: ## Canonical 4Y generation (2022-11-01 -> 2026-10-31; 3 splits: IS, OOS, TRADING)
	@echo "==> Generating Canonical 4Y Bronze data (SCALE=$(if $(SCALE),$(SCALE),0.01), SEED=$(SEED))..."
	$(PYTHON) -m quant_edai.generator.data_generator --profile canonical_4y --scale $(if $(SCALE),$(SCALE),0.01) --seed $(SEED) --out-dir $(OUT_DIR) $(if $(DATASET_ID),--dataset-id $(DATASET_ID),)
	@echo "==> CURRENT dataset: $$(cat $(OUT_DIR)/CURRENT_DATASET_ID 2>/dev/null || echo unknown)"

calibrate-scaling: ## Calibrate offline scaling projection for 10 GB and 100 GB workloads
	$(PYTHON) -m quant_edai.generator.data_generator --calibrate-scaling

generate-online-ticks: ## Materialize online 1y tick corpus into CURRENT (or new) dataset run
	@echo "==> Materializing online tick corpus ($(ONLINE_START)->$(ONLINE_END), SCALE=$(SCALE)) under runs/{dataset_id}"
	$(PYTHON) -m quant_edai.generator.tick_corpus --corpus online --scale $(SCALE) --seed $(SEED) --out-dir $(OUT_DIR) --start-date $(ONLINE_START) --end-date $(ONLINE_END) $(if $(DATASET_ID),--dataset-id $(DATASET_ID),)

generate-offline-ticks: ## Optional offline-window tick sample for heavy Spark/Flink benches
	@echo "==> Materializing offline tick corpus ($(OFFLINE_START)->$(OFFLINE_END), SCALE=$(SCALE)) under runs/{dataset_id}"
	$(PYTHON) -m quant_edai.generator.tick_corpus --corpus offline --scale $(SCALE) --seed $(SEED) --out-dir $(OUT_DIR) --start-date $(OFFLINE_START) --end-date $(OFFLINE_END) $(if $(DATASET_ID),--dataset-id $(DATASET_ID),)

generate-data: ## Canonical full gen: offline 3y daily OHLCV + online 1y ticks (same dataset_id)
	@echo "==> generate-data: offline daily + online ticks (SCALE=$(SCALE))"
	@$(MAKE) generate-offline
	@$(MAKE) generate-online-ticks DATASET_ID=$$(cat $(OUT_DIR)/CURRENT_DATASET_ID)

stream-test: ## Replay online tick corpus to streaming_ticks.jsonl (builds corpus if missing)
	@DR=$$($(RESOLVE_DATASET_ROOT)); \
	test -d $$DR/raw_market_ticks_online || $(MAKE) generate-online-ticks; \
	DR=$$($(RESOLVE_DATASET_ROOT)); \
	echo "==> Replaying online tick corpus -> $$DR/streaming_ticks.jsonl"; \
	$(PYTHON) -m quant_edai.generator.streaming_producer --seed $(SEED) --from-corpus $$DR/raw_market_ticks_online $(if $(STREAM_MAX_TICKS),--max-ticks $(STREAM_MAX_TICKS),) --out-file $$DR/streaming_ticks.jsonl

stream-kafka: ## Replay online corpus to Kafka (JSON) + local jsonl audit
	@DR=$$($(RESOLVE_DATASET_ROOT)); \
	test -d $$DR/raw_market_ticks_online || $(MAKE) generate-online-ticks; \
	DR=$$($(RESOLVE_DATASET_ROOT)); \
	echo "==> Replaying online tick corpus to Kafka ($(KAFKA_BOOTSTRAP))..."; \
	$(PYTHON) -m quant_edai.generator.streaming_producer --seed $(SEED) --kafka-bootstrap $(KAFKA_BOOTSTRAP) --topic market.ticks --format json --inject-malformed --from-corpus $$DR/raw_market_ticks_online $(if $(STREAM_MAX_TICKS),--max-ticks $(STREAM_MAX_TICKS),) --out-file $$DR/streaming_ticks.jsonl

stream-kafka-avro: ## Replay online corpus in Confluent Avro Wire Format with Schema Registry
	@DR=$$($(RESOLVE_DATASET_ROOT)); \
	test -d $$DR/raw_market_ticks_online || $(MAKE) generate-online-ticks; \
	DR=$$($(RESOLVE_DATASET_ROOT)); \
	echo "==> Replaying online tick corpus (Avro) to Kafka ($(KAFKA_BOOTSTRAP))..."; \
	$(PYTHON) -m quant_edai.generator.streaming_producer --seed $(SEED) --kafka-bootstrap $(KAFKA_BOOTSTRAP) --topic market.ticks --format avro --registry-url http://localhost:8085 --from-corpus $$DR/raw_market_ticks_online $(if $(STREAM_MAX_TICKS),--max-ticks $(STREAM_MAX_TICKS),) --out-file $$DR/streaming_ticks.jsonl

benchmark-images: ## Analyze Dockerfile multi-stage optimization metrics
	@echo "==> Evaluating Docker image optimization metrics..."
	@$(PYTHON) -m quant_edai.benchmark.docker_benchmark

.PHONY: generate generate-offline generate-3y generate-canonical-4y calibrate-scaling generate-online-ticks generate-offline-ticks generate-data stream-test stream-kafka stream-kafka-avro benchmark-images
