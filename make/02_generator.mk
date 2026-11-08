# ==============================================================================
# Module: 02_generator.mk - Synthetic Market Data & Anomaly Generation
# ==============================================================================

# --- [ Data Ingestion & Faults Generator ] ---

generate: ## Run synthetic market data generator (SCALE=$(SCALE), SEED=$(SEED))
	@echo "==> Generating synthetic market data (SCALE=$(SCALE), SEED=$(SEED))..."
	$(PYTHON) -m quant_edai.generator.data_generator --scale $(SCALE) --seed $(SEED) --out-dir $(OUT_DIR)

stream-test: ## Generate streaming tick events with late arrivals & duplicates
	@echo "==> Generating streaming tick events with late arrivals & duplicates..."
	$(PYTHON) -m quant_edai.generator.streaming_producer --seed $(SEED) --out-file $(OUT_DIR)/streaming_ticks.jsonl

stream-kafka: ## Publish streaming ticks directly to Kafka in JSON format (market.ticks)
	@echo "==> Publishing streaming tick events to Kafka broker ($(KAFKA_BOOTSTRAP))..."
	$(PYTHON) -m quant_edai.generator.streaming_producer --seed $(SEED) --kafka-bootstrap $(KAFKA_BOOTSTRAP) --topic market.ticks --format json --inject-malformed --out-file $(OUT_DIR)/streaming_ticks.jsonl

stream-kafka-avro: ## Publish streaming ticks in Confluent Avro Wire Format with Schema Registry
	@echo "==> Publishing streaming tick events in Confluent Avro format to Kafka ($(KAFKA_BOOTSTRAP))..."
	$(PYTHON) -m quant_edai.generator.streaming_producer --seed $(SEED) --kafka-bootstrap $(KAFKA_BOOTSTRAP) --topic market.ticks --format avro --registry-url http://localhost:8085 --out-file $(OUT_DIR)/streaming_ticks.jsonl

benchmark-images: ## Analyze Dockerfile multi-stage optimization metrics
	@echo "==> Evaluating Docker image optimization metrics..."
	@$(PYTHON) -m quant_edai.benchmark.docker_benchmark

.PHONY: generate stream-test stream-kafka stream-kafka-avro benchmark-images
