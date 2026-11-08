# ==============================================================================
# Module: 03_processing.mk - PySpark Batch & Apache Flink Streaming (Phase 2)
# ==============================================================================

# --- [ Stream & Batch Processing ] ---

spark-baseline spark-skew-baseline: ## Run PySpark skew baseline job showing stragglers (Rubric M1-09)
	@echo "==> Running PySpark baseline job (ENGINE=$(ENGINE))..."
	$(PYTHON) -m quant_edai.processing.spark.baseline_job --source-dir $(OUT_DIR) --partitions 16 --engine $(ENGINE)

spark-optimized spark-skew-optimized: ## Run PySpark skew optimization with salting & AQE (Rubric M1-10)
	@echo "==> Running PySpark skew-optimized job (ENGINE=$(ENGINE), Salting K=16, Broadcast Join, AQE)..."
	$(PYTHON) -m quant_edai.processing.spark.skew_optimized --source-dir $(OUT_DIR) --partitions 16 --salt-factor 16 --engine $(ENGINE)

spark-evolution spark-dedup: ## Run PySpark schema evolution & deduplication job (Rubric M1-11, M1-12)
	@echo "==> Running PySpark deduplication and Iceberg schema evolution job (ENGINE=$(ENGINE))..."
	$(PYTHON) -m quant_edai.processing.spark.dedup_job --source-dir $(OUT_DIR) --target-dir $(OUT_DIR)/stg_market_ohlcv --engine $(ENGINE)

iceberg-compaction: ## Run Lakehouse Iceberg binpack compaction procedure (Rubric M1-18)
	@echo "==> Running Lakehouse Iceberg compaction (ENGINE=$(ENGINE), rewrite_data_files binpack)..."
	$(PYTHON) -m quant_edai.processing.spark.iceberg_compaction --table-dir $(OUT_DIR)/raw_market_ohlcv --compacted-dir $(OUT_DIR)/compacted_market_ohlcv --engine $(ENGINE)

verify-scd2: ## Run SCD Type 2 dimension builder and point-in-time check (Rubric M1-33)
	@echo "==> Running SCD Type 2 dimension builder (ENGINE=$(ENGINE)) for dim_companies..."
	$(PYTHON) -m quant_edai.processing.spark.scd2_builder --source $(OUT_DIR)/dim_companies/dim_companies.parquet --target $(OUT_DIR)/gold_dim_companies --engine $(ENGINE)

flink-baseline: ## Run Flink baseline job showing late data loss in processing time (Rubric M1-14)
	@echo "==> Running Flink baseline job (Processing Time)..."
	$(PYTHON) -c "import pandas as pd, json; from quant_edai.processing.flink.streaming_job import FlinkStreamingJob; df = pd.read_json('$(OUT_DIR)/streaming_ticks.jsonl', lines=True); res = FlinkStreamingJob().run_baseline_processing_time(df); print(json.dumps(res, indent=2))"

flink-optimized flink-window: ## Run Flink event-time 1m tumbling & 5m sliding windows (Rubric M1-15, M1-17)
	@echo "==> Running Flink event-time windowing (ENGINE=$(ENGINE)) with BoundedOutOfOrderness watermarks..."
	$(PYTHON) -m quant_edai.processing.flink.streaming_job --input-file $(OUT_DIR)/streaming_ticks.jsonl --out-dir $(OUT_DIR)/flink_output --engine $(ENGINE)

flink-dedup flink-stream-dedup: ## Run Flink RocksDB stateful deduplication filter (Rubric M1-16)
	@echo "==> Running Flink RocksDB stateful deduplication filter..."
	$(PYTHON) -m quant_edai.processing.flink.streaming_dedup --input-file $(OUT_DIR)/streaming_ticks.jsonl

flink-build: ## Build custom PyFlink Docker image (quant_edai_flink:1.18.1-py310)
	@echo "==> Building custom PyFlink container image from repo root..."
	docker build -t quant_edai_flink:1.18.1-py310 -f infra/docker/Dockerfile.flink .

flink-cancel-all: ## Cancel all currently running jobs on Flink cluster
	@echo "==> Cancelling all running jobs on Flink cluster..."
	@docker exec quant_flink_jobmanager python -c "import urllib.request, json; [urllib.request.urlopen(urllib.request.Request(f'http://localhost:8081/jobs/{j[\"id\"]}?mode=cancel', method='PATCH')) for j in json.loads(urllib.request.urlopen('http://localhost:8081/jobs').read().decode()).get('jobs', []) if j.get('status') == 'RUNNING']" 2>/dev/null || true
	@echo "==> All running jobs cancelled."

flink-submit flink-submit-optimized: flink-cancel-all ## Submit PyFlink optimized streaming job (M1-15, M1-16, M1-17, DLQ)
	@echo "==> Submitting PyFlink Optimized Streaming Job (Dedup TTL 1h, Tumble 1m, Hop 5m, DLQ)..."
	docker exec quant_flink_jobmanager /opt/flink/bin/flink run -m localhost:8081 -d -py /opt/quant_edai/src/quant_edai/processing/flink/pyflink_streaming_job.py
	@echo "==> Optimized job submitted. Inspect topology and metrics on http://localhost:8081"

flink-submit-baseline: flink-cancel-all ## Submit PyFlink baseline streaming job using Processing Time (M1-14)
	@echo "==> Submitting PyFlink Baseline Streaming Job (PROCTIME, No Dedup -> market.bars.1m.baseline)..."
	docker exec quant_flink_jobmanager /opt/flink/bin/flink run -m localhost:8081 -d -py /opt/quant_edai/src/quant_edai/processing/flink/pyflink_baseline_job.py
	@echo "==> Baseline job submitted. Inspect topology on http://localhost:8081"

flink-list: ## List active and completed jobs on Flink cluster
	@echo "==> Listing Flink cluster jobs:"
	docker exec quant_flink_jobmanager /opt/flink/bin/flink list -m localhost:8081

flink-cancel: ## Cancel running Flink job (Usage: make flink-cancel JOB_ID=<id>)
	@if [ -z "$(JOB_ID)" ]; then echo "Error: specify JOB_ID (e.g. make flink-cancel JOB_ID=...)"; exit 1; fi
	@echo "==> Cancelling Flink job $(JOB_ID)..."
	docker exec quant_flink_jobmanager /opt/flink/bin/flink cancel -m localhost:8081 $(JOB_ID)

.PHONY: spark-baseline spark-skew-baseline spark-optimized spark-skew-optimized spark-evolution spark-dedup iceberg-compaction verify-scd2 flink-baseline flink-optimized flink-window flink-dedup flink-stream-dedup flink-build flink-cancel-all flink-submit flink-submit-optimized flink-submit-baseline flink-list flink-cancel
