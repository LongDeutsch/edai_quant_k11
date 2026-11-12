# ==============================================================================
# Module: 03_processing.mk - PySpark Batch & Apache Flink Streaming (Phase 2)
# ==============================================================================

# --- [ Stream & Batch Processing ] ---

spark-baseline spark-skew-baseline: ## Run PySpark skew baseline job showing stragglers (Rubric M1-09)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> PySpark baseline (ENGINE=$(ENGINE)) $$DR"; \
	$(PYTHON) -m quant_edai.processing.spark.baseline_job --source-dir $$DR --partitions 16 --engine $(ENGINE)

spark-optimized spark-skew-optimized: ## Run PySpark skew optimization with salting & AQE (Rubric M1-10)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> PySpark skew-optimized (ENGINE=$(ENGINE)) $$DR"; \
	$(PYTHON) -m quant_edai.processing.spark.skew_optimized --source-dir $$DR --partitions 16 --salt-factor 16 --engine $(ENGINE)

spark-evolution spark-dedup: ## Run PySpark schema evolution & deduplication job (Rubric M1-11, M1-12)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> PySpark dedup (ENGINE=$(ENGINE)) $$DR"; \
	$(PYTHON) -m quant_edai.processing.spark.dedup_job --source-dir $$DR --target-dir $$DR/stg_market_ohlcv --engine $(ENGINE)

iceberg-compaction: ## Run Lakehouse Iceberg binpack compaction procedure (Rubric M1-18)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> Iceberg compaction (ENGINE=$(ENGINE)) $$DR"; \
	$(PYTHON) -m quant_edai.processing.spark.iceberg_compaction --table-dir $$DR/raw_market_ohlcv --compacted-dir $$DR/compacted_market_ohlcv --engine $(ENGINE)

verify-scd2: ## Run SCD Type 2 dimension builder and point-in-time check (Rubric M1-33)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> SCD2 (ENGINE=$(ENGINE)) $$DR"; \
	$(PYTHON) -m quant_edai.processing.spark.scd2_builder --source $$DR/dim_companies/dim_companies.parquet --target $$DR/gold_dim_companies --engine $(ENGINE)

flink-baseline: ## Run Flink baseline job showing late data loss in processing time (Rubric M1-14)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> Flink baseline $$DR"; \
	$(PYTHON) -c "import pandas as pd, json; from quant_edai.processing.flink.streaming_job import FlinkStreamingJob; df = pd.read_json('$$DR/streaming_ticks.jsonl', lines=True); res = FlinkStreamingJob().run_baseline_processing_time(df); print(json.dumps(res, indent=2))"

flink-optimized flink-window: ## Run Flink event-time 1m tumbling & 5m sliding windows (Rubric M1-15, M1-17)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> Flink event-time (ENGINE=$(ENGINE)) $$DR"; \
	$(PYTHON) -m quant_edai.processing.flink.streaming_job --input-file $$DR/streaming_ticks.jsonl --out-dir $$DR/flink_output --engine $(ENGINE)

flink-dedup flink-stream-dedup: ## Run Flink RocksDB stateful deduplication filter (Rubric M1-16)
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> Flink dedup $$DR"; \
	$(PYTHON) -m quant_edai.processing.flink.streaming_dedup --input-file $$DR/streaming_ticks.jsonl

flink-build: ## Build custom PyFlink Docker image (quant_edai_flink:1.18.1-py310)
	@echo "==> Building custom PyFlink container image from repo root..."
	docker build -t quant_edai_flink:1.18.1-py310 -f infra/docker/Dockerfile.flink .

flink-recreate: flink-build ## Rebuild PyFlink image and recreate Flink JM/TM with source mount
	@echo "==> Recreating Flink cluster with quant_edai_flink:1.18.1-py310..."
	@$(COMPOSE) up -d --force-recreate flink-jobmanager flink-taskmanager
	@echo "==> Waiting for Flink JobManager..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12; do \
		curl -sf http://127.0.0.1:8081/overview >/dev/null && break; \
		sleep 5; \
	done
	@curl -sf http://127.0.0.1:8081/overview >/dev/null && echo "==> Flink ready at http://localhost:8081" || (echo "Flink JM not healthy yet"; exit 1)

flink-cancel-all: ## Cancel all currently running jobs on Flink cluster
	@echo "==> Cancelling all running jobs on Flink cluster..."
	@docker exec quant_flink_jobmanager bash -lc '\
		ids=$$(curl -sf http://127.0.0.1:8081/jobs | tr "," "\n" | sed -n "s/.*\"id\":\"\\([a-f0-9]*\\)\".*/\\1/p"); \
		for id in $$ids; do \
			status=$$(curl -sf "http://127.0.0.1:8081/jobs/$$id" | tr "," "\n" | sed -n "s/.*\"status\":\"\\([A-Z]*\\)\".*/\\1/p" | head -1); \
			if [ "$$status" = "RUNNING" ] || [ "$$status" = "CREATED" ] || [ "$$status" = "RESTARTING" ]; then \
				curl -sf -X PATCH "http://127.0.0.1:8081/jobs/$$id?mode=cancel" >/dev/null || true; \
				echo "Cancelled $$id ($$status)"; \
			fi; \
		done' 2>/dev/null || true
	@echo "==> All running jobs cancelled."

flink-submit flink-submit-optimized: ## Submit PyFlink optimized streaming job (M1-15, M1-16, M1-17, DLQ)
	@docker image inspect quant_edai_flink:1.18.1-py310 >/dev/null 2>&1 || $(MAKE) flink-build
	@docker exec quant_flink_jobmanager test -f /opt/quant_edai/src/quant_edai/processing/flink/pyflink_streaming_job.py \
		|| (echo "PyFlink sources not mounted. Run: make flink-recreate"; exit 1)
	@$(MAKE) flink-cancel-all
	@echo "==> Submitting PyFlink Optimized Streaming Job (Dedup TTL 1h, Tumble 1m, Hop 5m, DLQ)..."
	docker exec quant_flink_jobmanager /opt/flink/bin/flink run -m localhost:8081 -d -py /opt/quant_edai/src/quant_edai/processing/flink/pyflink_streaming_job.py
	@echo "==> Optimized job submitted. Inspect topology and metrics on http://localhost:8081"

flink-submit-baseline: ## Submit PyFlink baseline streaming job using Processing Time (M1-14)
	@docker image inspect quant_edai_flink:1.18.1-py310 >/dev/null 2>&1 || $(MAKE) flink-build
	@docker exec quant_flink_jobmanager test -f /opt/quant_edai/src/quant_edai/processing/flink/pyflink_baseline_job.py \
		|| (echo "PyFlink sources not mounted. Run: make flink-recreate"; exit 1)
	@$(MAKE) flink-cancel-all
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

.PHONY: spark-baseline spark-skew-baseline spark-optimized spark-skew-optimized spark-evolution spark-dedup iceberg-compaction verify-scd2 flink-baseline flink-optimized flink-window flink-dedup flink-stream-dedup flink-build flink-recreate flink-cancel-all flink-submit flink-submit-optimized flink-submit-baseline flink-list flink-cancel
