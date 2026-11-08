# ==============================================================================
# Module: 09_phases.mk - Milestone Phase Runners & Cleanup
# ==============================================================================

# --- [ Milestone Phase Runners ] ---

phase1: generate stream-test ## Execute Phase 1 end-to-end and gather evidence
	@echo "==> Executing Phase 1: Ingestion & Evidence Collection..."
	@mkdir -p evidence/phase1
	$(PYTHON) -m quant_edai.ingestion.s3_uploader --source-dir $(OUT_DIR)
	@cp -f $(OUT_DIR)/generation_stats.json evidence/phase1/data_generation_stats.json 2>/dev/null || true
	@echo "==> Phase 1 completed successfully. Evidence saved to evidence/phase1/"

phase2: ## Execute Phase 2: PySpark Skew, Iceberg Compaction & Flink Streaming (ENGINE=$(ENGINE))
	@echo "==> Executing Phase 2: Big Data Processing & Lakehouse Integration (ENGINE=$(ENGINE))..."
	@mkdir -p evidence/phase2
	@test -d $(OUT_DIR)/raw_market_ohlcv || (echo "Generating input data first..." && $(MAKE) phase1)
	@echo "1/6. Running PySpark baseline job..."
	$(PYTHON) -m quant_edai.processing.spark.baseline_job --source-dir $(OUT_DIR) --engine $(ENGINE) --out-file evidence/phase2/spark_baseline_metrics.json
	@echo "2/6. Running PySpark skew-optimized job (Key Salting & AQE)..."
	$(PYTHON) -m quant_edai.processing.spark.skew_optimized --source-dir $(OUT_DIR) --engine $(ENGINE) --out-file evidence/phase2/spark_optimized_metrics.json
	@$(PYTHON) -c "import json; b = json.load(open('evidence/phase2/spark_baseline_metrics.json')); o = json.load(open('evidence/phase2/spark_optimized_metrics.json')); comp = {'baseline_max_task_duration': b['max_task_duration_seconds'], 'optimized_max_task_duration': o['max_task_duration_seconds'], 'baseline_latency': b['effective_stage_latency_seconds'], 'optimized_latency': o['effective_stage_latency_seconds'], 'speedup': round(b['effective_stage_latency_seconds'] / max(0.01, o['effective_stage_latency_seconds']), 2), 'variance_reduction': f\"{round((1 - o['task_duration_variance']/max(0.0001, b['task_duration_variance'])) * 100, 2)}%\"}; json.dump(comp, open('evidence/phase2/spark_benchmark_comparison.json', 'w'), indent=2); print('Spark Benchmark:', json.dumps(comp, indent=2))"
	@echo "3/6. Running PySpark deduplication & schema evolution..."
	$(PYTHON) -m quant_edai.processing.spark.dedup_job --source-dir $(OUT_DIR) --target-dir $(OUT_DIR)/stg_market_ohlcv --engine $(ENGINE) --out-file evidence/phase2/spark_dedup_evolution_metrics.json
	@echo "4/6. Running Lakehouse Iceberg binpack compaction..."
	$(PYTHON) -m quant_edai.processing.spark.iceberg_compaction --table-dir $(OUT_DIR)/raw_market_ohlcv --compacted-dir $(OUT_DIR)/compacted_market_ohlcv --engine $(ENGINE) --out-file evidence/phase2/iceberg_compaction_metrics.json
	@echo "5/6. Running SCD Type 2 dimension builder for dim_companies..."
	$(PYTHON) -m quant_edai.processing.spark.scd2_builder --source $(OUT_DIR)/dim_companies/dim_companies.parquet --target $(OUT_DIR)/gold_dim_companies --engine $(ENGINE) --out-file evidence/phase2/scd2_verification.json
	@echo "6/6. Running Flink event-time windowing & RocksDB deduplication..."
	$(PYTHON) -m quant_edai.processing.flink.streaming_job --input-file $(OUT_DIR)/streaming_ticks.jsonl --out-dir $(OUT_DIR)/flink_output --engine $(ENGINE) --out-file evidence/phase2/flink_streaming_audit.json
	@echo "7/6. Running Phase 2 automated test verification..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_spark_salting.py tests/unit/test_flink_streaming.py tests/unit/test_iceberg_scd2.py tests/unit/test_schema_registry_and_avro.py tests/unit/test_dlq_and_kafka.py -v > evidence/phase2/pytest_test_run.txt
	@echo "==> Phase 2 completed successfully. Evidence saved to evidence/phase2/"

phase3: ## Execute Phase 3: Orchestration (DP1/2/3), Great Expectations, Lineage & Serving (ClickHouse/Pinot)
	@echo "==> Executing Phase 3: Airflow Orchestration, Data Quality & Serving (ENGINE=$(ENGINE))..."
	@mkdir -p evidence/phase3
	@test -d $(OUT_DIR)/raw_market_ohlcv || (echo "Generating input data first..." && $(MAKE) phase1)
	@echo "1/7. Triggering DP1 (Raw Ingestion & Great Expectations)..."
	@$(MAKE) airflow-trigger-dp1
	@echo "2/7. Triggering DP2 (Silver & Gold Lakehouse Processing)..."
	@$(MAKE) airflow-trigger-dp2
	@echo "3/7. Triggering DP3 (Quant Technical Features & Quality Gate)..."
	@$(MAKE) airflow-trigger-dp3
	@echo "4/7. Administering Apache Pinot realtime streaming table..."
	@$(MAKE) pinot-setup
	@echo "5/7. Synchronizing Gold datasets to ClickHouse OLAP..."
	@$(MAKE) clickhouse-sync
	@echo "6/7. Emitting DataHub end-to-end dataset lineage..."
	@$(MAKE) datahub-lineage
	@echo "7/7. Running Phase 3 automated test suite..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_phase3_quality_and_serving.py -v > evidence/phase3/pytest_test_run.txt
	@$(PYTHON) -c "import json, glob; summary = {'phase': 3, 'status': 'COMPLETED', 'artifacts': glob.glob('evidence/phase3/*')}; json.dump(summary, open('evidence/phase3/phase3_summary.json', 'w'), indent=2); print('Phase 3 Complete:', json.dumps(summary, indent=2))"
	@echo "==> Phase 3 completed successfully. Evidence saved to evidence/phase3/"

phase4 phase5 phase6 phase7 phase8 demo:
	@echo "Notice: Target '$@' is scheduled for subsequent implementation phases."
	@echo "Currently completed and verified milestones: Phase 1, Phase 2 & Phase 3."
	@echo "Refer to docs/plan/IMPLEMENTATION_STATUS.md for detailed status and evidence."
	@exit 1

clean: ## Clean generated temporary files and output directory
	@echo "==> Cleaning generated output directory ($(OUT_DIR))..."
	@rm -rf $(OUT_DIR)

.PHONY: phase1 phase2 phase3 phase4 phase5 phase6 phase7 phase8 demo clean
