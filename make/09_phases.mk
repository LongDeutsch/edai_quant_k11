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

phase3: ## Execute Phase 3: Orchestration (DP1/2/3), Great Expectations, Lineage & Serving (ClickHouse OLAP)
	@echo "==> Executing Phase 3: Airflow Orchestration, Data Quality & Serving (ENGINE=$(ENGINE))..."
	@mkdir -p evidence/phase3
	@test -d $(OUT_DIR)/raw_market_ohlcv || (echo "Generating input data first..." && $(MAKE) phase1)
	@echo "1/6. Triggering DP1 (Raw Ingestion & Great Expectations)..."
	@$(MAKE) airflow-trigger-dp1
	@echo "2/6. Triggering DP2 (Silver & Gold Lakehouse Processing)..."
	@$(MAKE) airflow-trigger-dp2
	@echo "3/6. Triggering DP3 (Quant Technical Features & Quality Gate)..."
	@$(MAKE) airflow-trigger-dp3
	@echo "4/6. Synchronizing Gold & Intraday datasets to ClickHouse OLAP (M1-19)..."
	@$(MAKE) clickhouse-sync
	@echo "5/6. Emitting DataHub end-to-end dataset lineage..."
	@$(MAKE) datahub-lineage
	@echo "6/6. Running Phase 3 automated test suite..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_phase3_quality_and_serving.py -v > evidence/phase3/pytest_test_run.txt
	@$(PYTHON) -c "import json, glob; summary = {'phase': 3, 'status': 'COMPLETED', 'artifacts': glob.glob('evidence/phase3/*')}; json.dump(summary, open('evidence/phase3/phase3_summary.json', 'w'), indent=2); print('Phase 3 Complete:', json.dumps(summary, indent=2))"
	@echo "==> Phase 3 completed successfully. Evidence saved to evidence/phase3/"

phase4: ## Execute Phase 4: Feast Feature Store, MLflow, KFP Training, KServe v2 Canary & Drift
	@echo "==> Executing Phase 4: Feature Store, Model Training, Serving & Continuous Drift Monitoring..."
	@mkdir -p evidence/phase4 models
	@echo "1/7. Applying Feast definitions and materializing online store..."
	@$(MAKE) feast-apply
	@$(MAKE) feast-materialize
	@echo "2/7. Verifying FastAPI Feature Fetch API (<10ms SLA)..."
	@$(MAKE) test-api
	@echo "3/7. Executing Kubeflow 4-Step Training Pipeline (MODE=$(MODE))..."
	@$(MAKE) kfp-run
	@echo "4/7. Checking MLflow experiment tracking and champion model..."
	@$(MAKE) mlflow-status
	@echo "5/7. Running KServe v2 mock canary routing verification (80/20 split)..."
	@$(MAKE) kserve-infer-test
	@echo "6/7. Running Kolmogorov-Smirnov & PSI Feature Drift Monitoring..."
	@$(MAKE) test-drift
	@echo "7/7. Running Phase 4 automated test suite..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_phase4_mlops.py -v > evidence/phase4/pytest_test_run.txt
	@$(PYTHON) -c "import json, glob; summary = {'phase': 4, 'status': 'COMPLETED', 'mode': '$(MODE)', 'artifacts': sorted(glob.glob('evidence/phase4/*'))}; json.dump(summary, open('evidence/phase4/phase4_summary.json', 'w'), indent=2); print('Phase 4 Complete:', json.dumps(summary, indent=2))"
	@echo "==> Phase 4 completed successfully. Evidence saved to evidence/phase4/"

phase5: ## Execute Phase 5: Quant Alpha Evaluation & Walk-Forward Backtesting (M1-37, M2-55)
	@echo "==> Executing Phase 5: Quant Alpha Backtester & Novel Ideas..."
	@mkdir -p evidence/phase5
	$(MAKE) quant-backtest
	@echo "==> Running mathematical and Zero Look-Ahead tests..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_quant_metrics.py tests/unit/test_alpha_synthesis.py tests/unit/test_backtester.py -v > evidence/phase5/pytest_test_run.txt
	@$(PYTHON) -c 'import json, glob, os; files = sorted([os.path.basename(p) for p in glob.glob("evidence/phase5/*")]); bt = json.load(open("evidence/phase5/backtest_summary.json")); json.dump({"phase": "phase5", "status": "PASSED", "rubrics_verified": ["M1-37", "M2-55"], "metrics_snapshot": bt["metrics"], "evidence_files": files}, open("evidence/phase5/phase5_summary.json", "w"), indent=2); print("==> Phase 5 Summary written to evidence/phase5/phase5_summary.json")'
	@echo "==> Phase 5 completed successfully. Evidence saved to evidence/phase5/"

phase6: ## Execute Phase 6: Real-time Paper Trading Simulation & Alpha Dashboard (M1-38)
	@echo "==> Executing Phase 6: Paper Trading Simulation & Market Replay..."
	@mkdir -p evidence/phase6
	$(MAKE) trader-replay SPEED=50
	@echo "==> Running Paper Trading & Idempotency unit tests..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_paper_trading.py -v > evidence/phase6/pytest_test_run.txt
	@$(PYTHON) -c 'import json, glob, os; files = sorted([os.path.basename(p) for p in glob.glob("evidence/phase6/*")]); ex = json.load(open("evidence/phase6/paper_trading_execution.json")); lat = json.load(open("evidence/phase6/paper_trading_latency.json")); json.dump({"phase": "phase6", "status": "PASSED", "rubrics_verified": ["M1-38"], "execution_snapshot": ex["execution"], "financials_snapshot": ex["financials"], "latency_p95_ms": lat["p95_ms"], "sla_met": lat["sla_met"], "evidence_files": files}, open("evidence/phase6/phase6_summary.json", "w"), indent=2); print("==> Phase 6 Summary written to evidence/phase6/phase6_summary.json")'
	@echo "==> Phase 6 completed successfully. Evidence saved to evidence/phase6/"

phase7: ## Execute Phase 7: Agentic AI, RAG Pipeline, MCP Tools & Multi-Agent Fleet (M2-01..M2-25)
	@echo "Notice: Target '$@' executes Phase 7: LLM, RAG & Multi-Agent Fleet."
	@echo "Evidence will be stored in evidence/phase7/."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-7-rag--llm--mcp--multi-agent."
	@exit 1

phase8 demo:
	@echo "Notice: Target '$@' is scheduled for subsequent implementation phases."
	@echo "Currently completed and verified milestones: Phase 1, Phase 2, Phase 3 & Phase 4."
	@echo "Refer to docs/plan/IMPLEMENTATION_STATUS.md for detailed status and evidence."
	@exit 1

phase:
	@echo "⚠️  Lỗi cú pháp: Vui lòng gõ 'make phase5' hoặc 'make phase6' (không có dấu cách giữa 'phase' và số)."

clean: ## Clean generated temporary files and output directory
	@echo "==> Cleaning generated output directory ($(OUT_DIR))..."
	@rm -rf $(OUT_DIR)

.PHONY: phase1 phase2 phase3 phase4 phase5 phase6 phase7 phase8 demo clean
