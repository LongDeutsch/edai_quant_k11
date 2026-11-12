# ==============================================================================
# Module: 09_phases.mk - Milestone Phase Runners & Cleanup
# ==============================================================================

# --- [ Milestone Phase Runners ] ---

phase1: generate stream-test ## Execute Phase 1 end-to-end and gather evidence
	@echo "==> Executing Phase 1: Ingestion & Evidence Collection (MODE=$(MODE), EXECUTION_MODE=$(EXECUTION_MODE))..."
	@mkdir -p evidence/phase1
	$(PYTHON) -m quant_edai.ingestion.s3_uploader --source-dir $(OUT_DIR) --mode $(MODE)
	@cp -f $(OUT_DIR)/generation_stats.json evidence/phase1/data_generation_stats.json 2>/dev/null || true
	@$(PYTHON) -c "import json,os; from pathlib import Path; from quant_edai.common.dataset_paths import read_current_dataset_id; ds=read_current_dataset_id(os.environ['OUT_DIR']); s={'phase':1,'dataset_id':ds,'status':'PASSED_SIMULATED' if os.environ.get('MODE','smoke')=='smoke' else 'SUCCESS','execution_mode':os.environ.get('EXECUTION_MODE','SIMULATED')}; Path('evidence/phase1').mkdir(parents=True,exist_ok=True); json.dump(s, open(f'evidence/phase1/{ds or \"unknown\"}_phase1_summary.json','w'), indent=2); json.dump(s, open('evidence/phase1/phase1_summary.json','w'), indent=2); print(s)"
	@echo "==> Phase 1 completed. Evidence saved to evidence/phase1/"

phase2: ## Execute Phase 2: PySpark Skew, Iceberg Compaction & Flink Streaming (ENGINE=$(ENGINE))
	@echo "==> Executing Phase 2 (ENGINE=$(ENGINE), MODE=$(MODE))..."
	@mkdir -p evidence/phase2
	@DR=$$($(RESOLVE_DATASET_ROOT)); \
	test -d $$DR/raw_market_ohlcv || (echo "Generating input data first..." && $(MAKE) phase1); \
	DR=$$($(RESOLVE_DATASET_ROOT)); \
	echo "dataset_root=$$DR"; \
	echo "1/7. PySpark baseline..."; \
	$(PYTHON) -m quant_edai.processing.spark.baseline_job --source-dir $$DR --engine $(ENGINE) --out-file evidence/phase2/spark_baseline_metrics.json; \
	echo "2/7. PySpark skew-optimized..."; \
	$(PYTHON) -m quant_edai.processing.spark.skew_optimized --source-dir $$DR --engine $(ENGINE) --out-file evidence/phase2/spark_optimized_metrics.json; \
	$(PYTHON) -c "import json; b = json.load(open('evidence/phase2/spark_baseline_metrics.json')); o = json.load(open('evidence/phase2/spark_optimized_metrics.json')); bw = b.get('actual_wall_time_seconds', b.get('effective_stage_latency_seconds', 0)); ow = o.get('actual_wall_time_seconds', o.get('effective_stage_latency_seconds', 0)); comp = {'baseline_max_task_duration': b['max_task_duration_seconds'], 'optimized_max_task_duration': o['max_task_duration_seconds'], 'baseline_wall_time': bw, 'optimized_wall_time': ow, 'speedup': round(bw / max(1e-9, ow), 2), 'variance_reduction': f\"{round((1 - o['task_duration_variance']/max(0.0001, b['task_duration_variance'])) * 100, 2)}%\"}; json.dump(comp, open('evidence/phase2/spark_benchmark_comparison.json', 'w'), indent=2); print('Spark Benchmark:', json.dumps(comp, indent=2))"; \
	echo "3/7. Dedup & schema evolution..."; \
	$(PYTHON) -m quant_edai.processing.spark.dedup_job --source-dir $$DR --target-dir $$DR/stg_market_ohlcv --engine $(ENGINE) --out-file evidence/phase2/spark_dedup_evolution_metrics.json; \
	echo "3b/7. Resample Gold bars..."; \
	$(PYTHON) -m quant_edai.processing.spark.resample_job --source-dir $$DR/stg_market_ticks --gold-1m-dir $$DR/gold_market_bars_1m --gold-daily-dir $$DR/gold_market_bars_daily --engine $(ENGINE) --out-file evidence/phase2/spark_resample_metrics.json; \
	echo "4/7. Iceberg compaction..."; \
	$(PYTHON) -m quant_edai.processing.spark.iceberg_compaction --table-dir $$DR/raw_market_ohlcv --compacted-dir $$DR/compacted_market_ohlcv --engine $(ENGINE) --out-file evidence/phase2/iceberg_compaction_metrics.json; \
	echo "5/7. SCD2..."; \
	$(PYTHON) -m quant_edai.processing.spark.scd2_builder --source $$DR/dim_companies/dim_companies.parquet --target $$DR/gold_dim_companies --engine $(ENGINE) --out-file evidence/phase2/scd2_verification.json; \
	echo "6/7. Flink streaming..."; \
	$(PYTHON) -m quant_edai.processing.flink.streaming_job --input-file $$DR/streaming_ticks.jsonl --out-dir $$DR/flink_output --engine $(ENGINE) --out-file evidence/phase2/flink_streaming_audit.json; \
	echo "6b/7. Spark↔Flink OHLCV consistency..."; \
	$(PYTHON) -m quant_edai.processing.consistency.spark_flink_ohlcv --dataset-id $$(cat $(OUT_DIR)/CURRENT_DATASET_ID 2>/dev/null || echo phase2); \
	echo "7/7. Phase 2 unit tests..."; \
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_tick_generator.py tests/unit/test_tick_resample_and_silver.py tests/unit/test_tick_data_consistency.py tests/unit/test_spark_salting.py tests/unit/test_flink_streaming.py tests/unit/test_iceberg_scd2.py tests/unit/test_schema_registry_and_avro.py tests/unit/test_dlq_and_kafka.py -v > evidence/phase2/pytest_test_run.txt
	@$(PYTHON) -c "import json,os; from quant_edai.common.dataset_paths import read_current_dataset_id; ds=read_current_dataset_id(os.environ['OUT_DIR']); mode=os.environ.get('MODE','smoke'); st='PASSED_SIMULATED' if mode=='smoke' or os.environ.get('ENGINE','sim')=='sim' else 'SUCCESS'; s={'phase':2,'dataset_id':ds,'status':st,'execution_mode':os.environ.get('EXECUTION_MODE','SIMULATED')}; json.dump(s, open('evidence/phase2/phase2_summary.json','w'), indent=2); print(s)"
	@echo "==> Phase 2 completed. Evidence saved to evidence/phase2/"

phase3: ## Execute Phase 3: Orchestration (DP1/2/3), Great Expectations, Lineage & Serving (ClickHouse OLAP)
	@echo "==> Executing Phase 3 (ENGINE=$(ENGINE), MODE=$(MODE))..."
	@mkdir -p evidence/phase3
	@DR=$$($(RESOLVE_DATASET_ROOT)); test -d $$DR/raw_market_ohlcv || (echo "Generating input data first..." && $(MAKE) phase1)
	@echo "1/6. Triggering DP1 (wait terminal)..."
	@$(MAKE) airflow-trigger-dp1
	@echo "2/6. Triggering DP2 (wait terminal)..."
	@$(MAKE) airflow-trigger-dp2
	@echo "3/6. Triggering DP3 (wait terminal)..."
	@$(MAKE) airflow-trigger-dp3
	@echo "4/6. ClickHouse sync..."
	@$(MAKE) clickhouse-sync
	@echo "5/6. DataHub lineage..."
	@$(MAKE) datahub-lineage
	@echo "6/6. Phase 3 tests..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_phase3_quality_and_serving.py -v > evidence/phase3/pytest_test_run.txt
	@$(PYTHON) -c "import json, glob, os; from quant_edai.common.dataset_paths import read_current_dataset_id; mode=os.environ.get('MODE','smoke'); st='PASSED_SIMULATED' if mode=='smoke' else 'SUCCESS'; summary = {'phase': 3, 'status': st, 'execution_mode': os.environ.get('EXECUTION_MODE','SIMULATED'), 'dataset_id': read_current_dataset_id(os.environ['OUT_DIR']), 'artifacts': glob.glob('evidence/phase3/*')}; json.dump(summary, open('evidence/phase3/phase3_summary.json', 'w'), indent=2); print('Phase 3 Complete:', json.dumps(summary, indent=2))"
	@echo "==> Phase 3 completed. Evidence saved to evidence/phase3/"

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
	@if [ "$(MODE)" = "integration" ] || [ "$(MODE)" = "full" ]; then \
		echo "==> MODE=$(MODE): live Flink bars + ML signals (no synthetic replay)"; \
		$(MAKE) trader-run; \
	else \
		$(MAKE) trader-replay SPEED=50; \
	fi
	@echo "==> Running Paper Trading & Idempotency unit tests..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/unit/test_paper_trading.py -v > evidence/phase6/pytest_test_run.txt
	@$(PYTHON) -c 'import json, glob, os; files = sorted([os.path.basename(p) for p in glob.glob("evidence/phase6/*")]); ex = json.load(open("evidence/phase6/paper_trading_execution.json")); lat = json.load(open("evidence/phase6/paper_trading_latency.json")); json.dump({"phase": "phase6", "status": ex.get("status", "PASSED_SIMULATED"), "execution_mode": ex.get("execution_mode", "SIMULATED"), "dataset_id": ex.get("dataset_id"), "model_version": ex.get("model_version"), "feature_source": ex.get("feature_source"), "provenance": ex.get("provenance"), "rubrics_verified": ["M1-38"], "execution_snapshot": ex["execution"], "financials_snapshot": ex["financials"], "latency_p95_ms": lat["p95_ms"], "sla_met": lat["sla_met"], "evidence_files": files}, open("evidence/phase6/phase6_summary.json", "w"), indent=2); print("==> Phase 6 Summary written to evidence/phase6/phase6_summary.json")'
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

lakehouse-offline: ## Offline 3y daily→Spark Silver→SCD2→online tick replay Flink→DP3 Gold (no generator gold)
	@echo "==> Lakehouse offline sync pipeline (ENGINE=$(ENGINE)) -> $(OUT_DIR)/runs/..."
	@mkdir -p evidence/phase3
	@echo "1/6. generate-offline..."
	@$(MAKE) generate-offline
	@echo "2/6. generate-online-ticks + stream-test..."
	@$(MAKE) generate-online-ticks DATASET_ID=$$(cat $(OUT_DIR)/CURRENT_DATASET_ID)
	@$(MAKE) stream-test
	@DR=$$($(RESOLVE_DATASET_ROOT)); \
	echo "3/6. spark-dedup dataset_root=$$DR"; \
	$(PYTHON) -m quant_edai.processing.spark.dedup_job --source-dir $$DR --target-dir $$DR/stg_market_ohlcv --engine $(ENGINE); \
	echo "4/6. verify-scd2..."; \
	$(PYTHON) -m quant_edai.processing.spark.scd2_builder --source $$DR/dim_companies/dim_companies.parquet --target $$DR/gold_dim_companies --engine $(ENGINE); \
	echo "5/6. flink-optimized..."; \
	$(PYTHON) -m quant_edai.processing.flink.streaming_job --input-file $$DR/streaming_ticks.jsonl --out-dir $$DR/flink_output --engine $(ENGINE); \
	echo "6/6. DP3 feature calculator..."; \
	PYTHONPATH=src $(PYTHON) -m quant_edai.processing.features.feature_calculator \
		--source $$DR/stg_market_ohlcv \
		--target $$DR/gold_features/feat_ticker_technical_indicators.parquet \
		--engine $(ENGINE) \
		--out-file evidence/phase3/feature_computation_metrics.json; \
	$(PYTHON) -c "import json,os; from pathlib import Path; import pandas as pd; from quant_edai.common.dataset_paths import resolve_dataset_root, read_current_dataset_id; base=Path(os.environ.get('OUT_DIR','/tmp/lakehouse_source')); root=resolve_dataset_root(base); ds=read_current_dataset_id(base); bronze=root/'raw_market_ohlcv'; silver=root/'stg_market_ohlcv'; gold=root/'gold_features'/'feat_ticker_technical_indicators.parquet'; ticks=root/'raw_market_ticks_online'; gen=json.load(open(root/'generation_stats.json')) if (root/'generation_stats.json').exists() else {}; man=json.load(open(root/'online_tick_corpus_manifest.json')) if (root/'online_tick_corpus_manifest.json').exists() else {}; b=len(pd.read_parquet(bronze)) if bronze.exists() else 0; s=len(pd.read_parquet(silver)) if silver.exists() else 0; g=len(pd.read_parquet(gold)) if gold.exists() else 0; mode=os.environ.get('MODE','smoke'); st='PASSED_SIMULATED' if mode=='smoke' or os.environ.get('ENGINE','sim')=='sim' else 'SUCCESS'; out={'status':st,'execution_mode':os.environ.get('EXECUTION_MODE','SIMULATED'),'dataset_id':ds,'pipeline':'lakehouse-offline','span_years':gen.get('span_years'),'start_date':gen.get('start_date'),'end_date':gen.get('end_date'),'scale_mode':gen.get('scale_mode','tick_budget'),'ohlcv_volume_scaled_by_scale':gen.get('ohlcv_volume_scaled_by_scale',False),'volume_skew_ratio':gen.get('volume_skew_ratio'),'offline_duplicate_rate':gen.get('offline_duplicate_rate'),'schema_evolution_boundary':gen.get('schema_evolution_boundary','2024-06-01'),'regime_drift_boundary':gen.get('regime_drift_boundary','2024-08-01'),'online_tick_rows':man.get('total_ticks'),'online_start':man.get('start_date'),'online_end':man.get('end_date'),'bronze_rows':b,'silver_rows':s,'gold_feature_rows':g,'gold_features_from_generator':False,'paths':{'bronze':str(bronze),'silver':str(silver),'gold_features':str(gold),'online_ticks':str(ticks),'streaming_ticks':str(root/'streaming_ticks.jsonl'),'flink_output':str(root/'flink_output')}}; Path('evidence/phase3').mkdir(parents=True,exist_ok=True); json.dump(out, open('evidence/phase3/lakehouse_offline_sync.json','w'), indent=2); json.dump(out, open('evidence/phase3/lakehouse_3y_sync.json','w'), indent=2); print(json.dumps(out, indent=2))"
	@echo "==> lakehouse-offline complete. Evidence: evidence/phase3/lakehouse_offline_sync.json"

lakehouse-3y: lakehouse-offline ## Alias for lakehouse-offline (offline 3y daily + online tick replay)

benchmark-pipeline: ## P2 scale benchmark → evidence/benchmark/{dataset_id}_report.json
	@echo "==> benchmark-pipeline MODE=$(MODE) SCALE=$(SCALE) OUT_DIR=$(OUT_DIR)"
	@mkdir -p evidence/benchmark
	$(PYTHON) -m quant_edai.benchmark.pipeline_benchmark --out-dir $(OUT_DIR) --scale $(SCALE) --mode $(MODE) --seed $(SEED) $(if $(DATASET_ID),--dataset-id $(DATASET_ID),)

clean: ## Clean generated temporary files and output directory
	@echo "==> Cleaning generated output directory ($(OUT_DIR))..."
	@rm -rf $(OUT_DIR)

.PHONY: phase1 phase2 phase3 phase4 phase5 phase6 phase7 phase8 demo lakehouse-offline lakehouse-3y benchmark-pipeline clean
