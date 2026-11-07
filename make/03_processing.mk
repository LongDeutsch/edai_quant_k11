# ==============================================================================
# Module: 03_processing.mk - PySpark Batch & Apache Flink Streaming (Phase 2)
# ==============================================================================

# --- [ Stream & Batch Processing ] ---

spark-skew-baseline: ## Run PySpark skew baseline job (Phase 2)
	@echo "Notice: Target '$@' is scheduled for Phase 2."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-2 for technical design."
	@exit 1

spark-skew-optimized: ## Run PySpark skew optimization with salting & AQE (Phase 2)
	@echo "Notice: Target '$@' is scheduled for Phase 2."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-2 for technical design."
	@exit 1

flink-stream-dedup: ## Run Apache Flink RocksDB streaming deduplication job (Phase 2)
	@echo "Notice: Target '$@' is scheduled for Phase 2."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-2 for technical design."
	@exit 1

.PHONY: spark-skew-baseline spark-skew-optimized flink-stream-dedup
