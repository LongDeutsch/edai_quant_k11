# ==============================================================================
# Module: 08_test.mk - Automated Testing & Rubric Verification
# ==============================================================================

# --- [ Verification & Testing ] ---

test: test-unit ## Run primary automated test suite

test-unit: ## Run fast Pytest unit tests with mock fixtures
	@echo "==> Running Pytest Unit Tests..."
	$(PYTHON) -m pytest tests/unit -v

test-p0: ## Run P0 hardening tests (dataset isolation, execution status)
	@echo "==> Running P0 tests..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/integration -m p0 -v

test-p1: ## Run P1 hardening tests (ClickHouse, ML provenance, trader pipeline, Spark/Flink consistency)
	@echo "==> Running P1 tests..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/integration -m p1 -v

test-p2: ## Run P2 benchmark smoke test
	@echo "==> Running P2 tests..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/integration -m p2 -v

test-live-e2e: ## Run comprehensive live closed-loop E2E test suite across all 8 steps
	@echo "==> Running Comprehensive Live Closed-Loop E2E Integration Suite..."
	PYTHONPATH=src $(PYTHON) -m pytest tests/live -v

consistency-spark-flink: ## Spark-vs-Flink OHLCV consistency harness (SIMULATED) -> evidence/consistency/{dataset_id}/
	PYTHONPATH=src $(PYTHON) -m quant_edai.processing.consistency.spark_flink_ohlcv --dataset-id $${DATASET_ID:-synthetic_consistency}

test-property: ## Run Hypothesis property-based idempotency tests (Phase 8)
	@echo "Notice: Target '$@' is scheduled for Phase 8 verification suite."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-8 for technical design."
	@exit 1

test-mutation: ## Run Mutmut mutation testing on changed files (Phase 8)
	@echo "Notice: Target '$@' is scheduled for Phase 8 verification suite."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-8 for technical design."
	@exit 1

benchmark-load: ## Run Locust load test measuring throughput and SLA (Phase 8)
	@echo "Notice: Target '$@' is scheduled for Phase 8 verification suite."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-8 for technical design."
	@exit 1

verify-rubric: ## Check rubric scorecard status in docs/plan/RUBRIC_TRACKER.md
	@echo "==> Rubric Tracker Status:"
	@head -n 28 docs/plan/RUBRIC_TRACKER.md

.PHONY: test-p0 test-p1 test-p2 consistency-spark-flink test test-unit test-property test-mutation benchmark-load verify-rubric
