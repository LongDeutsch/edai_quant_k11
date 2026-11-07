# ==============================================================================
# Module: 08_test.mk - Automated Testing & Rubric Verification
# ==============================================================================

# --- [ Verification & Testing ] ---

test: test-unit ## Run primary automated test suite

test-unit: ## Run fast Pytest unit tests with mock fixtures
	@echo "==> Running Pytest Unit Tests..."
	$(PYTHON) -m pytest tests/unit -v

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

.PHONY: test test-unit test-property test-mutation benchmark-load verify-rubric
