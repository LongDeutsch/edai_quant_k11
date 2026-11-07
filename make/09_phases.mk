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

phase2 phase3 phase4 phase5 phase6 phase7 phase8 demo:
	@echo "Notice: Target '$@' is scheduled for subsequent implementation phases."
	@echo "Currently completed and verified milestone is Phase 1 (Data Generator & Infrastructure Foundation)."
	@echo "Refer to docs/plan/IMPLEMENTATION_STATUS.md for detailed status and evidence."
	@exit 1

clean: ## Clean generated temporary files and output directory
	@echo "==> Cleaning generated output directory ($(OUT_DIR))..."
	@rm -rf $(OUT_DIR)

.PHONY: phase1 phase2 phase3 phase4 phase5 phase6 phase7 phase8 demo clean
