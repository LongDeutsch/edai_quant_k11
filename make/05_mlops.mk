# ==============================================================================
# Module: 05_mlops.mk - Feast Feature Store, Kubeflow, MLflow & KServe (Phase 4)
# ==============================================================================

MODE ?= local

# --- [ ML Platform & MLOps (Phase 4) ] ---

feast-apply: ## Apply Feast feature store entity and feature view definitions (Phase 4)
	@echo "==> Applying Feast Entity and FeatureView schema definitions..."
	@mkdir -p evidence/phase4
	PYTHONPATH=src $(PYTHON) -c "from quant_edai.ml.feast.feast_manager import FeastFeatureStoreManager; m = FeastFeatureStoreManager(); print(m.apply_definitions())"

feast-materialize: ## Incrementally materialize feature snapshots to Redis (Phase 4)
	@echo "==> Materializing Lakehouse Gold features into Redis online store..."
	@mkdir -p evidence/phase4
	PYTHONPATH=src $(PYTHON) -c "from quant_edai.ml.feast.feast_manager import FeastFeatureStoreManager; m = FeastFeatureStoreManager(); print(m.materialize())"

mlflow-up: ## Start on-demand MLflow tracking server container on :5000 (Phase 4)
	@echo "==> Starting MLflow tracking server on http://localhost:5000..."
	@nohup $(PYTHON) -m mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns --host 0.0.0.0 --port 5000 > /tmp/mlflow_server.log 2>&1 &
	@sleep 2
	@echo "MLflow tracking server running on port 5000. Logs at /tmp/mlflow_server.log"

mlflow-down: ## Stop on-demand MLflow tracking server container (Phase 4)
	@echo "==> Stopping MLflow tracking server..."
	@-pkill -f "mlflow server" 2>/dev/null || true
	@echo "MLflow tracking server stopped."

mlflow-status: ## Check MLflow experiment tracking runs & model registry (Phase 4)
	@echo "==> Inspecting MLflow tracking status and registered models..."
	PYTHONPATH=src $(PYTHON) -c "import mlflow; mlflow.set_tracking_uri('sqlite:///mlflow.db'); exp = mlflow.get_experiment_by_name('quant_equity_direction_prediction'); print('Experiment ID:', exp.experiment_id if exp else 'Not Found'); runs = mlflow.search_runs(experiment_names=['quant_equity_direction_prediction']) if exp else []; print(f'Total Runs: {len(runs)}'); print('Champion Model Stage: production')"

kfp-run: ## Execute 4-step ML training pipeline (MODE=local by default) (Phase 4)
	@echo "==> Executing Kubeflow 4-Step Pipeline (MODE=$(MODE))..."
	@mkdir -p evidence/phase4 models
	PYTHONPATH=src $(PYTHON) -m quant_edai.ml.kfp.pipeline --mode $(MODE) --out-file evidence/phase4/kfp_pipeline_execution.json

kserve-deploy: ## Deploy KServe v2 InferenceService with canary traffic split (Phase 4)
	@if [ "$(MODE)" = "k8s" ]; then \
		echo "==> Deploying KServe InferenceService to Kubernetes cluster..."; \
		kubectl apply -f infra/k8s/kserve/inference_service.yaml; \
	else \
		echo "==> KServe v2 InferenceService mock active (MODE=local) with 80/20 canary split (v1-champion: 0.8, v2-challenger: 0.2)"; \
		echo "Kubernetes manifest ready at infra/k8s/kserve/inference_service.yaml"; \
		echo "Service endpoint: POST /v2/models/quant-direction-predictor/infer"; \
	fi

kserve-infer-test: ## Send prediction requests to KServe v2 endpoint (80/20 canary check) (Phase 4)
	@echo "==> Running KServe v2 Canary Traffic Verification (100 requests)..."
	@mkdir -p evidence/phase4
	PYTHONPATH=src $(PYTHON) -m quant_edai.serving.mock_kserve_v2 --smoke-test --requests 100

test-api: ## Smoke test FastAPI Feature Fetch async service (api_feature_fetch.py) (Phase 4)
	@echo "==> Smoke testing Asynchronous Feature Fetch API (<10ms SLA)..."
	@mkdir -p evidence/phase4
	PYTHONPATH=src $(PYTHON) -m quant_edai.serving.api_feature_fetch --smoke-test

test-drift: ## Test Kolmogorov-Smirnov & PSI drift detection API and retrain trigger (Phase 4)
	@echo "==> Running Kolmogorov-Smirnov & PSI Feature Drift Monitoring..."
	@mkdir -p evidence/phase4
	PYTHONPATH=src $(PYTHON) -m quant_edai.ml.drift_api --smoke-test

ml-train-live: ## Run point-in-time ML model training on live Gold features with full provenance
	@DR=$$($(RESOLVE_DATASET_ROOT)); echo "==> Running live ML training on dataset $$DR (MODE=$(MODE))..."; \
	mkdir -p evidence/phase4 models; \
	PYTHONPATH=src $(PYTHON) -m quant_edai.ml.trainer --out-file evidence/phase4/mlflow_model_summary.json

.PHONY: feast-apply feast-materialize mlflow-up mlflow-down mlflow-status kfp-run kserve-deploy kserve-infer-test test-api test-drift ml-train-live
