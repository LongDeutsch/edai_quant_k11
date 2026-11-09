# ==============================================================================
# Module: 04_orchestration.mk - Apache Airflow & DataHub Governance (Phase 3)
# ==============================================================================

# --- [ Orchestration & Governance ] ---

verify-schema-registry: ## Verify Confluent Schema Registry v1/v2 compatibility & breaking changes (HTTP 409)
	@echo "==> Verifying Schema Registry compatibility..."
	$(PYTHON) -m quant_edai.governance.verify_schema_evolution --registry-url $(SCHEMA_REGISTRY_URL)

spike-iceberg-rest: ## Spike Apache Iceberg REST Catalog endpoint and warehouse configuration
	@echo "==> Spiking Iceberg REST Catalog..."
	$(PYTHON) -m quant_edai.governance.spike_iceberg_rest --catalog-url $(ICEBERG_REST_URL)

sync-schema-iceberg: ## Promote Avro schema evolution to Iceberg lakehouse tables unidirectionally
	@echo "==> Synchronizing Avro schema evolution to Iceberg..."
	$(PYTHON) -m quant_edai.governance.schema_sync --source-dir $(OUT_DIR) --engine $(ENGINE)

test-governance: verify-schema-registry spike-iceberg-rest sync-schema-iceberg ## Run all schema governance & lakehouse pre-flight checks
	@echo "==> Governance pre-flight checks completed successfully."

airflow-up: ## Start Apache Airflow orchestrator, ClickHouse, and Pinot serving layers
	@echo "==> Starting Airflow, ClickHouse, and Pinot services..."
	$(COMPOSE) up -d clickhouse pinot airflow-init airflow-webserver airflow-scheduler
	@echo "Airflow Web UI: http://localhost:8080 (admin/admin)"
	@echo "ClickHouse HTTP: http://localhost:8123"
	@echo "Pinot Controller: http://localhost:9002 | Broker: http://localhost:8099"

airflow-down: ## Stop Airflow, ClickHouse, and Pinot containers
	@echo "==> Stopping Airflow, ClickHouse, and Pinot services..."
	$(COMPOSE) stop airflow-webserver airflow-scheduler clickhouse pinot

airflow-trigger-dp1: ## Trigger Airflow DAG DP1 (Raw Ingestion & Great Expectations)
	@echo "==> Triggering Airflow DAG DP1: Raw Ingestion & Quality Gate..."
	$(PYTHON) -m quant_edai.orchestration.airflow_trigger --dag dag_dp1_raw_ingestion --engine $(ENGINE)

airflow-trigger-dp2: ## Trigger Airflow DAG DP2 (Silver & Gold Lakehouse Processing)
	@echo "==> Triggering Airflow DAG DP2: Silver/Gold Processing & Compaction..."
	$(PYTHON) -m quant_edai.orchestration.airflow_trigger --dag dag_dp2_silver_gold --engine $(ENGINE)

airflow-trigger-dp3: ## Trigger Airflow DAG DP3 (Technical Features, Lineage & Serving)
	@echo "==> Triggering Airflow DAG DP3: Quant Features & Lineage..."
	$(PYTHON) -m quant_edai.orchestration.airflow_trigger --dag dag_dp3_features --engine $(ENGINE)

clickhouse-sync: ## Synchronize Gold tables to ClickHouse ReplacingMergeTree tables
	@echo "==> Synchronizing Lakehouse Gold datasets to ClickHouse OLAP..."
	$(PYTHON) -m quant_edai.serving.clickhouse_sync --source-dir $(OUT_DIR) --engine $(ENGINE)

pinot-setup: ## Register realtime Kafka stream table in Apache Pinot
	@echo "==> Administering Apache Pinot schema and realtime table..."
	$(PYTHON) -m quant_edai.serving.pinot_admin --engine $(ENGINE)

datahub-lineage: ## View and verify DataHub pipeline lineage
	@echo "==> Emitting and verifying DataHub metadata and lineage..."
	$(PYTHON) -m quant_edai.quality.datahub_emitter --out-json evidence/phase3/lineage_proof.json --out-md evidence/phase3/lineage_proof.md

.PHONY: verify-schema-registry spike-iceberg-rest sync-schema-iceberg test-governance airflow-up airflow-down airflow-trigger-dp1 airflow-trigger-dp2 airflow-trigger-dp3 clickhouse-sync pinot-setup datahub-lineage
