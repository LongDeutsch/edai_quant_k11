# EDAI K11 Rubric Tracker & Verification Scorecard

> **Target Standard**: EDAI K11 Master Rubric (`rubic/EDAI K11.xlsx`)  
> **Evaluation Breakdown**: Module 1 (`edai-1`: 50%) + Module 2 / Final (`edai-2` / `final`: 50%) = 100% Total Grade  
> **Status Policy**: Strict Evidence-Based Tracking. **No criterion is marked achieved without verifiable code, logs, and screenshots stored in `evidence/`.**

---

## 1. Overall Scorecard Summary

| Module | Scope | Total Possible Points | Currently Implemented Points | Currently Verified Points | Current Grade (%) | Target Grade (%) |
|---|---|---|---|---|---|---|
| **Module 1 (`edai-1`)** | Data Platform, Spark, Flink, Iceberg, Airflow, DataHub, Novel Ideas | 100.0 pts | 17.0 pts | 17.0 pts | 17.0% (8.5 / 50.0) | 100.0% (50.0 / 50.0) |
| **Module 2 (`edai-2` / `final`)** | ML Platform, Feast, Kubeflow, KServe, RAG, Multi-Agent, Observability, Verification | 100.0 pts | 0.0 pts | 0.0 pts | 0.0% (0.0 / 50.0) | 100.0% (50.0 / 50.0) |
| **Overall Combined** | **End-to-End Quant AI Engineering Platform** | **200.0 pts** | **17.0 pts** | **17.0 pts** | **8.5% (8.5 / 100)** | **100.0% (100 / 100)** |

---

## 2. Module 1: Data Platform Scorecard (`edai-1 (50%)`)

*Evaluates big data infrastructure, anomaly injection, batch/stream processing optimization, lakehouse storage, orchestration, and data governance.*

| ID | Rubric Category | Specific Criterion | Pts | Phase | Implementation File(s) | Verification Command | Required Proof / Deliverable | Status | Verified Pts |
|---|---|---|---|---|---|---|---|---|---|
| **M1-01** | Fundamentals | Docker & Docker Compose setup | 2.0 | Phase 1 | `infra/docker-compose.yml` | `make up && make status` | Config & service specs verified (`evidence/phase1/docker_compose_config.txt`) | **Verified** | **2.0 / 2.0** |
| **M1-02** | Fundamentals | Multi-stage Dockerfile optimization | 3.0 | Phase 1 | `infra/docker/Dockerfile.generator` vs `.base` | `make benchmark-images` | Multi-stage build analysis: builder isolation, non-root user (`evidence/phase1/docker_image_size.txt`) | **Verified** | **3.0 / 3.0** |
| **M1-03** | Data Generator | Simulate volume skew (80% in 7 tickers) | 2.0 | Phase 1 | `src/quant_edai/generator/data_generator.py` | `make generate SCALE=0.1` | Volume skew measured at **80.09%** in 7 Mega-Caps (`evidence/phase1/data_generation_stats.json`) | **Verified** | **2.0 / 2.0** |
| **M1-04** | Data Generator | Simulate schema evolution (mid-stream fields) | 2.0 | Phase 1 | `src/quant_edai/generator/faults_injector.py` | `make generate SCALE=0.1` | Schema evolution verified: 1,772 rows post-2024-06-01 populated with `vwap` & `trade_count` | **Verified** | **2.0 / 2.0** |
| **M1-05** | Data Generator | Simulate offline duplicates (~2.0% retry dups) | 2.0 | Phase 1 | `src/quant_edai/generator/faults_injector.py` | `make generate SCALE=0.1` | Offline duplicate rate measured at **1.98%** (~2.0%) (`evidence/phase1/data_generation_stats.json`) | **Verified** | **2.0 / 2.0** |
| **M1-06** | Data Generator | Store data post-generation (MinIO / S3) | 2.0 | Phase 1 | `src/quant_edai/ingestion/s3_uploader.py` | `make phase1` | Staged 876 partitioned Parquet/JSON files (6.7MB at SCALE=0.1) in `/tmp/lakehouse_source` | **Verified** | **2.0 / 2.0** |
| **M1-07** | Data Generator | Simulate online late arrivals (5-10m delays) | 2.0 | Phase 1 | `src/quant_edai/generator/streaming_producer.py` | `make stream-test` | Late arrival rate measured at **5.02%** with 300–600s delay (`streaming_ticks.jsonl`) | **Verified** | **2.0 / 2.0** |
| **M1-08** | Data Generator | Simulate streaming duplicates (~1.5% at-least-once)| 2.0 | Phase 1 | `src/quant_edai/generator/streaming_producer.py` | `make stream-test` | Streaming duplicate rate measured at **1.48%** (~1.5%) (`streaming_ticks.jsonl`) | **Verified** | **2.0 / 2.0** |
| **M1-09** | Batch Spark | Baseline unoptimized Spark job | 2.0 | Phase 2 | `src/quant_edai/processing/spark/baseline_job.py`| `make spark-baseline` | Spark UI screenshot showing executor straggler | Unverified | 0.0 / 2.0 |
| **M1-10** | Batch Spark | Handle skew with salting & AQE explanation | 3.0 | Phase 2 | `src/quant_edai/processing/spark/skew_optimized.py`| `make spark-optimized` | Spark UI screenshot showing balanced partitions + 12.2x speedup | Unverified | 0.0 / 3.0 |
| **M1-11** | Batch Spark | Handle schema evolution with explanation | 3.0 | Phase 2 | `src/quant_edai/processing/spark/iceberg_merge.py` | `make spark-evolution` | Query output demonstrating unified scan over evolved partitions | Unverified | 0.0 / 3.0 |
| **M1-12** | Batch Spark | Handle offline duplicates with explanation | 3.0 | Phase 2 | `src/quant_edai/processing/spark/dedup_job.py` | `make spark-dedup` | Row count validation log before vs after deduplication | Unverified | 0.0 / 3.0 |
| **M1-13** | Batch Spark | Spark jobs integrated into Airflow | 2.0 | Phase 3 | `orchestration/dags/dag_dp2_silver_gold.py` | `make airflow-trigger-dp2` | Airflow DAG tree screenshot with green Spark tasks | Unverified | 0.0 / 2.0 |
| **M1-14** | Streaming Flink| Baseline unoptimized Flink job | 2.0 | Phase 2 | `src/quant_edai/processing/flink/baseline_stream.py`| `make flink-baseline` | Flink UI showing late data dropped/watermark lag | Unverified | 0.0 / 2.0 |
| **M1-15** | Streaming Flink| Handle late arrival via Watermarks | 3.0 | Phase 2 | `src/quant_edai/processing/flink/streaming_job.py` | `make flink-optimized` | Flink UI screenshot & log showing late data recovered | Unverified | 0.0 / 3.0 |
| **M1-16** | Streaming Flink| Handle streaming duplicates via RocksDB state | 3.0 | Phase 2 | `src/quant_edai/processing/flink/streaming_dedup.py`| `make flink-dedup` | Count comparison between Kafka input and Pinot sink | Unverified | 0.0 / 3.0 |
| **M1-17** | Streaming Flink| Tumbling & Sliding Window processing | 2.0 | Phase 2 | `src/quant_edai/processing/flink/window_agg.py` | `make flink-window` | Code explanation & Pinot query output of 1m/5m bars | Unverified | 0.0 / 2.0 |
| **M1-18** | Storage Opt | Lakehouse compaction (`rewrite_data_files`) | 3.0 | Phase 2 | `src/quant_edai/processing/spark/compaction.py` | `make iceberg-compaction`| File count reduction report (e.g. 84% reduction) | Unverified | 0.0 / 3.0 |
| **M1-19** | Storage Opt | Data Warehouse optimization (ClickHouse index) | 3.0 | Phase 3 | `sql/clickhouse/schema_quant.sql` | `make clickhouse-benchmark`| Query execution time comparison with minmax/set indexes | Unverified | 0.0 / 3.0 |
| **M1-20** | Orchestration | DP1: Raw ingestion stage to Bronze | 2.0 | Phase 3 | `orchestration/dags/dag_dp1_raw_ingestion.py` | `make airflow-trigger-dp1` | Airflow UI task execution screenshot | Unverified | 0.0 / 2.0 |
| **M1-21** | Orchestration | DP1: Great Expectations validation stage | 2.0 | Phase 3 | `configs/quality/suite_raw_market_data.json` | `make airflow-trigger-dp1` | Great Expectations Data Docs HTML report | Unverified | 0.0 / 2.0 |
| **M1-22** | Orchestration | DP2: Bronze to Silver & Gold stage | 2.0 | Phase 3 | `orchestration/dags/dag_dp2_silver_gold.py` | `make airflow-trigger-dp2` | Airflow UI task execution screenshot | Unverified | 0.0 / 2.0 |
| **M1-23** | Orchestration | DP2: Silver/Gold validation stage | 2.0 | Phase 3 | `configs/quality/suite_gold_market_data.json` | `make airflow-trigger-dp2` | Data Docs validation report | Unverified | 0.0 / 2.0 |
| **M1-24** | Orchestration | DP3: Offline technical features compute stage | 2.0 | Phase 3 | `orchestration/dags/dag_dp3_features.py` | `make airflow-trigger-dp3` | Airflow UI task execution screenshot | Unverified | 0.0 / 2.0 |
| **M1-25** | Orchestration | DP3: Feature validation stage | 2.0 | Phase 3 | `configs/quality/suite_features.json` | `make airflow-trigger-dp3` | Data Docs validation report | Unverified | 0.0 / 2.0 |
| **M1-26** | Governance | DataHub DP1 lineage & table linkages | 2.0 | Phase 3 | DataHub Airflow lineage hook | `make datahub-view` | DataHub UI screenshot of DP1 $\to$ Bronze lineage | Unverified | 0.0 / 2.0 |
| **M1-27** | Governance | DataHub DP1 data validation metadata | 2.0 | Phase 3 | Great Expectations DataHub action | `make datahub-view` | DataHub UI screenshot showing dataset assertions | Unverified | 0.0 / 2.0 |
| **M1-28** | Governance | DataHub DP2 lineage & table linkages | 2.0 | Phase 3 | DataHub Airflow lineage hook | `make datahub-view` | DataHub UI screenshot of Bronze $\to$ Silver $\to$ Gold | Unverified | 0.0 / 2.0 |
| **M1-29** | Governance | DataHub DP2 data validation metadata | 2.0 | Phase 3 | Great Expectations DataHub action | `make datahub-view` | DataHub UI screenshot showing Gold assertions | Unverified | 0.0 / 2.0 |
| **M1-30** | Governance | DataHub DP3 lineage & table linkages | 2.0 | Phase 3 | DataHub Airflow lineage hook | `make datahub-view` | DataHub UI screenshot of Gold $\to$ Feature tables | Unverified | 0.0 / 2.0 |
| **M1-31** | Governance | DataHub DP3 data validation metadata | 2.0 | Phase 3 | Great Expectations DataHub action | `make datahub-view` | DataHub UI screenshot showing Feature assertions | Unverified | 0.0 / 2.0 |
| **M1-32** | Schema Design | Multi-zone schema visualization | 2.0 | Phase 3 | `docs/STORAGE_AND_LAKEHOUSE.md` | `cat docs/STORAGE_AND_LAKEHOUSE.md`| ER diagram and schema DDLs across all zones | Unverified | 0.0 / 2.0 |
| **M1-33** | Schema Design | Dimension table with SCD Type 2 | 2.0 | Phase 2 | `src/quant_edai/processing/spark/scd2_builder.py`| `make verify-scd2` | Iceberg query output showing `valid_from_ts`, `valid_to_ts`, `is_current` | Unverified | 0.0 / 2.0 |
| **M1-34** | Schema Design | Feature tables with `event_timestamp` & `created`| 2.0 | Phase 2 | `sql/iceberg/feat_ticker_technical_indicators.sql`| `make verify-schema`| DDL and sample query showing required timestamp columns | Unverified | 0.0 / 2.0 |
| **M1-35** | Schema Design | Dim & Fact relational foreign key integrity | 2.0 | Phase 3 | DBeaver schema export / Mermaid ER | `make verify-schema`| DBeaver/ER diagram showing Dim-Fact relationships | Unverified | 0.0 / 2.0 |
| **M1-36** | Schema Design | Naming conventions (`dim_`, `fact_`, `feat_`, `raw_`)| 2.0 | Phase 2 | All DDLs and table names | `make verify-schema`| Catalog inspection proving consistent prefixes | Unverified | 0.0 / 2.0 |
| **M1-37** | Novel Idea 1 | Quant Alpha Factor Research Backtester | 10.0 | Phase 6 | `src/quant_edai/quant/backtester.py` | `make phase6` | Tear sheet report (IC, Rank IC, Sharpe, Drawdown vs SPY) | Unverified | 0.0 / 10.0 |
| **M1-38** | Novel Idea 2 | Real-time Paper Trading Simulator | 10.0 | Phase 7 | `src/quant_edai/quant/paper_trading.py`| `make phase7` | Stream replay, atomic accounting logs, and latency proof | Unverified | 0.0 / 10.0 |
| **TOTAL** | **Module 1** | **Total Rubric Points** | **100.0**| | | | | **0.0 / 100.0** | **0.0%** |

---

## 3. Module 2 / Final: ML, LLM & Multi-Agent Scorecard (`edai-2 (50%)`)

*Evaluates Feast feature store, ML training pipelines, model serving, drift detection, LLM inference, MCP microservices, multi-agent fleet, testing rigor, observability, and security.*

| ID | Rubric Category | Specific Criterion | Pts | Phase | Implementation File(s) | Verification Command | Required Proof / Deliverable | Status | Verified Pts |
|---|---|---|---|---|---|---|---|---|---|
| **M2-01** | LLM Platform | Deploy LLM inference platform (vLLM/LiteLLM) | 2.0 | Phase 5 | `infra/docker/litellm.yml` / Mock LLM | `make llm-status` | Benchmark throughput & TTFT results report | Unverified | 0.0 / 2.0 |
| **M2-02** | LLM Platform | Setup custom LLM model (Qwen 2.5 / Llama 3.3) | 2.0 | Phase 5 | `configs/llm/model_config.yaml` | `make llm-infer-test` | Model loading log & sample response | Unverified | 0.0 / 2.0 |
| **M2-03** | LLM Platform | Setup AgentGateway with security & routing policy | 1.0 | Phase 5 | `src/quant_edai/llm/gateway/gateway_service.py`| `make gateway-status` | Gateway routing policy config & curl trace | Unverified | 0.0 / 1.0 |
| **M2-04** | LLM Platform | Benchmark model server & optimize throughput | 2.0 | Phase 5 | `tests/performance/llm_benchmark.py` | `make benchmark-llm` | Throughput (tokens/s) and latency report | Unverified | 0.0 / 2.0 |
| **M2-05** | Global Config | Deploy 1 global model config for all agents | 2.0 | Phase 5 | `configs/llm/global_agent_model.yaml` | `make verify-phase5` | Config file link & runtime inspection output | Unverified | 0.0 / 2.0 |
| **M2-06** | Agent Registry | Deploy agent registry (Langfuse / MLflow) | 2.0 | Phase 5 | `src/quant_edai/llm/registry/` | `make registry-status`| UI screenshot showing registered agent entities | Unverified | 0.0 / 2.0 |
| **M2-07** | RAG Pipeline | Airflow RAG pipeline (extract $\to$ chunk $\to$ embed $\to$ Qdrant)| 2.0 | Phase 5 | `orchestration/dags/dag_rag_financial_news.py`| `make airflow-trigger-rag` | Airflow UI screenshot of completed RAG DAG | Unverified | 0.0 / 2.0 |
| **M2-08** | RAG Pipeline | Data governance & lineage on DataHub for RAG | 2.0 | Phase 5 | DataHub RAG lineage emitter | `make datahub-view` | DataHub UI screenshot of RAG lineage | Unverified | 0.0 / 2.0 |
| **M2-09** | Feature Store | Airflow incremental materialize (Iceberg $\to$ Redis) | 2.0 | Phase 4 | `orchestration/dags/dag_feast_materialize.py` | `make feast-materialize` | Airflow UI screenshot + Redis key inspection | Unverified | 0.0 / 2.0 |
| **M2-10** | Feature Store | Deploy jobs to push streaming features to Feast | 2.0 | Phase 4 | `src/quant_edai/processing/flink/feast_sink.py` | `make flink-feast-push` | Flink job running screenshot + Redis feature keys | Unverified | 0.0 / 2.0 |
| **M2-11** | ML Pipelines | Kubeflow 4-step training pipeline | 2.0 | Phase 4 | `src/quant_edai/ml/kfp/pipeline.py` | `make kfp-run` | Kubeflow UI screenshot showing completed 4 steps | Unverified | 0.0 / 2.0 |
| **M2-12** | ML Pipelines | MLflow tracking & auto-tagging `production` | 2.0 | Phase 4 | `src/quant_edai/ml/trainer.py` | `make mlflow-status` | MLflow UI screenshot with model version & `production` tag | Unverified | 0.0 / 2.0 |
| **M2-13** | Model Serving | KServe v2 model inference service deployment | 2.0 | Phase 4 | `infra/k8s/kserve/inference_service.yaml` | `make kserve-infer-test` | KServe pod status & inference response payload | Unverified | 0.0 / 2.0 |
| **M2-14** | Data Web API | FastAPI + Pydantic validation + healthcheck | 1.0 | Phase 4 | `src/quant_edai/serving/api_feature_fetch.py` | `make test-api` | OpenAPI `/docs` screenshot & healthcheck response | Unverified | 0.0 / 1.0 |
| **M2-15** | Data Web API | Async concurrent processing implementation | 1.0 | Phase 4 | `src/quant_edai/serving/api_feature_fetch.py` | `make test-api` | Code verification & async concurrency load test log | Unverified | 0.0 / 1.0 |
| **M2-16** | Data Web API | Deploy as MCP tool via Helm (`--atomic`) | 2.0 | Phase 5 | `infra/helm/mcp-features/` | `make helm-deploy-mcp` | Helm upgrade command output with `--atomic` flag | Unverified | 0.0 / 2.0 |
| **M2-17** | Agent 1 | Deploy Agent 1 using MCP tool (Technical Quant) | 2.0 | Phase 5 | `src/quant_edai/llm/agents/tech_quant_agent.py` | `make test-agent1` | Agent execution trace showing MCP tool invocation | Unverified | 0.0 / 2.0 |
| **M2-18** | Agent 1 | Configure Agent 1 in Sandbox for safety | 1.0 | Phase 5 | `infra/k8s/agents/security_context.yaml` | `make test-agent1` | SecurityContext & network policy configuration | Unverified | 0.0 / 1.0 |
| **M2-19** | Agent 1 | Publish Agent 1 to registry | 1.0 | Phase 5 | Agent registry registration hook | `make registry-status`| UI screenshot of Agent 1 in registry | Unverified | 0.0 / 1.0 |
| **M2-20** | Agent 2 | Deploy Agent 2 (Sentiment News) + Sandbox + Registry | 8.0 | Phase 5 | `src/quant_edai/llm/agents/sentiment_news_agent.py`| `make test-agent2` | Agent 2 trace, sandbox manifest, and registry UI | Unverified | 0.0 / 8.0 |
| **M2-21** | MCP Gateway | AgentGateway as MCP Gateway | 2.0 | Phase 5 | `src/quant_edai/llm/gateway/mcp_router.py` | `make test-gateway-mcp` | Trace showing gateway routing request to MCP server | Unverified | 0.0 / 2.0 |
| **M2-22** | A2A Gateway | AgentGateway as Agent-to-Agent (A2A) Gateway | 2.0 | Phase 5 | `src/quant_edai/llm/gateway/a2a_router.py` | `make test-gateway-a2a` | Trace showing A2A inter-agent message dispatch | Unverified | 0.0 / 2.0 |
| **M2-23** | Coordinator | Deploy Coordinator Agent with multi-reply | 2.0 | Phase 5 | `src/quant_edai/llm/agents/coordinator_agent.py`| `make test-coordinator` | Interaction log showing synthesis of Agent 1 & Agent 2 | Unverified | 0.0 / 2.0 |
| **M2-24** | Coordinator | Publish Coordinator Agent to registry | 2.0 | Phase 5 | Agent registry registration hook | `make registry-status`| UI screenshot of Coordinator in registry | Unverified | 0.0 / 2.0 |
| **M2-25** | System Warmup | Warm standby mode configuration | 2.0 | Phase 5 | `src/quant_edai/llm/warmup.py` | `make warmup-check` | System logs showing pre-loaded weights & connections | Unverified | 0.0 / 2.0 |
| **M2-26** | Verification | Pytest Unit Tests with Coverage $> 90\%$ | 1.0 | Phase 8 | `tests/unit/` | `make test-unit` | Pytest coverage HTML/terminal report ($>90\%$) | Unverified | 0.0 / 1.0 |
| **M2-27** | Verification | Equivalence Partitioning & Boundary Value Analysis | 2.0 | Phase 8 | `tests/unit/test_ep_bva.py` | `make test-unit` | Parameterized test file & pytest output | Unverified | 0.0 / 2.0 |
| **M2-28** | Verification | Mutation testing score $> 80\%$ via `mutmut` | 2.0 | Phase 8 | `mutmut_config.py` | `make test-mutation` | Mutmut summary report showing $>80\%$ killed mutants | Unverified | 0.0 / 2.0 |
| **M2-29** | Verification | Idempotency testing via `hypothesis` | 2.0 | Phase 8 | `tests/unit/test_idempotency.py` | `make test-property` | Hypothesis test execution report proving idempotence | Unverified | 0.0 / 2.0 |
| **M2-30** | Verification | Locust Web API load test SLA HTML report | 2.0 | Phase 8 | `tests/performance/locustfile.py` | `make benchmark-load` | Locust HTML report showing $>300$ req/s & $p95 < 30$ms | Unverified | 0.0 / 2.0 |
| **M2-31** | CI/CD | CI/CD for RAG Data Pipeline | 2.0 | Phase 8 | `.github/workflows/ci_rag.yml` / Jenkinsfile | `make verify-ci` | CI workflow run log & green status badge | Unverified | 0.0 / 2.0 |
| **M2-32** | CI/CD | CI/CD for Data Web API MCP Server & Agent | 2.0 | Phase 8 | `.github/workflows/ci_mcp.yml` | `make verify-ci` | CI build, package, and deploy pipeline run log | Unverified | 0.0 / 2.0 |
| **M2-33** | CI/CD | CI/CD for Coordinator Agent | 2.0 | Phase 8 | `.github/workflows/ci_coordinator.yml` | `make verify-ci` | CI workflow run log | Unverified | 0.0 / 2.0 |
| **M2-34** | CI/CD | CI/CD for Streaming Feature Jobs | 2.0 | Phase 8 | `.github/workflows/ci_streaming.yml` | `make verify-ci` | CI build and test pipeline run log | Unverified | 0.0 / 2.0 |
| **M2-35** | Ingress/Gateway| Hide Grafana behind NGINX Ingress Controller | 2.0 | Phase 8 | `infra/k8s/ingress/nginx_routes.yaml` | `make verify-ingress` | NGINX routing config & curl verification | Unverified | 0.0 / 2.0 |
| **M2-36** | Ingress/Gateway| Hide Kibana / Logs behind NGINX Ingress | 2.0 | Phase 8 | `infra/k8s/ingress/nginx_routes.yaml` | `make verify-ingress` | NGINX routing config | Unverified | 0.0 / 2.0 |
| **M2-37** | Ingress/Gateway| Hide Jaeger / Traces behind NGINX Ingress | 2.0 | Phase 8 | `infra/k8s/ingress/nginx_routes.yaml` | `make verify-ingress` | NGINX routing config | Unverified | 0.0 / 2.0 |
| **M2-38** | Ingress/Gateway| UI to test Agent accessible via Ingress | 2.0 | Phase 8 | `infra/k8s/ingress/nginx_routes.yaml` | `make verify-ingress` | Web UI accessible screenshot via routed URL | Unverified | 0.0 / 2.0 |
| **M2-39** | Ingress/Gateway| UI for Agent Registry accessible via Ingress | 2.0 | Phase 8 | `infra/k8s/ingress/nginx_routes.yaml` | `make verify-ingress` | Registry UI accessible screenshot | Unverified | 0.0 / 2.0 |
| **M2-40** | Ingress/Gateway| Basic Auth & Rate Limiting at Gateway | 2.0 | Phase 8 | `infra/k8s/ingress/auth_ratelimit.yaml` | `make test-ratelimit` | 401 Unauthorized & 429 Too Many Requests curl proofs | Unverified | 0.0 / 2.0 |
| **M2-41** | Ingress/Gateway| Custom Domain & HTTPS TLS configuration | 1.0 | Phase 8 | `infra/k8s/ingress/tls_cert.yaml` | `make verify-tls` | TLS handshake curl output with HTTPS | Unverified | 0.0 / 1.0 |
| **M2-42** | IaC | Terraform setup for GKE / Cloud infrastructure | 1.0 | Phase 8 | `infra/terraform/` | `make tf-validate` | `terraform plan` execution output log | Unverified | 0.0 / 1.0 |
| **M2-43** | IaC | Ansible roles to configure & deploy services | 1.0 | Phase 8 | `infra/ansible/roles/` | `make ansible-lint` | Ansible playbook syntax & dry-run log | Unverified | 0.0 / 1.0 |
| **M2-44** | Observability | Web API metrics (req/s, latency, error count) | 1.0 | Phase 8 | Prometheus metrics endpoint in FastAPI | `make verify-metrics` | Grafana dashboard screenshot of Web API | Unverified | 0.0 / 1.0 |
| **M2-45** | Observability | Computing telemetry (CPU, RAM, Disk, Network) | 1.0 | Phase 8 | Node Exporter / cAdvisor + Prometheus | `make verify-metrics` | Grafana dashboard screenshot of infrastructure | Unverified | 0.0 / 1.0 |
| **M2-46** | Observability | Centralized Logging (Promtail / Loki / Kibana) | 1.0 | Phase 8 | Vector / Promtail log collection config | `make verify-logs` | Kibana/Loki UI screenshot showing log stream | Unverified | 0.0 / 1.0 |
| **M2-47** | Observability | Distributed Tracing (OpenTelemetry + Jaeger) | 1.0 | Phase 8 | OTel instrumentor in FastAPI & Agents | `make verify-traces` | Jaeger UI screenshot showing distributed trace spans | Unverified | 0.0 / 1.0 |
| **M2-48** | Observability | LLM Telemetry (Tokens, TTFT, RTT, Safety blocks) | 2.0 | Phase 8 | LiteLLM Prometheus exporter / OpenTelemetry | `make verify-llm-metrics`| Grafana dashboard screenshot of LLM metrics | Unverified | 0.0 / 2.0 |
| **M2-49** | Observability | Agent Telemetry (Call counts, tool calls, errors)| 2.0 | Phase 8 | Langfuse / Prometheus agent instrumentation | `make verify-agent-metrics`| Grafana/Langfuse dashboard screenshot of agent calls | Unverified | 0.0 / 2.0 |
| **M2-50** | A/B Testing | Agent A/B testing with multiple model configs | 1.0 | Phase 5 | AgentGateway router A/B routing split | `make test-ab-agent` | A/B comparison metrics log & dashboard | Unverified | 0.0 / 1.0 |
| **M2-51** | A/B Testing | LLM version A/B test (vLLM candidate vs prod) | 1.0 | Phase 5 | LiteLLM model split routing | `make test-ab-llm` | Traffic distribution report between LLM models | Unverified | 0.0 / 1.0 |
| **M2-52** | Security | Centralized Secret Management (HashiCorp Vault)| 1.0 | Phase 8 | Vault Helm & Agent injector configs | `make vault-verify` | Vault status & injected secret verification log | Unverified | 0.0 / 1.0 |
| **M2-53** | Repo Design | Clean Code, README structure & Design Patterns | 2.0 | Phase 8 | Repository root & docstrings | `make verify-repo` | Clean code linting output & design pattern doc | Unverified | 0.0 / 2.0 |
| **M2-54** | Novel Idea 1 | Real-time Market Regime Drift Detection API | 2.0 | Phase 4 | `src/quant_edai/ml/drift_api.py` | `make test-drift` | KS-test / PSI drift calculation output & retrain hook | Unverified | 0.0 / 2.0 |
| **M2-55** | Novel Idea 2 | Multi-Tier Alpha Synthesis & Dynamic Sizing | 2.0 | Phase 6 | `src/quant_edai/quant/alpha_synthesis.py` | `make test-alpha-novel`| Empirical evidence of risk-adjusted alpha boost | Unverified | 0.0 / 2.0 |
| **TOTAL** | **Module 2** | **Total Rubric Points** | **100.0**| | | | | **0.0 / 100.0** | **0.0%** |
