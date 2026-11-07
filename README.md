# Quant AI Engineering Platform - End-to-End Medallion Lakehouse, ML & Agentic System

[![Domain: Quant Finance](https://img.shields.io/badge/Domain-Quant%20Finance-blue.svg)](domain/quant/01_data_platform.md)
[![Architecture: Medallion Iceberg](https://img.shields.io/badge/Architecture-Medallion%20Iceberg-teal.svg)](domain/quant/01_data_platform.md)
[![ML: Kubeflow & KServe](https://img.shields.io/badge/ML-Kubeflow%20%7C%20KServe-orange.svg)](domain/quant/02_ml_platform.md)
[![Agent: MCP & AgentGateway](https://img.shields.io/badge/Agent-MCP%20%7C%20A2A-purple.svg)](domain/quant/03_llm_agent_platform.md)
[![Rubric: EDAI K11](https://img.shields.io/badge/Rubric-EDAI%20K11%20100%25-green.svg)](rubic/EDAI%20K11.xlsx)

---

## Table of Contents

1. [Executive Summary & Business Domain](#1-executive-summary--business-domain)
2. [High-Level System Deployment Architecture](#2-high-level-system-deployment-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Medallion Architecture & Data Flow](#4-medallion-architecture--data-flow)
5. [Data Generator & Injected Anomalies (10% Test Mode Support)](#5-data-generator--injected-anomalies-10-test-mode-support)
6. [Batch & Stream Processing (Spark Skew & Flink Late Data)](#6-batch--stream-processing-spark-skew--flink-late-data)
7. [ML Platform (Feast, Kubeflow, MLflow, KServe & Drift Detection)](#7-ml-platform-feast-kubeflow-mlflow-kserve--drift-detection)
8. [Multi-Agent Platform & RAG (AgentGateway, MCP Tools, Coordinator Fleet)](#8-multi-agent-platform--rag-agentgateway-mcp-tools-coordinator-fleet)
9. [Verification, Quality Testing & Quant Alpha Backtesting](#9-verification-quality-testing--quant-alpha-backtesting)
10. [Documentation Index](#10-documentation-index)

---

## 1. Executive Summary & Business Domain

This repository implements a production-grade, end-to-end **Data Engineering, Machine Learning, and Multi-Agent AI Platform** tailored for quantitative finance (**US Equities Top 100 S&P 500 & Mega-Cap Tech**).

The platform continuously consumes and analyzes two core information streams:
1. **High-Frequency Market Data**: Intraday ticks, 1-minute aggregations, and daily OHLCV bars.
2. **Unstructured Financial News & Corporate Filings**: News articles, earnings releases, and regulatory filings.

### Key Capabilities
- **Medallion Lakehouse Storage**: Apache Iceberg on MinIO S3 (Bronze, Silver, Gold with SCD Type 2 dimension tables).
- **Dual Analytical Serving**: ClickHouse for historical OLAP/backtesting, Apache Pinot for sub-50ms real-time queries.
- **Fault-Tolerant Processing**: Spark batch jobs resolving data skew via key salting and AQE; Apache Flink streaming handling out-of-order late arrivals via watermarks and stateful RocksDB deduplication.
- **Enterprise ML Lifecycle**: Feast feature store (offline Iceberg, online Redis), Kubeflow Pipelines, MLflow model registry, KServe v2 serving with 80/20 canary deployment, and real-time drift detection.
- **Hierarchical Agent Fleet**: vLLM/LiteLLM, AgentGateway routing Agent-to-Agent (A2A) interactions, 3 containerized MCP tools (FastAPI), and a Coordinator Agent orchestrating investment briefings.
- **Quant Alpha Backtesting**: Automated walk-forward evaluation calculating Information Coefficient (IC), Rank IC, Sharpe Ratio, and Maximum Drawdown vs SPY.

---

## 2. High-Level System Deployment Architecture

All components represent distinct, containerized **deployable units**. Data flows sequentially along numbered, annotated directional pathways.

```mermaid
flowchart TD
    %% Sources
    subgraph S_Ingestion["01. Ingestion Layer"]
        Gen["Data Generator<br>(--scale 0.1 / 1.0)"]
        ExtMinIO["External Source MinIO<br>(Simulated Source Bucket)"]
        Kafka["Apache Kafka<br>(market_ticks topic)"]
    end

    %% Storage & Lakehouse
    subgraph S_Storage["02. Storage & Lakehouse Layer (MinIO S3)"]
        Bronze["Bronze Zone (MinIO S3)<br>raw_market_ohlcv, raw_news"]
        Silver["Silver Zone (Apache Iceberg)<br>stg_market_ohlcv, stg_news"]
        Gold["Gold Zone (Apache Iceberg)<br>dim_companies (SCD2), fact_ohlcv, feat_tables"]
    end

    %% Processing & Serving
    subgraph S_Processing["03. Processing & Analytical Serving"]
        SparkCluster["PySpark Cluster<br>(Salting, AQE, Compaction)"]
        FlinkCluster["Apache Flink Cluster<br>(RocksDB Dedup, Watermarks)"]
        ClickHouse["ClickHouse OLAP<br>(Historical Quant Analytics)"]
        Pinot["Apache Pinot<br>(Real-time Serving)"]
    end

    %% ML Platform
    subgraph S_ML["04. ML Platform Layer"]
        FeastOnline["Redis Cluster<br>(Feast Online Store)"]
        Airflow["Apache Airflow<br>(DP1, DP2, DP3, Drift DAGs)"]
        DataHub["DataHub<br>(Catalog & Lineage)"]
        KFP["Kubeflow Pipelines<br>(4-step Train Pipeline)"]
        MLflow["MLflow Registry<br>(Model & Backtest Artifacts)"]
        KServe["KServe Model Server<br>(Canary A/B 80/20)"]
        DriftAPI["Drift Detection API<br>(FastAPI + KEDA)"]
    end

    %% LLM & Agents
    subgraph S_Agent["05. LLM & Multi-Agent Fleet"]
        vLLM["vLLM / LiteLLM<br>(Qwen 2.5 / Llama 3.3)"]
        AgentGateway["AgentGateway<br>(Auth, Rate-limit, A2A)"]
        CoordAgent["Coordinator Agent<br>(Langfuse Registry)"]
        TechAgent["Technical Quant Agent"]
        NewsAgent["Sentiment News Agent"]
        MCPTools["MCP Tools (FastAPI Helm)<br>Feast, KServe, Qdrant"]
        Qdrant["Qdrant Vector DB<br>(News Embeddings)"]
        AgentUI["KAgent / OpenWebUI<br>(Chat Interface)"]
    end

    %% Data Flow Arrows with numbers
    Gen -->|"1. Generate bulk seed files"| ExtMinIO
    Gen -->|"2. Produce streaming tick events"| Kafka

    ExtMinIO -->|"3. DP1 Airflow Ingestion"| Bronze
    Bronze -->|"4. DP2 Spark clean & dedup"| SparkCluster
    SparkCluster -->|"5. Write Silver/Gold Iceberg"| Silver
    Silver -->|"6. Build SCD2 & Aggregations"| Gold
    Gold -->|"7. Sync historical time-series"| ClickHouse

    Kafka -->|"8. Consume streaming ticks"| FlinkCluster
    FlinkCluster -->|"9. Ingest 1m aggregates"| Pinot
    FlinkCluster -->|"10. Push streaming features"| FeastOnline

    Gold -->|"11. DP3 Compute technical features"| Gold
    Gold -->|"12. Daily Feast materialization"| FeastOnline
    Airflow -.->|"13. Emit lineage & quality assertions"| DataHub

    Gold -->|"14. Ingest training dataset"| KFP
    KFP -->|"15. Log metrics & register champion"| MLflow
    MLflow -->|"16. Deploy model artifact"| KServe

    FeastOnline -->|"17. Real-time feature pull"| DriftAPI
    DriftAPI -.->|"18. Alert on distribution drift"| Airflow

    Bronze -->|"19. RAG chunk & embed"| Qdrant
    AgentUI -->|"20. User analytical query"| AgentGateway
    AgentGateway -->|"21. Route request"| CoordAgent
    CoordAgent -->|"22. A2A Sub-task dispatch"| TechAgent
    CoordAgent -->|"22. A2A Sub-task dispatch"| NewsAgent

    TechAgent -->|"23. Fetch features & ML prediction"| MCPTools
    NewsAgent -->|"23. Semantic news vector search"| MCPTools
    MCPTools -->|"24. Query Redis, KServe, Qdrant"| FeastOnline
    MCPTools -->|"24. Query Redis, KServe, Qdrant"| KServe
    MCPTools -->|"24. Query Redis, KServe, Qdrant"| Qdrant

    CoordAgent -.->|"25. Inference synthesis"| vLLM
    CoordAgent -->|"26. Return Investment Memo"| AgentUI
```

---

## 3. Repository Structure

```
quant_edai/
├── README.md                           # Master system overview and operational runbook
├── Makefile                            # Unified command center (includes make/*.mk)
├── make/                               # Modular Makefile components (00_env to 09_phases)
│   ├── 00_env.mk                       # Environment, Python, uv & self-documenting help
│   ├── 01_infra.mk                     # Infrastructure lifecycle (MinIO, Kafka, Postgres, Redis)
│   ├── 02_generator.mk                 # Synthetic market data & anomaly generation
│   ├── 03_processing.mk                # PySpark Batch & Apache Flink Streaming (Phase 2)
│   ├── 04_orchestration.mk             # Apache Airflow & DataHub Governance (Phase 3)
│   ├── 05_mlops.mk                     # Feast Feature Store, Kubeflow & KServe (Phase 4)
│   ├── 06_agents.mk                    # LLM Inference, AgentGateway & Agent Fleet (Phase 5)
│   ├── 07_quant.mk                     # Quant Alpha Backtesting & Paper Trading (Phases 6-7)
│   ├── 08_test.mk                      # Pytest unit tests, Hypothesis & benchmarks
│   └── 09_phases.mk                    # Milestone phase runners (phase1 -> phase8)
├── coursework_proposal.md              # Official coursework project proposal
├── domain/
│   ├── ecommerce/                      # Provided reference baseline
│   │   ├── 01_data_platform.md
│   │   ├── 02_ml_platform.md
│   │   └── 03_llm_agent_platform.md
│   └── quant/                          # Implemented Quant Domain Architecture
│       ├── 01_data_platform.md         # Iceberg, MinIO, ClickHouse, Pinot, Spark/Flink
│       ├── 02_ml_platform.md           # Feast, Kubeflow, MLflow, KServe, Drift, Backtest
│       └── 03_llm_agent_platform.md    # AgentGateway, MCP Tools, Multi-Agent, RAG
├── docs/                               # Detailed technical reports and rubric proofs
│   ├── ARCHITECTURE_BLUEPRINT.md       # Complete architectural specifications
│   ├── DOCKER_OPTIMIZATION.md          # Multi-stage Docker optimization, image size & security
│   ├── DATA_PROBLEMS_AND_SKEW.md       # Injected anomalies, Spark UI optimization & proof
│   ├── STORAGE_AND_LAKEHOUSE.md        # Iceberg schema, compaction, partitioning & ClickHouse
│   ├── VERIFICATION_AND_TESTING.md     # Pytest >90%, Great Expectations, Locust SLA
│   └── QUANT_ALPHA_BACKTESTING.md      # Factor evaluation, IC, Sharpe, Drawdown tear sheet
├── rubic/
│   └── EDAI K11.xlsx                   # Rubric specification for grading criteria
├── config/                             # Centralized system configurations
│   ├── data_generator.yaml             # Generator config (10% test mode toggle & fault rates)
│   ├── feast_feature_store.yaml        # Feast offline & online store definitions
│   └── airflow_connections.yaml        # Centralized Airflow connections and variables
├── src/                                # Application and pipeline source code
│   ├── generator/                      # Multi-worker synthetic data generator
│   │   ├── data_generator.py           # Core generator engine (--scale 0.1 / 1.0)
│   │   └── faults_injector.py          # Skew, schema evolution, duplicates & late arrivals
│   ├── spark_jobs/                     # PySpark batch jobs
│   │   ├── baseline_job.py             # Baseline unoptimized job showing stragglers
│   │   ├── optimized_skew_job.py       # Key salting, AQE skew join, broadcast join
│   │   ├── iceberg_compaction.py       # Small-file rewrite & maintenance
│   │   └── compute_features.py         # DP3 offline technical indicators generator
│   ├── flink_jobs/                     # Apache Flink streaming jobs
│   │   ├── streaming_dedup.py          # RocksDB stateful deduplication filter
│   │   └── window_aggregations.py      # Tumbling 1m & sliding 5m event-time windows
│   ├── dags/                           # Apache Airflow orchestration DAGs
│   │   ├── dag_raw_ingestion_bronze.py # DP1: Ingestion to Bronze + Great Expectations
│   │   ├── dag_bronze_to_gold.py       # DP2: Silver/Gold Iceberg processing + SCD2
│   │   ├── dag_compute_features.py     # DP3: Feature computation + DataHub lineage
│   │   └── dag_drift_retrain.py        # Periodic drift detection & Kubeflow trigger
│   ├── ml_platform/                    # ML pipeline and serving components
│   │   ├── kfp_pipeline.py             # Kubeflow 4-step pipeline (Load, Split, Train, Eval)
│   │   ├── model_trainer.py            # LightGBM vs Logistic Regression training logic
│   │   └── quant_backtester.py         # Walk-forward backtester (IC, Sharpe, Max Drawdown)
│   ├── agent_platform/                 # Multi-Agent and MCP services
│   │   ├── mcp_tools_api.py            # FastAPI service exposing 3 standard MCP tools
│   │   ├── coordinator_agent.py        # Hierarchical coordinator dispatching A2A requests
│   │   └── rag_pipeline.py             # Document chunking, embedding & Qdrant indexing
│   └── tests/                          # Automated test suites
│       ├── unit/                       # Pytest unit tests (Coverage >= 90%)
│       ├── data_quality/               # Great Expectations checkpoint suites
│       └── load_test/                  # Locust load test scenarios
└── deployments/                        # Infrastructure and deployment descriptors
    ├── docker-compose.yml              # Local multi-service composition
    ├── Dockerfile.multistage           # Optimized multi-stage Docker build
    ├── helm/                           # Helm charts for MCP tools and agent deployments
    └── kubernetes/                     # Kubernetes manifests for Kind / K8s clusters
```

---

## 4. Medallion Architecture & Data Flow

- **Bronze Zone (`raw_market_ohlcv`, `raw_financial_news`)**:
  - Ingested directly as append-only raw Parquet and JSON files on MinIO.
  - Preserves exact source schema and audit metadata (`ingestion_timestamp`, `source_filename`).
- **Silver Zone (`stg_market_ohlcv`, `stg_financial_news`)**:
  - Apache Iceberg tables stored on MinIO.
  - Implements automated deduplication (`ticker_symbol`, `timestamp`), handles schema evolution, drops corrupted price points, and parses raw text.
- **Gold Zone (`dim_companies`, `fact_daily_ohlcv`, `fact_market_news`, `feat_ticker_technical_indicators`)**:
  - Curated, business-ready Iceberg tables modeled in a **Star Schema**:
    - `dim_companies`: Slowly Changing Dimension Type 2 (SCD2) with `valid_from_ts`, `valid_to_ts`, and `is_current` flags.
    - `feat_ticker_technical_indicators`: Gold feature table adhering to rubric naming convention with `event_timestamp` and `created` columns.
- **Serving Layer**:
  - **ClickHouse OLAP**: Mirrored from Gold Iceberg for high-speed historical queries and multi-year quant backtesting.
  - **Apache Pinot**: Ingests real-time 1m bars from Flink/Kafka for sub-50ms dashboard and API retrieval.

---

## 5. Data Generator & Injected Anomalies (10% Test Mode Support)

The generator supports flexible scaling via the unified Makefile:
```bash
# Display all commands, options, and parameters
make help

# Rapid local verification and CI/CD testing (10% volume, ~10 GB)
make generate SCALE=0.1

# Full benchmark execution (>= 100 GB)
make generate SCALE=1.0

# Run complete Phase 1 pipeline (Data Gen + Streaming Ticks + Staging + Evidence)
make phase1 SCALE=0.1

# Benchmark Dockerfile Multi-Stage Optimization (Rubric M1-03 & M1-04)
make benchmark-images
```

### Injected Fault Matrix & Cardinality Characteristics (Rubric M1-05)
1. **Volume Skew**: 80% of volume and news articles concentrated in 7 Mega-Cap tickers (AAPL, NVDA, MSFT, AMZN, GOOGL, META, TSLA).
2. **Schema Evolution**: At partition boundary (`2024-06-01`), the source appends `vwap` (float), `trade_count` (int), and `sentiment_score` (float).
3. **Offline Duplicates**: 2.0% duplicate price records injected with identical `trade_id` and slightly drifted timestamps.
4. **Streaming Late Arrivals**: 5% of streaming ticks delayed by 5 to 10 minutes to simulate secondary OTC feeds.
5. **Streaming Duplicates**: 1.5% duplicate Kafka messages injected via producer at-least-once simulation.
6. **Data Drift**: Market regime transition from low-volatility Bull market to high-volatility Bear market.
7. **Cardinality Profiling**:
   - **High Cardinality ($\text{Ratio} \approx 0.98 - 1.0$)**: `trade_id` in `market_ohlcv`, `article_id` in `financial_news` (indexed with Parquet Bloom Filter).
   - **Medium Cardinality ($\text{Ratio} \approx 0.06$)**: `trade_date` (218 trading days, used as Iceberg physical partition key).
   - **Low / Ultra-Low Cardinality ($\text{Ratio} < 0.005$)**: `ticker_symbol` (16–100 distinct tickers, compressed via ClickHouse `LowCardinality(String)` and Parquet Dictionary), `exchange` (2 distinct values: NASDAQ, NYSE).
   - Complete profiling matrix generated automatically in `generation_stats.json`.

---

## 6. Batch & Stream Processing (Spark Skew & Flink Late Data)

- **PySpark Skew Mitigation**:
  - Demonstrates severe partition bottlenecks on the baseline unoptimized job.
  - Employs **Key Salting** (generating salted keys `ticker_symbol_salt = concat(ticker, "_", rand(0, 15))`), **Broadcast Joins** on `dim_companies`, and enables **Adaptive Query Execution (AQE)** skew join handling.
- **Apache Flink Stream Handling**:
  - Resolves late arrivals using **BoundedOutOfOrderness Watermarks** (`Duration.ofMinutes(10)`).
  - Implements **Stateful RocksDB Deduplication** over a 1-hour rolling TTL window.
  - Computes 1-minute Tumbling and 5-minute Sliding window technical metrics.

---

## 7. ML Platform (Feast, Kubeflow, MLflow, KServe & Drift Detection)

- **Feast Feature Store**:
  - Offline store on Iceberg Gold; Online store on Redis cluster.
  - Airflow DAG incrementally materializes daily snapshots into Redis.
- **Kubeflow 4-Step Pipeline**:
  - 1. Ingest historical features and join forward label table.
  - 2. Perform temporal walk-forward split (80% Train, 10% Val, 10% Test).
  - 3. Train baseline Logistic Regression vs tuned LightGBM Classifier.
  - 4. Evaluate AUC-ROC, Log-Loss, and directional accuracy; auto-register champion model on MLflow with `production` tag.
- **KServe v2 Canary Serving**:
  - Deploys InferenceService with 80% traffic to champion (`v1`) and 20% to challenger (`v2`).
- **Real-Time Drift Monitoring**:
  - FastAPI service evaluates KS-statistic and PSI on incoming features without ground-truth.
  - Pushes metrics to Prometheus PushGateway; Airflow triggers automated Kubeflow retrain upon threshold breach.

---

## 8. Multi-Agent Platform & RAG (AgentGateway, MCP Tools, Coordinator Fleet)

- **LLM Inference Server**: vLLM / LiteLLM hosting open weights (Qwen 2.5 / Llama 3.3).
- **AgentGateway & Registry**: Centralized entrypoint managing authentication, rate limiting, and Agent-to-Agent (A2A) communications; monitored via Langfuse.
- **Containerized MCP Tools (FastAPI via Helm `--atomic`)**:
  - `fetch_ticker_quant_features`: Sub-second online feature retrieval from Redis.
  - `predict_ticker_direction`: KServe model invocation returning directional probabilities.
  - `search_market_news_rag`: Dense vector semantic search against Qdrant financial news embeddings.
- **Hierarchical Fleet**:
  - `Technical Quant Agent`: Queries quantitative metrics and ML model probabilities.
  - `Sentiment & News Agent`: Evaluates qualitative news sentiment and corporate filings.
  - `Coordinator Agent`: Synthesizes outputs into an authoritative **Investment Memorandum** delivered to the User Chat UI.

---

## 9. Verification, Quality Testing & Quant Alpha Backtesting

### Testing Rigor
- **Unit Testing**: Pytest suite with mock fixtures and equivalence partitioning achieving **>90% test coverage**.
- **Data Quality**: Great Expectations automated checkpoints validating schema, null constraints, and price positivity across all Airflow stages.
- **Load Testing**: Locust load test measuring throughput (>500 req/s) and latency SLAs (p95 < 25ms).
- **Scale Testing**: Validated across `--scale 0.1` and `--scale 1.0`.

### Quant Alpha Backtesting Module
Integrated module (`src/ml_platform/quant_backtester.py`) evaluating strategy performance without look-ahead bias:
- **Information Coefficient (IC)** and **Rank IC**.
- **Annualized Sharpe Ratio** and **Maximum Drawdown (MDD)**.
- **Benchmark Comparison**: Benchmarked directly against S&P 500 ETF (SPY) Buy & Hold. Performance tear sheets logged automatically to MLflow.

---

## 10. Documentation Index

For exhaustive technical breakdowns, implementation code, and screenshot proofs, refer to:
- [Domain Data Platform Design](domain/quant/01_data_platform.md)
- [Domain ML Platform Design](domain/quant/02_ml_platform.md)
- [Domain LLM & Agent Platform Design](domain/quant/03_llm_agent_platform.md)
- [Master Architecture Blueprint](docs/ARCHITECTURE_BLUEPRINT.md)
- [Docker Multi-Stage Optimization Benchmark](docs/DOCKER_OPTIMIZATION.md)
- [Data Problems, Skew & Cardinality](docs/DATA_PROBLEMS_AND_SKEW.md)
- [Storage Optimization & Lakehouse](docs/STORAGE_AND_LAKEHOUSE.md)
- [Verification, Testing & Load SLA](docs/VERIFICATION_AND_TESTING.md)
- [Quant Alpha Backtesting Report](docs/QUANT_ALPHA_BACKTESTING.md)
- [Real-Time Paper Trading Simulation](docs/PAPER_TRADING_SIMULATION.md)
