# Architecture Blueprint: Quant AI Engineering Platform

## 1. System Vision & Objective

The **Quant AI Engineering Platform** provides an institutional-grade, end-to-end infrastructure for quantitative equity trading, factor research, and automated financial intelligence. It synthesizes:
- **Tick-First Big Data Engineering**: Streaming and batch ingestion of high-frequency market ticks and financial news into an Apache Iceberg Medallion Lakehouse on MinIO S3.
- **High-Performance OLAP Analytical Serving**: Sub-50ms vectorized queries on ClickHouse (`quant_dw`) serving multi-year factor research and intraday 1-minute bars.
- **Enterprise MLOps & Real-Time Feature Store**: Point-in-time correct feature management via Feast (offline Iceberg, online Redis), automated training via Kubeflow Pipelines, artifact tracking in MLflow, canary serving on KServe v2, and automated data drift monitoring.
- **Quantitative Alpha Research & Lifecycle Governance**: A 10-alpha strategy catalog, institutional quality gates, 3-split walk-forward validation (IS, OOS, TRADING), dynamic regime adaptation, and PM governance risk controls.
- **Event-Driven Paper Trading & Market Replay**: Institutional scalping engine (`PaperTradingEngine`) with realistic friction (5 bps slippage, 1 bps commission), strict risk guards (Stop-Loss, Take-Profit, Circuit Breaker, EOD flatting), and dual-panel time-synchronized charts (TradingView / QuantConnect style).
- **PostgreSQL ACID Ledger & State Persistence**: Comprehensive order and trade audit logging with unique idempotency constraints and automated SQLite fallback.
- **Agentic AI & RAG Platform**: Hierarchical Multi-Agent Fleet communicating via Model Context Protocol (MCP) and AgentGateway to deliver contextualized Investment Memorandums.
- **Interactive Multi-Page Quant Terminal**: Streamlit application providing real-time charting, alpha lifecycle management, live simulation control, and OLAP benchmarking.

---

## 2. End-to-End System Architecture Diagram

```mermaid
flowchart TD
    subgraph S1_Ingestion["1. Ingestion Layer"]
        DG["Synthetic Tick Generator<br>(--scale 0.1 | 1.0)"]
        KafkaFeed["Apache Kafka Cluster<br>(market.ticks, market.bars.1m)"]
        RawMinIO["External Source MinIO<br>(Raw files partition)"]
        DG --> RawMinIO
        DG --> KafkaFeed
    end

    subgraph S2_Lakehouse["2. Medallion Lakehouse Storage (MinIO S3)"]
        BronzeZone["Bronze Zone (MinIO S3)<br>raw_market_ticks, raw_financial_news"]
        SilverZone["Silver Zone (Apache Iceberg)<br>stg_market_ticks (trade_id dedup), stg_financial_news"]
        GoldZone["Gold Zone (Apache Iceberg & Resampled Bars)<br>gold_market_bars_1m, gold_market_bars_daily,<br>dim_companies (SCD2), feat_ticker_technical_indicators"]
        RawMinIO --> BronzeZone
    end

    subgraph S3_Processing["3. Processing Layer"]
        SparkBatch["PySpark Cluster<br>(AQE, Key Salting, Dedup, Iceberg Compaction)"]
        FlinkStream["Apache Flink Cluster<br>(Event-time Watermarks, RocksDB Dedup, 1m Windows)"]
        BronzeZone --> SparkBatch
        SparkBatch --> SilverZone
        SilverZone --> GoldZone
        KafkaFeed --> FlinkStream
        FlinkStream --> GoldZone
    end

    subgraph S4_OLAP["4. Analytical Serving & Feature Store"]
        ClickHouseDB["ClickHouse OLAP Database (quant_dw)<br>(ReplacingMergeTree: market_daily_ohlcv,<br>market_intraday_bars, feat_technical_indicators)"]
        RedisOnline["Redis Cluster<br>(Feast Online Store: Sub-10ms Feature Cache)"]
        GoldZone --> ClickHouseDB
        GoldZone -->|"Feast Offline"| RedisOnline
        FlinkStream --> RedisOnline
    end

    subgraph S5_MLOps["5. MLOps Platform"]
        Airflow["Apache Airflow (DP1 - DP4 DAGs)"]
        KFP["Kubeflow Pipelines (4-step Train Pipeline)"]
        MLflow["MLflow Model Registry (Champion Model)"]
        KServe["KServe v2 Model Serving (Canary 80/20)"]
        DriftAPI["FastAPI Drift Detection (KS-Test & PSI)"]
        GoldZone --> KFP
        KFP --> MLflow
        MLflow --> KServe
        RedisOnline --> DriftAPI
        DriftAPI -.-> Airflow
    end

    subgraph S6_Alpha["6. Quant Alpha Research & Serving"]
        AlphaCatalog["Alpha Catalog (10 Strategies)<br>alpha_catalog.py (score_strategy_strict)"]
        AlphaReg["Alpha Registry (Lifecycle & Quality Gates)<br>RESEARCH -> CANDIDATE -> PAPER_TRIAL -> PROMOTED -> RETIRED"]
        AlphaServe["AlphaSignalServer (alpha_serving.py)<br>Equal-Average Ensemble (1/K sum Z_i) | Regime Adapt"]
        PMControl["PMControlService (pm_controls.py)<br>Max Positions, Drawdown Breaker, Audit Trail"]
        AlphaCatalog --> AlphaServe
        AlphaReg --> AlphaServe
        PMControl --> AlphaServe
    end

    subgraph S7_Trading["7. Event-Driven Paper Trading & Replay Engine"]
        RC["ReplayController (dashboard_service.py)<br>Thread Worker | Pacing (1x - 50x) | Step-by-Step"]
        PTE["PaperTradingEngine.on_bar() (paper_trading.py)<br>Slippage 5 bps | Commission 1 bps | MTM | EOD Flat 15:55"]
        ClickHouseDB -->|"TRADING Split Bars"| RC
        ClickHouseDB -->|"Lagged As-Of Features"| RC
        RC --> PTE
        AlphaServe --> PTE
    end

    subgraph S8_Ledger["8. ACID Ledger & Persistence"]
        PostgresDB["PostgreSQL / SQLite Fallback (paper_db.py)<br>• paper_orders (Idempotency: uq_order_idempotency)<br>• paper_trades (Fills & Commissions)<br>• paper_positions (Active MTM)<br>• paper_portfolio_snapshots (Equity & Drawdown)"]
        PTE --> PostgresDB
    end

    subgraph S9_Terminal["9. Interactive Quant Terminal (Streamlit)"]
        UI_P1["Page 1: Market Candlestick Charts"]
        UI_P2["Page 2: Alpha Factor Research & Quality Gates"]
        UI_P3["Page 3: Real-Time Trading Terminal<br>Dual-Panel Charts (TradingView / QuantConnect Style)"]
        UI_P4["Page 4: Historical EDA & Live OLAP Benchmark"]
        ClickHouseDB --> UI_P1
        ClickHouseDB --> UI_P4
        RC <--> UI_P3
        PostgresDB --> UI_P2
        PostgresDB --> UI_P3
    end

    subgraph S10_Agent["10. LLM & Multi-Agent Fleet"]
        AgentGW["AgentGateway (Auth, Rate-limit, A2A Router)"]
        CoordAgent["Coordinator Agent"]
        TechAgent["Technical Quant Agent"]
        NewsAgent["Sentiment News Agent"]
        MCPTools["MCP Tools (FastAPI Helm)<br>Feast, KServe, Qdrant"]
        Qdrant["Qdrant Vector DB (News Embeddings)"]
        vLLM["vLLM / LiteLLM (Qwen 2.5 / Llama 3.3)"]
        AgentGW --> CoordAgent
        CoordAgent --> TechAgent
        CoordAgent --> NewsAgent
        TechAgent --> MCPTools
        NewsAgent --> MCPTools
        MCPTools --> RedisOnline
        MCPTools --> KServe
        MCPTools --> Qdrant
        CoordAgent -.-> vLLM
    end
```

---

## 3. Core Architectural Stacks & Responsibilities

| Stack | Primary Technologies | Core Responsibilities & Missions | Key Components |
|---|---|---|---|
| **1. Data Platform & Lakehouse** | MinIO S3, Apache Iceberg, PySpark, Apache Flink, Kafka | Atomic tick ingestion, deduplication (`trade_id`), schema evolution (`2024-06-01`), skew mitigation (salting + AQE), tumbling 1m and sliding 5m aggregations, SCD Type 2 dimension tables. | `data_generator.py`, `dedup_job.py`, `resample_job.py`, `iceberg_compaction.py`, `scd2_builder.py` |
| **2. Analytical OLAP Serving** | ClickHouse (`quant_dw`), Vectorized SIMD | Sub-50ms interactive querying for 1m intraday candles and multi-year daily bars; point-in-time technical indicators join without look-ahead bias; 7.1x speedup over direct Parquet scans. | `quant_dw.market_intraday_bars`, `quant_dw.market_daily_ohlcv`, `quant_dw.feat_technical_indicators`, `clickhouse_sync.py` |
| **3. MLOps & Real-Time Feature Store** | Feast, Redis, Kubeflow Pipelines, MLflow, KServe v2 | Point-in-time offline feature storage, sub-10ms Redis online feature serving, 4-step training pipeline (Logistic Regression vs LightGBM), canary 80/20 deployment, real-time drift detection (KS-test + PSI). | `feast_manager.py`, `kfp/pipeline.py`, `trainer.py`, `drift_api.py`, `mock_kserve_v2.py` |
| **4. Quant Alpha Research** | Python, NumPy, Pandas, SciPy | 10-alpha strategy catalog, 5-stage alpha lifecycle governance (`RESEARCH` to `APPROVED_FROZEN`), walk-forward validation (IS, OOS, TRADING), dynamic regime adaptation, PM governance risk controls. | `alpha_catalog.py`, `alpha_registry.py`, `alpha_serving.py`, `alpha_split_evaluation.py`, `pm_controls.py` |
| **5. Paper Trading & Replay Engine** | Python, Multi-threading | Microsecond event-driven execution on 1m bars, parametric institutional friction (5 bps slippage, 1 bps commission), risk guards (SL -12 bps, TP +20 bps, Circuit Breaker -2.0%, EOD flat 15:55 ET), Single Alpha & Top-5 Ensemble replay. | `paper_trading.py`, `market_replay.py`, `continuous_bar_feeder.py`, `ReplayController` |
| **6. ACID Ledger & Persistence** | PostgreSQL, SQLite Fallback | Complete transactional persistence for orders, trades, active positions, and portfolio valuation snapshots; hard idempotency key `uq_order_idempotency` preventing duplicate executions. | `paper_db.py`, `02_paper_trading_schema.sql` |
| **7. Multi-Agent AI Platform** | vLLM, LiteLLM, AgentGateway, MCP Tools, Qdrant | Hierarchical multi-agent fleet (Coordinator, Technical Quant, Sentiment News), standard MCP tool microservices, semantic vector search on financial filings, automated Investment Memorandum generation. | `mcp_tools_api.py`, `coordinator_agent.py`, `rag_pipeline.py` |
| **8. Interactive Quant Terminal** | Streamlit, Plotly | Unified 4-page UI for real-time candlestick visualization, alpha lifecycle management, live simulation controls, and OLAP benchmarking; dual-panel time-synchronized performance charts. | `1_Market_Charts.py`, `2_Alpha_Research.py`, `3_Real_Time_Trading.py`, `4_Historical_EDA_and_Engineering.py` |

---

## 4. Canonical 4-Year Dataset Splitting & Zero Look-Ahead Protection

To prevent data snooping and information leakage across training and trading:
- **In-Sample (IS)**: Start Date $\longrightarrow$ `2025-04-30` (Used exclusively for model training, alpha factor calibration, and baseline backtesting).
- **Out-of-Sample (OOS)**: `2025-05-01` $\longrightarrow$ `2026-04-30` (Used exclusively for quality gate evaluation, candidate ranking, and ensemble freezing).
- **Trading Replay (TRADING)**: `2026-05-01` $\longrightarrow$ `2026-10-31` (Held-out forward simulation data fed strictly into `PaperTradingEngine`).

Point-in-time features from ClickHouse are joined with a mandatory 1-day lag (`event_timestamp + Timedelta(days=1)`) to ensure bar scoring at Day $T$ uses only indicators computed up through Day $T-1$.
