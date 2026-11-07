# Architecture Blueprint: Quant AI Engineering Platform

## 1. System Vision & Objective

The Quant AI Engineering Platform provides an institutional-grade data, machine learning, and generative AI infrastructure tailored for quantitative equity trading and research.

The architecture synthesizes:
- **Streaming & Batch Big Data Engineering**: Ingesting high-frequency market data and corporate filings into a unified Medallion Lakehouse.
- **Enterprise MLOps**: Managing feature engineering with point-in-time correctness, automated model training pipelines, canary model deployments, and real-time distribution drift monitoring.
- **Agentic AI & RAG**: Hierarchical Multi-Agent Fleet communicating via Model Context Protocol (MCP) and AgentGateway to deliver contextualized, data-backed financial memorandums.

---

## 2. Layer-by-Layer Architectural Breakdown

```mermaid
flowchart TD
    subgraph IngestionLayer["1. Ingestion Layer"]
        DG["Synthetic Data Generator<br>(--scale 0.1 | 1.0)"]
        KafkaFeed["Apache Kafka Cluster<br>(market_ticks, live_news)"]
        RawMinIO["External Source MinIO<br>(Raw files partition)"]
    end

    subgraph StorageLayer["2. Medallion Lakehouse Storage (MinIO S3)"]
        BronzeZone["Bronze Zone (Parquet/JSON)<br>raw_market_ohlcv, raw_financial_news"]
        SilverZone["Silver Zone (Apache Iceberg)<br>stg_market_ohlcv, stg_financial_news"]
        GoldZone["Gold Zone (Apache Iceberg Star Schema)<br>dim_companies (SCD2), fact_daily_ohlcv,<br>fact_market_news, feat_ticker_technical_indicators"]
    end

    subgraph ProcessingLayer["3. Processing Layer"]
        SparkBatch["PySpark Cluster<br>(AQE, Key Salting, Dedup, Iceberg Compaction)"]
        FlinkStream["Apache Flink Cluster<br>(Event-time Watermarks, RocksDB State Dedup, 1m/5m Windows)"]
    end

    subgraph ServingLayer["4. Analytical & Operational Serving"]
        ClickHouseDB["ClickHouse OLAP Database<br>(Columnar Historical Quant Backtesting)"]
        PinotRealtime["Apache Pinot<br>(Real-time 1m Bar Queries)"]
        RedisOnline["Redis Cluster<br>(Feast Online Feature Store)"]
    end

    subgraph OrchestrationGovernance["5. Orchestration & Data Governance"]
        AirflowDags["Apache Airflow<br>(DP1 Ingest, DP2 Silver/Gold, DP3 Features, Drift DAG)"]
        DataHub["DataHub Catalog<br>(Automated Lineage & Quality Metadata)"]
        GreatExp["Great Expectations<br>(Schema, Non-null, Price Positivity Checks)"]
    end

    subgraph MLPlatform["6. ML Platform & MLOps"]
        FeastOffline["Feast Feature Store (Iceberg Gold)"]
        KubeflowPipe["Kubeflow Pipelines (KFP)<br>(Load, Split, Train LightGBM, Evaluate)"]
        MLflowReg["MLflow Model Registry<br>(Champion Model auto-tagged 'production')"]
        KServeInfra["KServe v2 Model Serving<br>(Canary A/B 80/20 Traffic Split)"]
        DriftMonitor["FastAPI Real-Time Drift Service<br>(KS-Test, PSI, Prometheus PushGateway)"]
    end

    subgraph AgentPlatform["7. LLM & Multi-Agent Fleet"]
        vLLMServer["vLLM / LiteLLM Server<br>(Qwen 2.5 / Llama 3.3)"]
        AgentGW["AgentGateway<br>(JWT Auth, Rate Limit, A2A Router)"]
        AgentRegistry["Langfuse & Agent Registry<br>(Prompt Management & Distributed Traces)"]
        MCPFastAPI["MCP Tools Container<br>(Feast online features, KServe model, Qdrant RAG)"]
        CoordFleet["Hierarchical Agent Fleet<br>(Coordinator + Technical Quant + Sentiment News Agents)"]
        QdrantDB["Qdrant Vector Database<br>(Dense Financial News Embeddings)"]
        ChatInterface["KAgent / OpenWebUI<br>(Ingress via NGINX Controller)"]
    end

    subgraph SimulationLayer["8. Real-time Paper Trading Simulation (Optional Add-on)"]
        ReplayEngine["Market Data Replay Service<br>(1x, 10x, 100x Speed Toggle)"]
        SignalEngine["Signal Generator Service<br>(Rules: >0.6 BUY, <0.4 SELL)"]
        PaperSim["Paper Trading Simulator<br>(FastAPI + PostgreSQL State)"]
        LiveDash["Real-time Dashboard<br>(Streamlit / Pinot / PostgreSQL)"]
    end

    %% Key interconnections
    DG --> RawMinIO
    DG --> KafkaFeed
    RawMinIO --> BronzeZone
    KafkaFeed --> FlinkStream
    BronzeZone --> SparkBatch
    SparkBatch --> SilverZone
    SilverZone --> GoldZone
    GoldZone --> ClickHouseDB
    FlinkStream --> PinotRealtime
    FlinkStream --> RedisOnline
    GoldZone --> FeastOffline
    FeastOffline --> RedisOnline
    GoldZone --> KubeflowPipe
    KubeflowPipe --> MLflowReg
    MLflowReg --> KServeInfra
    RedisOnline --> MCPFastAPI
    KServeInfra --> MCPFastAPI
    QdrantDB --> MCPFastAPI
    MCPFastAPI --> CoordFleet
    CoordFleet --> AgentGW
    AgentGW --> ChatInterface

    %% Paper Trading Simulation Loop
    ReplayEngine -->|"market.bars"| KafkaFeed
    FlinkStream -->|"quant.features"| SignalEngine
    SignalEngine -->|"Inference Request"| KServeInfra
    SignalEngine -->|"quant.signals"| PaperSim
    PaperSim -->|"quant.paper_trades"| PinotRealtime
    PaperSim -->|"State Persistence"| PostgreSQLState["PostgreSQL State Store"]
    PostgreSQLState --> LiveDash
    PinotRealtime --> LiveDash
```

---

## 3. Storage Layer & Medallion Schema Design

### Bronze Zone (MinIO S3)
- Stores immutable, append-only raw ingested files.
- Prefixes: `lakehouse-bronze/raw_market_ohlcv/date=YYYY-MM-DD/*.parquet` and `lakehouse-bronze/raw_financial_news/date=YYYY-MM-DD/*.json`.

### Silver Zone (Apache Iceberg)
- Tables: `lakehouse.silver.stg_market_ohlcv` and `lakehouse.silver.stg_financial_news`.
- Standardizes column types, parses ISO timestamps, eliminates duplicates via primary composite key, and dynamically adopts evolving schemas.

### Gold Zone (Apache Iceberg Star Schema)
- `dim_companies` (Slowly Changing Dimension Type 2):
  - Primary Key: `company_key` (surrogate), `ticker_symbol`.
  - Temporal Tracking: `valid_from_ts`, `valid_to_ts`, `is_current` (boolean).
  - Attributes: `company_name`, `sector`, `industry`, `market_cap_tier`, `exchange`.
- `fact_daily_ohlcv`:
  - Grain: One row per ticker per trading day.
  - Measures: `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `vwap`, `trade_count`.
- `fact_market_news`:
  - Grain: One row per published article.
  - Measures: `sentiment_score` ($-1.0$ to $+1.0$), `relevance_score`, `article_tokens`.
- `feat_ticker_technical_indicators`:
  - Standard Rubric Feature Table with required metadata columns: `event_timestamp`, `created`.
  - Measures: `rsi_14d`, `sma_ratio_20_50`, `macd_histogram_12_26`, `bollinger_pct_b`, `realized_volatility_30d`.

---

## 4. Analytical Serving (ClickHouse & Pinot)

1. **ClickHouse OLAP Engine**:
   - Source: Synced in micro-batches from Gold Iceberg.
   - Purpose: Multi-year historical factor backtesting and high-speed analytical aggregations across billions of rows.
   - Engine: `ReplacingMergeTree` with `ORDER BY (ticker_symbol, trade_date)`.
2. **Apache Pinot Real-Time Engine**:
   - Source: Real-time ingestion stream directly from Apache Kafka.
   - Purpose: Sub-50ms queries for intraday 1-minute bars, live price anomalies, and real-time dashboard visualization.

---

## 5. Orchestration & Data Governance

### Airflow DAG Flow
```
[External MinIO]
       │
       ▼ (DP1: Ingestion to Bronze)
[Bronze Zone: raw_market_ohlcv, raw_news] ──> [Great Expectations Checkpoint]
       │
       ▼ (DP2: Iceberg Clean & Transform)
[Silver Zone] ──> [Gold Zone: SCD2 Dim & Fact Tables] ──> [ClickHouse Sync]
       │
       ▼ (DP3: Feature Generation)
[Gold Zone: feat_ticker_technical_indicators] ──> [Feast Offline Catalog]
       │
       ▼ (Daily Materialize)
[Redis Online Feature Store]
```

All DAG executions emit automated open lineage via `acryl-datahub-airflow-plugin` to **DataHub**, displaying complete graph lineage and validation results.

---

## 6. Real-time Paper Trading Simulation Layer (Operational Add-on)

To demonstrate real-time closed-loop decision making, the platform includes a decoupled, optional **Paper Trading Simulation Module**:
- **Market Data Replay**: Replays out-of-sample historical OHLCV bars into Kafka topic `market.bars` with configurable speed (1x, 10x, 100x).
- **Streaming Pipeline**: Flink consumes bars, computes rolling indicators, and publishes to `quant.features`.
- **ML Scoring**: Signal Generator queries KServe v2 for $P(\text{Up})$ and emits BUY/SELL/HOLD events to `quant.signals`.
- **Execution Simulator**: Ingests signals, executes orders at Next Bar Open with realistic transaction costs (5 bps), and persists state in PostgreSQL (`sim_portfolios`, `sim_positions`, `sim_trades`).
- **Telemetry & Visualization**: Apache Pinot and PostgreSQL power a live dashboard tracking real-time equity curves, win rates, Sharpe Ratio, and Alpha vs. SPY Buy & Hold.

Detailed contracts, state schemas, and verification plans are in [PAPER_TRADING_SIMULATION.md](PAPER_TRADING_SIMULATION.md).
