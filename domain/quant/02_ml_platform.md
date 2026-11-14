# Quant Finance - ML Platform Architecture & Design

## 1. ML Use Case: Next-Day Directional Prediction & Alpha Signal Generation

### Task Definition
Predict whether the next trading day's closing price for an individual equity will close higher than the current day's close:
$$\text{Return}_{t+1} = \frac{\text{Close}_{t+1} - \text{Close}_{t}}{\text{Close}_{t}}$$

- **Prediction Entity**: `ticker_symbol`
- **Prediction Cadence**: Evaluated daily at market close ($T$).
- **Prediction Target**: Directional classification $\mathbb{I}(\text{Return}_{t+1} > 0)$ and continuous expected return for alpha factor ranking.

---

## 2. Label Definition & Time-Series Splitting

### Label Table: `label_ticker_daily_return`
- **Columns**: `ticker_symbol`, `label_date`, `next_day_close`, `daily_return_fwd`, `direction_label` ($1$ for Up, $0$ for Down).
- **Grain**: One row per ticker per trading day.
- **Join Key**: `(ticker_symbol, label_date = event_timestamp)` joined against the offline feature snapshots.

### Split Strategy: Temporal Walk-Forward Split
To eliminate **look-ahead bias** and data leakage:
- **Train Set**: First 80% chronological trading days.
- **Validation Set**: Next 10% chronological trading days.
- **Test Set**: Final 10% chronological trading days.

---

## 3. Feature Store Plan (Feast)

### Offline & Online Architecture
- **Offline Store**: Apache Iceberg Gold table `feat_technical_indicators` (partitioned/tracked by `dataset_id`).
- **Online Store**: Redis cluster for sub-10ms point-in-time feature lookup.

### Feature Definitions
```mermaid
flowchart LR
    Iceberg["Iceberg Gold: feat_technical_indicators<br>(Offline Store)"] -->|"Airflow DP3 Materialize"| FeastOnline["Feast Online Store<br>(Redis)"]
    Flink["Flink Streaming Engine"] -->|"Real-time Ingestion"| FeastOnline
    FeastOnline -->|"Sub-10ms Feature Retrieval"| WebAPI["Data Fetch Web API<br>(FastAPI)"]
    WebAPI -->|"Enriched Features"| KServe["KServe v2 Inference Service<br>(FastAPI Runner)"]
```

#### Offline Historical Features (`feat_technical_indicators`)
> Features are sourced directly from the Lakehouse Gold layer produced by PySpark/Airflow DP3 (`feat_technical_indicators`).

| Tier | Feature Name | Type | Description | Source / Status |
|---|---|---|---|---|
| **[MVP Core]** | `close_price` | Float | Closing price of equity | DP3 Gold table |
| **[MVP Core]** | `sma_20` | Float | 20-day Simple Moving Average | `feature_calculator.py` |
| **[MVP Core]** | `sma_50` | Float | 50-day Simple Moving Average | `feature_calculator.py` |
| **[MVP Core]** | `sma_ratio_20_50` | Float | Ratio of 20-day SMA to 50-day SMA (trend proxy) | `feature_calculator.py` |
| **[MVP Core]** | `rsi_14d` | Float | 14-day Relative Strength Index (momentum/exhaustion) | `feature_calculator.py` |
| **[MVP Core]** | `macd_12_26` | Float | MACD line (12 EMA - 26 EMA) | `feature_calculator.py` |
| **[MVP Core]** | `macd_signal_9` | Float | MACD 9-day Signal line | `feature_calculator.py` |
| **[MVP Core]** | `macd_histogram` | Float | MACD histogram (MACD line - Signal line) | `feature_calculator.py` |
| **[MVP Core]** | `rolling_volatility_30d`| Float | 30-day annualized realized return volatility | `feature_calculator.py` |
| **[MVP Core]** | `intraday_return_1d` | Float | Close-to-close daily return | `feature_calculator.py` |
| **[MVP Core - Audit]**| `event_timestamp` | Timestamp | Point-in-time correctness timestamp (Rubric M1-34) | DP3 Gold table |
| **[MVP Core - Audit]**| `dataset_id` | String | Dataset isolation key matching Bronze/Silver/Gold | DP3 Gold table |
| **[MVP Core - Audit]**| `created` | Timestamp | Feature registration timestamp (Rubric M1-34) | DP3 Gold table |
| *[Stretch / v2]* | `bollinger_pct_b` | Float | Position of close price relative to Bollinger Bands | Planned v2 expansion |
| *[Stretch / v2]* | `news_sentiment_avg_3d` | Float | Rolling 3-day weighted average news sentiment score | Depends on RAG (Phase 7) |
| *[Stretch / v2]* | `news_article_count_3d` | Int | Surge in company-specific news mentions | Depends on RAG (Phase 7) |

#### Streaming Real-Time Features (Flink Push to Feast Online Store)
> Real-time feature push tier. Phase 4 MVP prioritizes robust batch-to-online materialization (Airflow DP $\to$ Redis); streaming feature push serves as an enhancement tier.

| Tier | Feature Name | Type | Description | Status |
|---|---|---|---|---|
| *[Stretch / v2]* | `intraday_return_1h` | Float | Price return over trailing 60 minutes | Streaming enhancement |
| *[Stretch / v2]* | `intraday_vwap_spread` | Float | Spread between current price & 1m rolling VWAP | Streaming enhancement |
| *[Stretch / v2]* | `order_flow_imbalance_15m`| Float | 15-minute buy vs sell trade volume ratio | Streaming enhancement |

---

## 4. Kubeflow Training Pipeline (KFP) & MLflow Registry

The training pipeline consists of 4 containerized components run via Kubeflow Pipelines:

```mermaid
flowchart TD
    Step1["Step 1: Load Features & Join Labels<br>(Feast get_historical_features + label table)"]
    Step2["Step 2: Temporal Train/Val/Test Split<br>(Walk-forward without look-ahead bias)"]
    Step3["Step 3: Model Training & Hyperparameter Tuning<br>(Baseline Logistic Regression vs LightGBM Classifier)"]
    Step4["Step 4: Model Evaluation & Alpha Backtesting<br>(AUC-ROC, Sharpe Ratio, Max Drawdown, Log to MLflow)"]

    Step1 --> Step2 --> Step3 --> Step4
    Step4 -->|"Auto-tag 'production'"| MLflow["MLflow Model Registry"]
    MLflow -->|"Deploy Artifact"| KServeDeploy["KServe Canary Deployment"]
```

### Models & Evaluation
- **Baseline**: Logistic Regression with L2 regularization.
- **Production Model**: LightGBM Classifier with early stopping on validation AUC-ROC.
- **MLflow Tracking**: Logs hyperparameters (`num_leaves`, `learning_rate`, `n_estimators`), metrics (AUC-ROC, Log-loss, F1), and model artifacts. Automatically tags the top-performing run as `latest` and `production`.

---

## 5. Model Serving (KServe v2 Protocol) & Canary A/B Testing

- Deployed using **KServe InferenceService** running standard v2 data plane.
- **Canary Traffic Split**:
  - `v1` (Current champion model): Serves 80% of inference requests.
  - `v2` (Challenger candidate model): Serves 20% of inference requests.
- Automatic routing through Kubernetes Ingress / Gateway.

---

## 6. Real-Time Drift Detection & Auto-Retraining Loop

1. **FastAPI Drift Detection Service**:
   - Computes Kolmogorov-Smirnov (KS) test and Population Stability Index (PSI) on incoming streaming features against training feature distributions.
   - Operates in real-time without requiring immediate ground-truth labels.
2. **Observability & Retraining Trigger**:
   - Pushes drift scores to Prometheus PushGateway, visualized on Grafana.
   - Airflow drift detection DAG triggers an automated call to the Kubeflow Pipeline Run API when feature drift exceeds threshold (PSI > 0.25).

---

## 7. Quant Alpha Factor Research & Signal Serving Pipeline

A specialized, reproducible evaluation module and real-time signal serving microservice:
- **10-Alpha Catalog**: Includes Mean Reversion (`alpha_rsi_mr_intraday`, `alpha_bollinger_reversal`, `alpha_vwap_mean_revert`), Trend & Momentum (`alpha_mom_z_intraday`, `alpha_macd_z_intraday`, `alpha_breakout_donchian`), Volatility (`alpha_vol_breakout_atr`), Composite Ensemble (`alpha_composite_eq_intraday`), Dynamic Regime-Adaptive (`alpha_regime_adapt_intraday`), and ML Directional (`alpha_lightgbm_direction`).
- **Signal Normalization**: Warm-up rolling z-score normalization ($z = \frac{x - \mu}{\sigma}$) mapped to discrete trading actions (`BUY` if $z > 1.5$, `SELL` if $z < -1.5$, `FLAT` otherwise).
- **Ensemble Modes**: Supports Single Alpha isolation mode and Approved Frozen Ensemble aggregation (`APPROVED_FROZEN` status in `alpha_registry`).
- **Factor Evaluation Metrics**:
  - **Information Coefficient (IC)**: Pearson correlation between predicted alpha and actual forward returns.
  - **Rank IC**: Spearman rank correlation assessing monotonic ranking power.
  - **Hit Rate**: Percentage of trading intervals with positive directional return.
  - **Risk-Adjusted Metrics**: Annualized Sharpe Ratio, Sortino Ratio, Maximum Drawdown (MDD).

```mermaid
flowchart TD
    subgraph FeatureInputs["Feature Inputs (Zero Look-Ahead)"]
        F1["Feast Online Store (Redis)"]
        F2["ClickHouse quant_dw.market_intraday_bars"]
        F3["Iceberg Gold feat_technical_indicators"]
    end

    subgraph AlphaScoringPipeline["Alpha Scoring & Signal Generation Pipeline"]
        Reg["Alpha Registry (Provenance & Status Check)"]
        Cat["10-Alpha Catalog (alpha_catalog.py)"]
        Norm["Rolling Z-Score Normalizer & Warm-up (N=30)"]
        Agg["Weight Aggregator: Single Alpha vs. Ensemble"]
        Thresh["Action Classifier (|z| > 1.5 -> BUY / SELL / FLAT)"]
    end

    subgraph DownstreamExec["Execution & Evaluation Handoff"]
        PaperEngine["PaperTradingEngine (on_bar Execution)"]
        Backtester["Walk-Forward Backtester (metrics.py)"]
        MLflowTrack["MLflow Tracking & Registry"]
    end

    FeatureInputs --> Cat
    Reg --> Cat
    Cat --> Norm
    Norm --> Agg
    Agg --> Thresh
    Thresh --> PaperEngine
    Thresh --> Backtester
    Backtester --> MLflowTrack
```

---

## 8. Implemented & Verified Codebase Architecture Mapping

| Component | Source File / Module | Implementation Status | Description & Verification |
|---|---|---|---|
| **Feature Calculator** | [feature_calculator.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/processing/features/feature_calculator.py) | `VERIFIED` | Generates SMA, RSI, MACD, volatility from Iceberg Gold `market_daily_ohlcv` with zero lookahead bias. |
| **Feature Store (Feast & Redis)** | [feast_manager.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/ml/feast/feast_manager.py) | `VERIFIED` | Offline store on Iceberg Gold `feat_technical_indicators`, online store materialization to Redis (`redis:6379`). |
| **Model Trainer & Registry** | [trainer.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/ml/trainer.py) | `VERIFIED` | Trains LogisticRegression & LightGBM with walk-forward temporal split; logs to MLflow (`sqlite:///mlflow.db`). |
| **Alpha Registry & 10-Alpha Catalog** | [alpha_registry.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/alpha_registry.py), [alpha_catalog.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/alpha_catalog.py) | `VERIFIED` | Registers and evaluates 10 alpha strategies with metadata provenance (`model_version`, `dataset_id`, `status`). |
| **Alpha Serving & Signal Server** | [alpha_serving.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/alpha_serving.py) | `VERIFIED` | Microservice serving directional probabilities and ranking signals from ML models and Feast features. |
| **KServe v2 Protocol Serving** | `src/quant_edai/serving/kserve_v2_runner.py` / `mock_kserve_v2.py` | `VERIFIED` | Standalone FastAPI KServe v2 runner implementing `POST /v2/models/{name}/infer`, loading champion models from MLflow. |
| **Paper Trading Engine** | [paper_trading.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/paper_trading.py), [paper_db.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/paper_db.py) | `VERIFIED` | Long/Flat order execution at bar close with 5 bps slippage, 1 bps fee, SL/TP/Circuit Breaker, ACID deduplication in PostgreSQL. |
| **Walk-Forward Backtesting** | [backtester.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/backtester.py), [metrics.py](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/metrics.py) | `VERIFIED` | Calculates IC, Rank IC, Sharpe, Drawdown over canonical 4-year walk-forward dataset partitions. |
