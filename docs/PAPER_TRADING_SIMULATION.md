# Real-Time Event-Driven Paper Trading & Market Replay Specification

## 1. Executive Summary & Objective

The **Real-Time Paper Trading Simulation & Market Replay Module** (Rubric M1-38, M1-39) provides an institutional-grade, event-driven paper execution engine and market replay controller. It bridges big data analytical serving (ClickHouse), quantitative alpha research (Alpha Registry & Signal Server), and real-time portfolio execution with strict ACID persistence (PostgreSQL / SQLite).

```mermaid
flowchart TD
    subgraph Data_Source["1. Market Data Feed"]
        CH["ClickHouse OLAP (quant_dw.market_intraday_bars)<br>split_id = 'TRADING' (1-minute OHLCV + VWAP)"]
        CH_Feat["ClickHouse Features (quant_dw.feat_technical_indicators)<br>Lagged T-1 Point-in-Time As-Of Join"]
        Kafka["Apache Kafka (market.bars.1m)<br>Optional Live Flink Stream"]
    end

    subgraph Replay_Controller["2. Replay Orchestrator & State Management"]
        RC["ReplayController (dashboard_service.py)<br>Background Thread | Pacing (1x - 50x) | Step-by-Step"]
        RS["ReplayState (Thread-safe Lock)<br>Session State, Telemetry, Signal Cache"]
        RC <--> RS
    end

    subgraph Signal_Engine["3. Alpha Signal Serving (alpha_serving.py)"]
        ASS["AlphaSignalServer"]
        Cat["Alpha Catalog (alpha_catalog.py)<br>10 Strategies | score_strategy_strict()"]
        Reg["Alpha Registry (alpha_registry.py)<br>APPROVED_FROZEN Ensemble or Single Alpha"]
        ASS --> Cat
        ASS --> Reg
    end

    subgraph Execution_Core["4. Paper Trading Engine (paper_trading.py)"]
        PTE["PaperTradingEngine.on_bar()"]
        PMC["PMControlService (pm_controls.py)<br>Emergency Block & Exposure Limits"]
        PTE --> PMC
        PTE --> ASS
    end

    subgraph Risk_Controls["5. Institutional Risk Governance"]
        SL["Stop-Loss: -12 bps (-0.12%)"]
        TP["Take-Profit: +20 bps (+0.20%)"]
        CB["Circuit Breaker: -2.0% Daily Loss"]
        EOD["EOD Flatting: 15:55 America/New_York"]
        CAP["Max Positions: 5 | Trade Notional: $10,000"]
    end

    subgraph Ledger_Storage["6. ACID Ledger & Persistence (paper_db.py)"]
        PDB["PaperDatabase (PostgreSQL / SQLite Fallback)"]
        T_Ord["paper_orders (Idempotency: uq_order_idempotency)"]
        T_Trd["paper_trades (Fills, Slippage 5 bps, Commission 1 bps)"]
        T_Pos["paper_positions (Mark-to-Market Real-Time)"]
        T_Snap["paper_portfolio_snapshots (Equity, Cash, Drawdown)"]
        T_Hist["paper_alpha_scores | paper_position_lifecycle | paper_position_marks"]
        PDB --> T_Ord
        PDB --> T_Trd
        PDB --> T_Pos
        PDB --> T_Snap
        PDB --> T_Hist
    end

    subgraph Presentation["7. Streamlit Quant Terminal (3_Real_Time_Trading.py)"]
        UI_Ctrl["Replay Controls: Start / Pause / Resume / Stop / Step"]
        UI_KPI["Live Portfolio KPIs: Cash, Equity, Realized & Unrealized PnL"]
        UI_Sig["Signals Matrix & Individual Alpha Breakdown"]
        UI_Chart["Dual-Panel Synchronized Plotly Charts<br>• Panel 1: Portfolio Equity Curve (Auto-Scaled)<br>• Panel 2: Realized & Unrealized PnL (Zero Baseline)"]
    end

    CH -->|Streaming TRADING Bars| RC
    CH_Feat -->|As-Of Features| RC
    Kafka -.->|Live Streaming Bars| RC
    RC -->|MarketBar Instance| PTE
    PTE --> Risk_Controls
    PTE -->|Transactions & Marks| PDB
    RS -.->|In-Memory Push| UI_KPI
    RS -.->|In-Memory Push| UI_Sig
    RS -.->|Equity History| UI_Chart
    PDB -.->|Historical Audit Query| UI_Ctrl
```

---

## 2. Core Execution Engine (`PaperTradingEngine`)

### 2.1 Dependencies & Initialization
Located at [`src/quant_edai/quant/paper_trading.py`](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/quant/paper_trading.py#L87-L187).

- **Dependencies**:
  - `PaperDatabase`: Data access layer for PostgreSQL with automatic SQLite fallback (`paper_db.py`).
  - `AlphaSignalServer`: Multi-alpha signal scoring and equal-average aggregation (`alpha_serving.py`).
  - `PMControlService`: Portfolio Manager governance checks and emergency circuit overrides (`pm_controls.py`).
  - `market_time`: Timezone utilities for Eastern Time (ET) session windows and EOD flat checks (`common/market_time.py`).
- **Institutional Execution Parameters**:
  - `starting_cash`: `$100,000.00` default portfolio cash.
  - `trade_notional`: Fixed `$10,000.00` notional position size.
  - `max_positions`: 5 concurrent open positions.
  - `slippage_bps`: `5.0 bps` ($0.05\%$) institutional spread and market impact.
  - `commission_bps`: `1.0 bps` ($0.01\%$) broker/exchange fee.
  - `tp_bps`: `+20.0 bps` ($+0.20\%$) Take-Profit target.
  - `sl_bps`: `-12.0 bps` ($-0.12\%$) Stop-Loss threshold.
  - `max_daily_loss_pct`: `-2.0%` daily portfolio circuit breaker.
  - `alpha_source`: `"store"` (pure `AlphaSignalServer`) or `"inline"` (local rolling buffers).

### 2.2 Complete `on_bar()` Sequence Flow

```mermaid
sequenceDiagram
    autonumber
    participant Worker as Replay Worker Loop
    participant PTE as PaperTradingEngine
    participant Mark as Mark-to-Market & Stops
    participant Risk as Risk & Circuit Breaker
    participant Alpha as AlphaSignalServer
    participant PDB as PaperDatabase (PostgreSQL)

    Worker->>PTE: on_bar(MarketBar)
    Note over PTE: Step 1: Update Cumulative VWAP & Parse Timestamp

    PTE->>Mark: Evaluate Open Position for Ticker
    alt Ticker has Open Position
        Mark->>Mark: Update mark price = close_price
        Mark->>Mark: Compute Unrealized PnL & %
        Mark->>PDB: upsert_position() & record_position_mark()
        alt close <= stop_loss_price OR close >= take_profit_price
            Mark->>PTE: _close_position(reason="STOP_LOSS"|"TAKE_PROFIT")
            PTE->>PDB: record_order() & record_trade() & close_position_lifecycle()
        end
    end

    Note over PTE: Step 2: Check EOD Flatting (15:55 America/New_York)
    opt dt_et == 15:55:00 ET
        PTE->>PTE: Liquidate all open positions (reason="EOD_FLAT")
        PTE->>PDB: Record closing trades & lifecycle updates
    end

    PTE->>Risk: Compute Portfolio Total Equity
    Risk->>Risk: total_equity = cash + sum(pos.mark * pos.qty)
    Risk->>Risk: daily_pnl_pct = (total_equity - daily_start_equity) / daily_start_equity
    opt daily_pnl_pct <= -2.0% (Circuit Breaker Tripped)
        Risk->>PTE: Liquidate all positions (reason="CIRCUIT_BREAKER") & daily_halted = True
    end

    Note over PTE: Step 3: Alpha Scoring & Entry Check
    opt Not EOD AND Not Daily Halted AND PM Not Blocked
        PTE->>Alpha: get_intraday_signal(ticker, close, features, alpha_ids)
        Alpha-->>PTE: Return TradeSignal (action, z_score, alpha_scores)
        opt Open Position flips/weakens (FLAT / opposite side)
            PTE->>PTE: _close_position(reason="SIGNAL_EXIT")
        end
        opt Capacity Available & Action == BUY / SELL & In Top-K
            PTE->>PTE: _open_position(side, price * (1 ± slippage))
            PTE->>PDB: record_order(), record_trade(), open_position_lifecycle()
        end
    end

    Note over PTE: Step 4: Record Snapshot & Latency Telemetry
    PTE->>PDB: record_snapshot(session_id, equity, cash, realized, unrealized, drawdown)
    PTE-->>Worker: Return on_bar Summary Result
```

---

## 3. Alpha Research & Signal Serving Stack

### 3.1 10-Alpha Strategy Catalog (`alpha_catalog.py`)
All alphas operate under standardized `FeatureContext` and output normalized `(z_score, confidence)`:
1. `alpha_mom_z`: OHLCV return rolling z-score momentum.
2. `alpha_vwap_z`: Close vs. VWAP deviation z-score.
3. `alpha_rsi_mr`: RSI-14 mean-reversion factor.
4. `alpha_macd_z`: MACD histogram z-score.
5. `alpha_sma_cross`: SMA 20/50 ratio trend factor.
6. `alpha_vol_break`: Price breakout vs. rolling volatility.
7. `alpha_volume_z`: Volume surge z-score.
8. `alpha_ml_prob`: Centered ML directional probability $(P_{\text{ML}} - 0.5) \times 4.0$.
9. `alpha_composite_eq`: Equal-weight blend of momentum, technical, and ML factors.
10. `alpha_regime_adapt`: Market regime-adaptive factor weighting (Bull / Bear / Sideway).

### 3.2 Dual Scoring Modes: Strict vs. Fallback
- `score_strategy_strict()`: Strictly evaluates real features and clean rolling buffers. Returns `(0.0, 0.0)` if inputs are missing; strictly **forbids** synthetic trigonometric imputation.
- `score_strategy()`: Fallback mode for smoke testing with graceful geometric imputation.

### 3.3 Multi-Alpha Combination: Equal Average (Rule 5)
For an ensemble of $K$ active alphas:
$$Z_{\text{combined}} = \frac{1}{K} \sum_{i=1}^{K} Z_i, \quad \text{Action} = \begin{cases} \text{BUY} & \text{if } Z_{\text{combined}} \ge \bar{Z}_{\text{entry}} \\ \text{SELL} & \text{if } Z_{\text{combined}} \le -\bar{Z}_{\text{entry}} \\ \text{FLAT} & \text{otherwise} \end{cases}$$

---

## 4. Replay Controller & Concurrency Management

Located at [`src/quant_edai/serving/dashboard/dashboard_service.py`](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/serving/dashboard/dashboard_service.py#L2285).

- **Execution Threading**: Runs in a dedicated background `threading.Thread(daemon=True)` to prevent blocking the Streamlit UI.
- **Pacing Mechanism**: Adjustable replay speed (1x, 5x, 10x, 25x, 50x) via `time.sleep(0.05 / speed)`.
- **Step-by-Step Execution**: While paused, pressing `Step` queues `pending_steps += 1`, allowing exactly one bar to process through the engine before pausing again.
- **Session Modes**:
  - **Approved Ensemble Mode**: Strictly validates `load_approved_replay_ensemble(dataset_id)` and freezes session metadata into `replay_session_ensemble`.
  - **Single Alpha Studio Mode**: Allows executing any standalone registered alpha with isolated session IDs.

---

## 5. PostgreSQL ACID Ledger & State Persistence

### 5.1 Tables & Schema Constraints
1. **`paper_orders`**:
   - Primary Key: `order_id VARCHAR(64)`.
   - Idempotency Guarantee: `CONSTRAINT uq_order_idempotency UNIQUE (session_id, ticker_symbol, bar_timestamp, direction, order_type)`.
2. **`paper_trades`**: Executed fills with `fill_price`, `slippage_bps`, `commission`, `realized_pnl`, `executed_at`, and ML provenance tags.
3. **`paper_positions`**: Current active mark-to-market positions keyed by `(session_id, ticker_symbol)`.
4. **`paper_portfolio_snapshots`**: Sequential portfolio valuation keyed by `snapshot_id BIGSERIAL`.
5. **Additive Audit Tables**:
   - `paper_alpha_scores`: Historical individual alpha scores per bar.
   - `paper_position_lifecycle`: End-to-end trade roundtrips with entry, peak open mark, exit reason, and net realized PnL.
   - `paper_position_marks`: Granular mark-to-market trajectory per open position.

---

## 6. Streamlit Visual Terminal: Dual-Panel Performance Charts

Located at [`src/quant_edai/serving/dashboard/pages/3_Real_Time_Trading.py`](file:///Users/nguyenbuihoanglong/Desktop/FSDS/edai_11/quant_edai/src/quant_edai/serving/dashboard/pages/3_Real_Time_Trading.py).

To eliminate the **Scale Mismatch** problem (where small PnL variations appear flat against $100,000 cash), the terminal uses a **TradingView / QuantConnect dual-panel stacked layout**:

```mermaid
graph TD
    subgraph Dual_Panel_Chart["TradingView / QuantConnect Dual-Panel Architecture"]
        subgraph Panel1["Panel 1: Portfolio Equity Curve ($) (62% Height)"]
            P1_Trace["Equity Line (TradingView Blue #2962FF)"]
            P1_Y["Independent Auto-Ranged Y-Axis: Zoomed to [$99,500 - $100,500]"]
        end
        subgraph Panel2["Panel 2: PnL Performance ($) (38% Height)"]
            P2_R["Realized PnL (Teal #26A69A)"]
            P2_U["Unrealized PnL (Orange #FF9800)"]
            P2_Zero["Zero Baseline: y=0 Dash Line"]
            P2_Y["Independent Auto-Ranged Y-Axis: Centered around $0"]
        end
        SharedX["Shared Time X-Axis & Synchronized Crosshair (hovermode='x unified')"]
        Panel1 --- SharedX
        Panel2 --- SharedX
    end
```

- **Synchronized Hover**: Crosshairs and tooltips move simultaneously across both panels.
- **Zero-Lag Telemetry**: In-memory `live_metrics` are pushed to the UI, while historical audit reconciliation reads PostgreSQL on demand.
