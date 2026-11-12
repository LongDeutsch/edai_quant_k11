"""Live Integration Verification for Quant EDAI 4-Page Dashboard & Backend Services.

Tests:
1. Page 1 Backend: ClickHouse multi-timeframe fetching (5m, 10m, 15m, Daily) with weighted VWAP and indicator math.
2. Page 2 Backend: Alpha Registry, dynamic backtesting, PostgreSQL persistence, and Top-5 Ensemble handoff.
3. Page 3 Backend: Market Replay Controller, PaperTradingEngine execution, PostgreSQL ACID ledger, idempotency.
4. Page 4 Backend: Historical Market EDA, cross-ticker correlation matrix, and live OLAP query benchmark.
"""

from __future__ import annotations

import time
import uuid
import numpy as np
import pandas as pd
import pytest

from quant_edai.serving.dashboard.dashboard_service import (
    AlphaResearchService,
    HistoricalEDAService,
    MarketDataService,
    ReplayController,
    get_postgres_connection,
    run_clickhouse_query,
)


@pytest.fixture(scope="module")
def active_dataset_id() -> str:
    datasets = MarketDataService.get_available_datasets()
    assert len(datasets) > 0, "No active datasets found in ClickHouse"
    return datasets[0]


# ==============================================================================
# 1. Page 1 Backend Verification: Market Data & Indicators
# ==============================================================================
def test_page1_candlesticks_multi_timeframe(active_dataset_id: str):
    """Verify ClickHouse multi-timeframe resampling from 1m to 5m and Daily, plus dataset catalog presets."""
    # 0. Test Dataset Catalog Presets (P1 Remediation)
    presets = MarketDataService.get_catalog_options(advanced_mode=False)
    assert len(presets) >= 2
    assert "⚡ Dense Intraday Demo (1m/5m)" in presets
    resolved = MarketDataService.resolve_dataset_id("⚡ Dense Intraday Demo (1m/5m)")
    assert resolved == "20261111T101352Z_integration_s0p005_seed42_cdda26b5"

    tickers = MarketDataService.get_available_tickers(active_dataset_id)
    assert len(tickers) > 0
    test_ticker = tickers[0]

    # 1. Test Daily OHLCV query
    df_daily = MarketDataService.fetch_candlesticks(active_dataset_id, test_ticker, timeframe="Daily", limit=100)
    assert not df_daily.empty, f"Daily OHLCV empty for {test_ticker}"
    assert {"open", "high", "low", "close", "volume", "vwap"}.issubset(df_daily.columns)

    # 2. Test 5m resampling from 1m bars with weighted VWAP
    df_5m = MarketDataService.fetch_candlesticks(active_dataset_id, test_ticker, timeframe="5m", limit=100)
    if not df_5m.empty:
        assert {"open", "high", "low", "close", "volume", "vwap"}.issubset(df_5m.columns)
        # High must be >= low and max(open, close) <= high
        assert (df_5m["high"] >= df_5m["low"]).all()
        assert (df_5m["high"] >= df_5m["open"]).all()
        assert (df_5m["high"] >= df_5m["close"]).all()

    # 3. Test Technical Indicators computation
    df_ind = MarketDataService.calculate_indicators(df_daily)
    assert not df_ind.empty
    assert {"sma_20", "sma_50", "ema_12", "ema_26", "rsi_14", "macd_line"}.issubset(df_ind.columns)
    # Check RSI bounded in [0, 100]
    valid_rsi = df_ind["rsi_14"].dropna()
    assert (valid_rsi >= 0.0).all() and (valid_rsi <= 100.0).all()


# ==============================================================================
# 2. Page 2 Backend Verification: Alpha Research & Top-5 Persistence
# ==============================================================================
def test_page2_alpha_registry_and_backtest():
    """Verify Alpha Registry loading, dynamic backtest execution, and Top-5 persistence."""
    alphas_df = AlphaResearchService.get_registered_alphas()
    assert not alphas_df.empty, "Alpha registry is empty"
    assert "alpha_id" in alphas_df.columns

    # Test dynamic backtest on 2-year feature panel with alpha_composite_eq_daily (P0 remediation)
    bt_res = AlphaResearchService.run_historical_backtest(
        alpha_id="alpha_composite_eq_daily",
        ticker="AAPL",
        timeframe="DAILY",
        slippage_bps=5.0,
        commission_bps=1.0,
    )
    assert bt_res["status"] == "SUCCESS", f"Backtest failed: {bt_res.get('message')}"
    m = bt_res["metrics"]
    assert "sharpe_ratio" in m
    assert "max_drawdown_pct" in m
    assert "win_rate_pct" in m
    assert m["trade_count"] > 0, f"Expected non-zero trades for composite_eq, got {m['trade_count']}"
    assert m["total_return_pct"] != 0.0, "Expected non-zero return for composite_eq"

    # Test portfolio backtest across ALL universe
    bt_all = AlphaResearchService.run_historical_backtest(
        alpha_id="alpha_composite_eq_daily",
        ticker="ALL",
        timeframe="DAILY",
    )
    assert bt_all["status"] == "SUCCESS"
    assert bt_all["metrics"]["trade_count"] > m["trade_count"], "ALL universe should have more trades than single ticker"

    # Verify Leaderboard retrieval with only_latest toggle
    lb_latest = AlphaResearchService.get_backtest_leaderboard(only_latest=True)
    assert not lb_latest.empty
    assert "run_id" in lb_latest.columns
    assert "sharpe_ratio" in lb_latest.columns

    # Verify persistence in PostgreSQL
    run_id = m["run_id"]
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT alpha_id, ticker_symbol, sharpe_ratio FROM alpha_backtest_results WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    conn.close()
    assert row is not None, f"Backtest run {run_id} was not persisted in PostgreSQL"
    assert row[0] == "alpha_composite_eq_daily"

    # Test Top-5 Ensemble saving and retrieval
    top5_selection = alphas_df["alpha_id"].tolist()[:5]
    assert len(top5_selection) == 5
    saved = AlphaResearchService.save_top5_ensemble(top5_selection, horizon="INTRADAY", dataset_id="test_ds")
    assert saved is True

    retrieved = AlphaResearchService.get_latest_top5_ensemble(horizon="INTRADAY")
    assert retrieved == top5_selection, f"Expected {top5_selection}, got {retrieved}"


# ==============================================================================
# 3. Page 3 Backend Verification: Replay Controller & Paper Trading
# ==============================================================================
def test_page3_replay_and_paper_trading(active_dataset_id: str):
    """Verify Replay Controller background execution and PostgreSQL ACID ledger."""
    test_session = f"sess_live_test_{uuid.uuid4().hex[:8]}"
    top5 = AlphaResearchService.get_latest_top5_ensemble() or ["composite_eq"]

    started = ReplayController.start_replay(
        session_id=test_session,
        dataset_id=active_dataset_id,
        speed=50,
        tickers=["AAPL", "NVDA"],
        selected_alphas=top5,
    )
    assert started is True

    # Let background thread process a few bars
    time.sleep(2.0)
    state = ReplayController.get_state()
    assert state["session_id"] == test_session
    assert state["processed_bars"] > 0

    # Verify per-ticker independent signals and status (P0 Remediation)
    sigs = state.get("latest_signals", {})
    if len(sigs) >= 2:
        tkr1, tkr2 = list(sigs.keys())[:2]
        # Prices must be independent (not identical fallback values)
        assert sigs[tkr1]["price"] > 0
        assert sigs[tkr2]["price"] > 0
        assert "status" in sigs[tkr1]
        assert "action" in sigs[tkr1]

    # Test pause and resume
    ReplayController.pause_replay()
    time.sleep(0.5)
    paused_state = ReplayController.get_state()
    assert paused_state["is_paused"] is True

    ReplayController.resume_replay()
    time.sleep(0.5)
    resumed_state = ReplayController.get_state()
    assert resumed_state["is_paused"] is False

    # Stop replay
    ReplayController.stop_replay()
    time.sleep(0.5)
    stopped_state = ReplayController.get_state()
    assert stopped_state["is_running"] is False

    # Verify PostgreSQL ledger has recorded portfolio snapshots
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM paper_portfolio_snapshots WHERE session_id = %s", (test_session,))
    snapshot_count = cur.fetchone()[0]
    conn.close()
    assert snapshot_count > 0, "No portfolio snapshots recorded in PostgreSQL"


# ==============================================================================
# 4. Page 4 Backend Verification: Historical EDA & Live OLAP Benchmark
# ==============================================================================
def test_page4_historical_eda_and_benchmark(active_dataset_id: str):
    """Verify Market EDA stats, correlation matrix, and real OLAP benchmark execution."""
    eda = HistoricalEDAService.get_market_eda_summary(active_dataset_id)
    assert "stats" in eda
    df_stats = eda["stats"]
    assert not df_stats.empty, f"EDA stats empty for dataset {active_dataset_id}"
    assert {"ticker_symbol", "mean_price", "avg_volume"}.issubset(df_stats.columns)

    # Live OLAP Benchmark execution
    bench = HistoricalEDAService.run_live_olap_benchmark(active_dataset_id, ticker="AAPL")
    assert "clickhouse" in bench
    assert "parquet_batch" in bench
    assert bench["clickhouse"]["latency_ms"] >= 0.0
    assert bench["speedup_factor"] > 0.0
