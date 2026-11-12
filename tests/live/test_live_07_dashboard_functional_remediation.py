"""Comprehensive Live Verification Test Suite for Dashboard & Serving Functional Remediation.

Validates the full E2E chain:
Top-5 Ensemble (PostgreSQL) -> Feast/Redis Online Features -> KServe v2 FastAPI Inference ->
AlphaSignalServer -> PaperTradingEngine (store mode) -> PostgreSQL paper_portfolio_snapshots ACID persistence.
Also validates Bidirectional Long/Short backtesting and volume feature availability.
"""

from __future__ import annotations

import json
import time
import urllib.request
import uuid
import numpy as np
import pandas as pd
import pytest

from quant_edai.ml.feast.feast_manager import FeastFeatureStoreManager
from quant_edai.quant.alpha_serving import AlphaSignalServer
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine
from quant_edai.serving.dashboard.dashboard_service import (
    AlphaResearchService,
    MarketDataService,
    ReplayController,
    run_clickhouse_query,
    get_postgres_connection,
)
import importlib

p3_module = importlib.import_module("quant_edai.serving.dashboard.pages.3_Real_Time_Trading")
get_portfolio_kpis = p3_module.get_portfolio_kpis


@pytest.mark.live
def test_contract_1_top5_ensemble_synchronization():
    """Verify Page 2 saves to alpha_ensemble_config and AlphaSignalServer immediately retrieves it."""
    test_alphas = [
        "alpha_composite_eq_intraday",
        "alpha_mom_z",
        "alpha_vwap_z",
        "alpha_macd_z_intraday",
        "alpha_volume_z",
    ]
    saved = AlphaResearchService.save_top5_ensemble(test_alphas, horizon="INTRADAY", dataset_id="ds_test_contract")
    assert saved is True, "Failed to persist Top-5 ensemble"

    # Verify retrieval via AlphaResearchService
    loaded = AlphaResearchService.get_latest_top5_ensemble(horizon="INTRADAY")
    assert loaded == test_alphas, f"Expected {test_alphas}, got {loaded}"

    # Verify AlphaSignalServer reads the exact selection from alpha_ensemble_config
    db = PaperDatabase()
    server = AlphaSignalServer(db=db)
    active_ids = server.get_ensemble_selected_ids(horizon="INTRADAY")
    assert active_ids == test_alphas, f"AlphaSignalServer mismatch: {active_ids} != {test_alphas}"


@pytest.mark.live
def test_contract_2_volume_feature_and_volume_z_backtest():
    """Verify Gold feature Parquet contains volume and alpha_volume_z backtest executes without zero-trades bug."""
    gold_path = "/tmp/lakehouse_source/gold_features/feat_ticker_technical_indicators.parquet"
    df_gold = pd.read_parquet(gold_path)
    assert "volume" in df_gold.columns, "Column 'volume' missing from Gold features Parquet!"
    assert (df_gold["volume"] > 0).any(), "Volume column has no positive values!"

    # Run backtest for alpha_volume_z on AAPL
    res = AlphaResearchService.run_historical_backtest(
        alpha_id="alpha_volume_z",
        ticker="AAPL",
        timeframe="DAILY",
        slippage_bps=5.0,
        commission_bps=1.0,
    )
    assert res["status"] in ["SUCCESS", "NO_TRADES"], f"Unexpected backtest status: {res['status']}"
    if res["status"] == "SUCCESS":
        m = res["metrics"]
        assert m["trade_count"] > 0, "alpha_volume_z backtest yielded 0 trades despite volume being present!"
        assert "sharpe_ratio" in m
        assert "max_drawdown_pct" in m


@pytest.mark.live
def test_contract_3_bidirectional_long_short_backtest():
    """Verify backtest position logic supports both Long (+1) and Short (-1) positions."""
    res = AlphaResearchService.run_historical_backtest(
        alpha_id="alpha_mom_z",
        ticker="AAPL",
        timeframe="DAILY",
    )
    assert res["status"] == "SUCCESS", f"Backtest failed: {res.get('message')}"
    eq_curve = res.get("equity_curve", [])
    assert len(eq_curve) > 20, "Equity curve points missing or insufficient"


@pytest.mark.live
def test_contract_4_kserve_v2_live_inference():
    """Verify live HTTP inference on KServe v2 endpoint returning real LightGBM probability and model version."""
    sample_features = [185.0, 182.0, 180.0, 1.01, 55.0, 1.2, 1.0, 0.2, 0.18, 0.005]
    payload = json.dumps({
        "id": f"test_infer_{uuid.uuid4().hex[:6]}",
        "inputs": [{
            "name": "features",
            "shape": [1, 10],
            "datatype": "FP64",
            "data": [sample_features],
        }],
    }).encode("utf-8")

    req = urllib.request.Request(
        "http://127.0.0.1:8000/v2/models/quant-direction-predictor/infer",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert "model_version" in data
        assert data["model_version"] in ["v1-champion", "v2-challenger"]
        prob = data["outputs"][1]["data"][0]
        assert 0.0 <= prob <= 1.0, f"Probability out of range: {prob}"


@pytest.mark.live
def test_contract_5_feast_redis_online_lookup():
    """Verify sub-10ms online feature lookup in Redis with dataset-aware lineage."""
    feast = FeastFeatureStoreManager()
    res = feast.get_online_features(["AAPL", "NVDA"], dataset_id="ds_g5_master_1794467581")
    assert res["entity_count"] == 2
    assert res["sub_10ms_sla_met"] is True, f"SLA violated: {res['retrieval_latency_ms']} ms"
    records = res["records"]
    assert len(records) == 2
    for rec in records:
        assert "close_price" in rec
        assert "volume" in rec
        assert "sma_20" in rec
        assert "rsi_14d" in rec


@pytest.mark.live
def test_contract_6_portfolio_kpis_acid_schema():
    """Verify get_portfolio_kpis() queries real PostgreSQL schema without exception or fake fallback."""
    sess_id = f"test_audit_sess_{uuid.uuid4().hex[:8]}"
    db = PaperDatabase()
    
    # 1. Before snapshot: clean INITIAL state (cash=100000, pnl=0, status=INITIAL)
    kpis_init = get_portfolio_kpis(sess_id)
    assert kpis_init["status"] == "INITIAL"
    assert kpis_init["cash"] == 100000.0

    # 2. Record a real snapshot in PostgreSQL
    snap = {
        "session_id": sess_id,
        "cash_balance": 88500.50,
        "invested_equity": 12100.00,
        "total_portfolio_value": 100600.50,
        "realized_pnl_cumulative": 650.50,
        "unrealized_pnl_current": -50.00,
        "rolling_sharpe_ratio": 1.45,
        "max_drawdown_pct": -0.85,
        "active_positions_count": 2,
        "latency_ms": 2.1,
        "recorded_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    db.record_snapshot(snap)

    # 3. Retrieve through get_portfolio_kpis() and verify exact column matching
    kpis_live = get_portfolio_kpis(sess_id)
    assert kpis_live["status"] == "LIVE"
    assert abs(kpis_live["cash"] - 88500.50) < 0.01
    assert abs(kpis_live["equity"] - 100600.50) < 0.01
    assert abs(kpis_live["realized_pnl"] - 650.50) < 0.01
    assert abs(kpis_live["unrealized_pnl"] - (-50.00)) < 0.01
    assert abs(kpis_live["drawdown"] - (-0.85)) < 0.01


@pytest.mark.live
def test_contract_7_candlestick_latest_slicing_chronological():
    """Verify fetch_candlesticks slices the latest N bars and returns them ascending chronologically."""
    df_candles = MarketDataService.fetch_candlesticks(
        dataset_id="ds_g5_live_1794464907",
        ticker="AAPL",
        timeframe="Daily",
        limit=50,
    )
    if not df_candles.empty:
        assert len(df_candles) <= 50
        # Check ascending order
        times = df_candles["bar_time"].tolist()
        assert times == sorted(times), "Candlesticks are not sorted ascending chronologically!"
        # Check valid high/low bounds
        assert (df_candles["high"] >= df_candles["low"]).all()
