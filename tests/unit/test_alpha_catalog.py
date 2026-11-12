"""Unit tests for alpha catalog scorers and seed registry counts."""

from __future__ import annotations

import math

import pytest

from quant_edai.quant.alpha_catalog import (
    ALPHA_SPECS,
    FeatureContext,
    list_alpha_specs,
    score_row_features,
    score_strategy,
)
from quant_edai.quant.alpha_registry import AlphaRegistry
from quant_edai.quant.alpha_seed import run_seed
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine


def test_catalog_has_ten_strategies():
    specs = list_alpha_specs()
    assert len(specs) == 10
    assert len({s.strategy for s in specs}) == 10
    assert len(ALPHA_SPECS) == 10


@pytest.mark.parametrize("spec", ALPHA_SPECS, ids=lambda s: s.strategy)
def test_each_strategy_returns_finite_z(spec):
    ctx = FeatureContext(
        ticker="AAPL",
        price=180.0,
        p_ml=0.62,
        volume=50_000.0,
        features={
            "rsi_14d": 42.0,
            "macd_histogram": 0.25,
            "sma_ratio_20_50": 1.02,
            "intraday_return_1d": 0.01,
            "rolling_volatility_30d": 0.2,
        },
        mom_buf=[0.001] * 20 + [0.02],
        vwap_buf=[0.0] * 20 + [0.01],
        ml_buf=[0.05] * 20 + [0.12],
        volume_buf=[1000.0] * 20 + [5000.0],
        close_buf=[100.0 + i * 0.2 for i in range(25)],
    )
    z, conf = score_strategy(spec.strategy, ctx, spec.registry_config())
    assert math.isfinite(z)
    assert 0.1 <= conf <= 1.0


def test_score_row_features_entry_exit_thresholds():
    row = {
        "ticker_symbol": "MSFT",
        "close_price": 420.0,
        "rsi_14d": 30.0,
        "macd_histogram": 0.5,
        "sma_ratio_20_50": 1.03,
        "intraday_return_1d": 0.02,
        "p_ml": 0.7,
        "rolling_volatility_30d": 0.18,
    }
    z, conf = score_row_features("momentum_z", row, {"entry_z": 1.0, "exit_z": 0.25})
    assert math.isfinite(z)
    assert conf >= 0.1


def test_alpha_seed_registry_counts(tmp_path):
    db_path = tmp_path / "seed_alphas.db"
    result = run_seed(sqlite_path=str(db_path), promote=False)
    assert result["status"] in ("SUCCESS", "PASSED_SIMULATED")  # synthetic panel => SIMULATED
    assert result["registry"]["registry_rows"] == 20

    db = PaperDatabase(sqlite_path=str(db_path))
    db.init_db(reset=False)
    intraday = db.list_alphas(alpha_type="INTRADAY")
    daily = db.list_alphas(alpha_type="DAILY")
    assert len(intraday) >= 10
    assert len(daily) >= 10

    reg = AlphaRegistry(db=db)
    assert len(reg.get_active_alphas(horizon="INTRADAY", mode="paper")) >= 10
    assert len(reg.get_active_alphas(horizon="DAILY", mode="paper")) >= 10
    assert result["daily_signals"]["effective_date"] > result["daily_signals"]["asof_date"]


def test_signal_exit_and_sl_five_percent(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "sl.db"))
    db.init_db(reset=True)
    engine = PaperTradingEngine(
        db=db,
        starting_cash=100_000.0,
        trade_notional=10_000.0,
        alpha_source="store",
        sl_pct=0.05,
        z_entry_threshold=0.1,
    )
    assert engine.sl_bps == pytest.approx(500.0)

    # Force-open a LONG then hit stop-loss at -5%
    engine._open_position("AAPL", 100.0, "2024-03-15T10:00:00Z", side="LONG")
    assert "AAPL" in engine.positions
    sl = engine.positions["AAPL"].stop_loss_price
    assert sl == pytest.approx(95.0, rel=1e-3)

    bar_sl = MarketBar(
        ticker_symbol="AAPL",
        timestamp="2024-03-15T10:01:00Z",
        open_price=95.0,
        high_price=95.5,
        low_price=94.0,
        close_price=94.5,
        volume=1000.0,
        vwap=95.0,
        p_ml=0.4,
    )
    res = engine.on_bar(bar_sl)
    assert "AAPL" not in engine.positions
    assert any("SL_" in e for e in res["events"])
