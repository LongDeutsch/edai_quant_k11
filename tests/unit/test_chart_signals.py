"""Candle window, strict alpha signals, and dashboard replay contracts."""

from __future__ import annotations

import inspect

import numpy as np
import pandas as pd

from quant_edai.quant.alpha_catalog import FeatureContext, action_from_z, score_strategy_strict
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine
from quant_edai.quant.alpha_serving import AlphaSignalServer
from quant_edai.serving.dashboard.dashboard_service import (
    DATASET_CATALOG,
    MarketDataService,
    ReplayController,
    trading_replay_bars_sql,
)
from quant_edai.serving.dashboard.market_chart import build_chart_payload


def _preset(split_id: str) -> str:
    return next(name for name, spec in DATASET_CATALOG.items() if spec.get("split_id") == split_id)


def test_daily_marker_bound_includes_intraday_fill_on_last_candle():
    candles = pd.DataFrame({"bar_time": [pd.Timestamp("2026-05-06", tz="UTC")]})
    start, end = MarketDataService.candle_marker_bounds(candles, "Daily")
    fill = pd.Timestamp("2026-05-06 14:21:00", tz="UTC")
    assert start <= fill <= end
    assert start == pd.Timestamp("2026-05-06 00:00:00", tz="UTC")
    assert end >= pd.Timestamp("2026-05-06 23:59:59", tz="UTC")


def test_trading_replay_window_covers_may_2026_fills():
    fill_min = pd.Timestamp("2026-05-01 15:08:00", tz="UTC")
    fill_max = pd.Timestamp("2026-05-06 14:21:00", tz="UTC")
    start, end = MarketDataService.resolve_candle_window(_preset("TRADING"), "Daily", fill_min, fill_max)
    assert start == "2026-05-01"
    assert end == "2026-05-06"
    assert start <= "2026-05-01"
    assert end >= "2026-05-06"


def test_is_window_stays_on_catalog_range_when_fills_exist():
    start, end = MarketDataService.resolve_candle_window(
        _preset("IS"),
        "5m",
        pd.Timestamp("2026-05-01", tz="UTC"),
        pd.Timestamp("2026-05-06", tz="UTC"),
    )
    assert (start, end) == ("2022-11-01", "2025-04-30")


def test_trading_replay_without_fills_uses_catalog_range():
    start, end = MarketDataService.resolve_candle_window(_preset("TRADING"), "Daily", None, None)
    assert (start, end) == ("2026-05-01", "2026-10-31")


def test_strict_scorer_missing_feature_is_flat():
    missing = FeatureContext(ticker="AAPL", price=180.0, features={})
    z_score, confidence = score_strategy_strict("rsi_mr", missing, {})
    assert z_score == 0.0
    assert confidence == 0.0
    assert action_from_z(z_score, 0.5, 0.2) == "FLAT"

    momentum = score_strategy_strict("momentum_z", missing, {})
    assert momentum == (0.0, 0.0)
    assert action_from_z(momentum[0], 0.5, 0.2) == "FLAT"


def test_strict_scorer_uses_real_rsi():
    ctx = FeatureContext(ticker="AAPL", price=180.0, features={"rsi_14d": 20.0})
    z_score, _ = score_strategy_strict("rsi_mr", ctx, {})
    assert z_score == 3.0
    assert action_from_z(z_score, 0.5, 0.2) == "BUY"


def test_score_candle_frame_aligns_actions_to_bars():
    candles = pd.DataFrame({
        "bar_time": pd.date_range("2026-05-01", periods=6, freq="D", tz="UTC"),
        "time_label": [f"d{i}" for i in range(6)],
        "open": [100.0] * 6,
        "high": [101.0] * 6,
        "low": [99.0] * 6,
        "close": [100.0, 101.0, 102.0, 103.0, 104.0, 90.0],
        "volume": [1_000_000.0] * 6,
        "rsi_14": [50.0, 50.0, 50.0, 50.0, 20.0, 80.0],
    })
    scored = MarketDataService.score_candle_frame(
        candles,
        strategy="rsi_mr",
        config={"entry_z": 0.5, "exit_z": 0.2},
        ticker="AAPL",
    )
    assert list(scored["time_label"]) == list(candles["time_label"])
    assert scored.loc[4, "action"] == "BUY"
    assert scored.loc[5, "action"] == "SELL"
    assert set(scored["action"]).issubset({"BUY", "SELL", "FLAT"})


def test_chart_window_is_not_capped_and_payload_keeps_every_bar():
    assert MarketDataService.chart_bar_limit("10m", 300) is None
    assert MarketDataService.chart_bar_limit("Daily", 300) is None
    candles = pd.DataFrame({
        "time_label": [f"2026/05/{day:02d}" for day in range(1, 21)],
        "open": range(20),
        "high": range(1, 21),
        "low": range(20),
        "close": range(20),
        "volume": range(20),
    })
    payload = build_chart_payload(candles, ticker="AAPL", timeframe="Daily")
    assert len(payload["bars"]) == 20


def test_signal_book_earns_the_next_bar_and_stays_flat_without_a_breach():
    scored = pd.DataFrame({
        "z_score": [0.0, 3.0, 3.0, 0.0, 0.0],
        "close": [100.0, 100.0, 110.0, 121.0, 121.0],
        "high": [100.0, 101.0, 112.0, 122.0, 121.0],
        "low": [99.0, 99.0, 108.0, 119.0, 120.0],
    })
    book = MarketDataService.simulate_scored_book(
        scored, entry_z=0.5, exit_z=0.2, booksize=100_000.0, timeframe="Daily", cost_bps=6.0,
    )
    assert MarketDataService.positions_from_z(scored["z_score"].tolist(), 0.5, 0.2) == [0, 0, 1, 1, 0]
    assert book["pnl"] > 0
    assert book["trade_count"] == 1
    assert [(m["index"], m["action"]) for m in book["markers"]] == [(1, "BUY")]
    assert book["booksize"] == 100_000.0

    flat = MarketDataService.simulate_scored_book(
        pd.DataFrame({"z_score": [0.1, -0.1, 0.0], "close": [10.0, 11.0, 9.0], "high": [10, 11, 9], "low": [10, 11, 9]}),
        entry_z=0.5,
        exit_z=0.2,
        booksize=100_000.0,
        timeframe="10m",
    )
    assert flat["trade_count"] == 0
    assert flat["pnl"] == 0.0
    assert flat["trading_pnl"] == 0.0
    assert flat["cash_interest"] == 0.0
    assert flat["final_equity"] == 100_000.0
    assert flat["sharpe"] is None
    assert flat["sharpe_reason"] == "zero_volatility"
    assert flat["markers"] == []
    assert MarketDataService.bars_per_year("10m") == 39 * 252
    assert MarketDataService.bars_per_year("5m") == 78 * 252
    assert MarketDataService.bars_per_year("15m") == 26 * 252
    assert MarketDataService.bars_per_year("Daily") == 252

    n = 50_000
    long_flat = MarketDataService.simulate_scored_book(
        pd.DataFrame({
            "z_score": np.zeros(n),
            "close": np.linspace(100.0, 180.0, n),
            "high": np.linspace(101.0, 181.0, n),
            "low": np.linspace(99.0, 179.0, n),
        }),
        entry_z=0.5,
        exit_z=0.2,
        booksize=100_000.0,
        timeframe="5m",
    )
    assert long_flat["trade_count"] == 0
    assert long_flat["pnl"] == 0.0
    assert long_flat["cash_interest"] == 0.0
    assert long_flat["max_drawdown"] == 0.0
    assert long_flat["final_equity"] == 100_000.0
    assert long_flat["sharpe"] is None
    assert long_flat["sharpe_reason"] == "zero_volatility"
    assert book["sharpe_reason"] is None
    assert np.isfinite(book["sharpe"])


def test_trading_replay_sql_has_no_cap_and_worker_skips_mock():
    sql = trading_replay_bars_sql("ds_canonical_4y_eval_s05", ["AAPL", "NVDA"])
    assert "LIMIT" not in sql.upper()
    assert "split_id = 'TRADING'" in sql
    source = inspect.getsource(ReplayController._worker_loop)
    assert "mock_kserve_v2" not in source
    assert "LIMIT 500" not in source
    assert "get_online_features" not in source
    assert "trading_replay_bars_sql" in source


def test_store_replay_stamps_selected_alpha_on_fill(monkeypatch, tmp_path):
    monkeypatch.setenv("USE_SQLITE_ONLY", "1")
    db = PaperDatabase(sqlite_path=str(tmp_path / "paper.db"))
    db.init_db(reset=True)
    db.upsert_alpha({
        "alpha_id": "alpha_rsi_mr_intraday",
        "version": "v1",
        "alpha_type": "INTRADAY",
        "status": "PAPER_TRIAL",
        "description": "unit",
        "config_json": {"strategy": "rsi_mr", "entry_z": 0.5, "exit_z": 0.2},
        "metrics_snapshot": {},
        "model_version": "champion-unit",
        "dataset_id": "ds_unit",
        "feature_source": "unit_features",
    })
    server = AlphaSignalServer(db=db)
    engine = PaperTradingEngine(
        session_id="sess_alpha_stamp",
        db=db,
        alpha_source="store",
        signal_server=server,
        dataset_id="ds_unit",
        feature_source="unit_features",
        model_version="champion-unit",
        z_entry_threshold=0.5,
    )
    engine.signal_alpha_ids = ["alpha_rsi_mr_intraday"]
    engine.signal_strict = True
    engine.bar_features = {"rsi_14d": 20.0}
    engine.on_bar(MarketBar(
        ticker_symbol="AAPL",
        timestamp="2026-05-04T14:30:00Z",
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.0,
        volume=1000.0,
        vwap=100.0,
        model_version="champion-unit",
        dataset_id="ds_unit",
        feature_source="unit_features",
    ))
    trades = pd.read_sql("SELECT alpha_id, ticker_symbol, direction FROM paper_trades", db._sqlite_conn)
    assert len(trades) == 1
    assert trades.iloc[0]["ticker_symbol"] == "AAPL"
    assert trades.iloc[0]["alpha_id"] == "alpha_rsi_mr_intraday"
    assert trades.iloc[0]["direction"] == "BUY"
