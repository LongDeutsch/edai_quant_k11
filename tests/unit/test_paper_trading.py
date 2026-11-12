"""Unit tests for Real-time Paper Trading Simulator (Rubric M1-38)."""

from datetime import datetime
import os
import numpy as np
import pytest

from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine


@pytest.fixture
def test_engine():
    db = PaperDatabase(sqlite_path=":memory:")
    db.init_db(reset=True)
    engine = PaperTradingEngine(
        db=db,
        starting_cash=100_000.0,
        trade_notional=10_000.0,
        max_positions=3,
        tp_bps=20.0,  # +20 bps = +0.20%
        sl_bps=12.0,  # -12 bps = -0.12%
        slippage_bps=5.0,
        commission_bps=1.0,
        alpha_source="inline",  # CI smoke: local buffer thresholds
        sl_pct=None,            # use explicit sl_bps above, not 5% override
    )
    return engine


def test_paper_trading_on_bar_and_latency(test_engine):
    engine = test_engine
    bar = MarketBar(
        ticker_symbol="AAPL",
        timestamp="2024-03-15T10:00:00Z",
        open_price=180.0,
        high_price=180.5,
        low_price=179.8,
        close_price=180.2,
        volume=1000.0,
        vwap=180.0,
        p_ml=0.55,
    )

    res = engine.on_bar(bar)
    assert res is not None
    assert res["ticker"] == "AAPL"
    assert res["equity"] > 0
    # Sub-50ms SLA check
    assert res["latency_ms"] < 50.0


def test_idempotency_duplicate_bar_handling(test_engine):
    engine = test_engine
    # Feed 20 bars with strong upward momentum to trigger entry
    for i in range(20):
        bar = MarketBar(
            ticker_symbol="NVDA",
            timestamp=f"2024-03-15T10:{i:02d}:00Z",
            open_price=100.0 + i * 0.1,
            high_price=100.5 + i * 0.1,
            low_price=99.9 + i * 0.1,
            close_price=100.4 + i * 0.1,
            volume=5000.0,
            vwap=100.0,
            p_ml=0.75,
        )
        engine.on_bar(bar)

    # Check that position was opened
    positions = engine.db.get_positions()
    trades_before = len(engine.db.get_recent_trades())
    cash_before = engine.cash

    # Re-send the exact same last bar (simulating stream replay / retry)
    last_bar = MarketBar(
        ticker_symbol="NVDA",
        timestamp="2024-03-15T10:19:00Z",
        open_price=101.9,
        high_price=102.4,
        low_price=101.8,
        close_price=102.3,
        volume=5000.0,
        vwap=100.0,
        p_ml=0.75,
    )
    engine.on_bar(last_bar)

    trades_after = len(engine.db.get_recent_trades())
    # No duplicate trade or double deduction
    assert trades_after == trades_before
    assert engine.cash == cash_before


def test_take_profit_and_stop_loss_in_bps(test_engine):
    engine = test_engine
    # 1. Warmup and enter LONG
    for i in range(20):
        engine.on_bar(MarketBar(
            ticker_symbol="MSFT",
            timestamp=f"2024-03-15T11:{i:02d}:00Z",
            open_price=400.0,
            high_price=402.0,
            low_price=399.0,
            close_price=401.5 + (0.1 * i),
            volume=2000.0,
            vwap=400.0,
            p_ml=0.80,
        ))

    assert "MSFT" in engine.positions
    entry_price = engine.positions["MSFT"].average_entry_price

    # 2. Price surges by +30 bps (surpassing +20 bps TP target)
    tp_price = entry_price * (1.0 + 0.0030)
    res = engine.on_bar(MarketBar(
        ticker_symbol="MSFT",
        timestamp="2024-03-15T11:21:00Z",
        open_price=tp_price,
        high_price=tp_price + 0.5,
        low_price=tp_price - 0.1,
        close_price=tp_price,
        volume=2000.0,
        vwap=400.0,
    ))

    # Position should be closed via TAKE_PROFIT
    assert "MSFT" not in engine.positions
    assert any("TP_LONG_MSFT" in ev for ev in res["events"])
    assert engine.realized_pnl_cumulative > 0.0


def test_eod_flatten_at_15_55(test_engine):
    engine = test_engine
    # Warmup and enter position
    for i in range(20):
        engine.on_bar(MarketBar(
            ticker_symbol="GOOGL",
            timestamp=f"2024-03-15T14:{i:02d}:00Z",
            open_price=150.0,
            high_price=151.0,
            low_price=149.5,
            close_price=150.5 + (0.05 * i),
            volume=1500.0,
            vwap=150.0,
            p_ml=0.70,
        ))

    assert "GOOGL" in engine.positions

    # Bar arrives at 15:55:00 EST
    res = engine.on_bar(MarketBar(
        ticker_symbol="GOOGL",
        timestamp="2024-03-15T15:55:00Z",
        open_price=151.5,
        high_price=152.0,
        low_price=151.0,
        close_price=151.5,
        volume=2000.0,
        vwap=150.0,
    ))

    # All positions liquidated for EOD flattening
    assert len(engine.positions) == 0
    assert any("EOD_FLAT_GOOGL" in ev for ev in res["events"])


def test_circuit_breaker_max_daily_drawdown(test_engine):
    engine = test_engine
    # Set starting equity and simulate a catastrophic intraday drop of -3.0%
    engine.daily_start_equity = 100_000.0
    engine.cash = 90_000.0  # $10k in active position

    # Force open position
    engine._open_position("TSLA", 200.0, "2024-03-15T10:00:00Z", "LONG")
    assert "TSLA" in engine.positions

    # TSLA crashes to 140.0 (-30% on position = -$3,000 loss on $100k equity = -3.0%)
    res = engine.on_bar(MarketBar(
        ticker_symbol="TSLA",
        timestamp="2024-03-15T10:05:00Z",
        open_price=140.0,
        high_price=141.0,
        low_price=139.0,
        close_price=140.0,
        volume=10000.0,
        vwap=190.0,
    ))

    # Circuit breaker should halt and close position
    assert engine.daily_halted is True
    assert len(engine.positions) == 0
    assert any("CIRCUIT_BREAKER_TSLA" in ev or "SL_LONG_TSLA" in ev for ev in res["events"])


def test_market_replay_runner(tmp_path):
    from quant_edai.quant.market_replay import MarketReplayRunner
    test_db = PaperDatabase(sqlite_path=str(tmp_path / "test_paper_trading.db"))
    runner = MarketReplayRunner(
        mode="replay",
        speed=1000.0,
        reset_db=True,
        max_bars=100,
        output_dir=str(tmp_path),
        db=test_db,
    )
    summary = runner.run()
    assert summary["execution"]["bars_processed"] == 100
    assert summary["latency_sla"]["sla_met"] is True
    assert os.path.exists(tmp_path / "paper_trading_execution.json")
    assert os.path.exists(tmp_path / "paper_trading_latency.json")

