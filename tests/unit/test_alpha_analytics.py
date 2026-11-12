"""Unit tests for Alpha Quality Analytics and performance metric correctness."""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock

from quant_edai.quant.analytics_service import (
    AlphaAnalyticsService,
    IntradayPerformanceSummary,
)


def test_intraday_performance_closed_trades_only():
    """Verify that win rate, profit factor, and PnL are computed strictly on closed trades."""
    # Mock trades: 2 opening fills (PnL=0) and 4 closed exits
    mock_trades = pd.DataFrame([
        # Opening entry 1 (LONG)
        {"trade_id": "trd_open_1", "order_id": "ord_1", "direction": "BUY", "order_type": "MARKET", "realized_pnl": 0.0, "fill_price": 100.0, "fill_quantity": 10.0, "commission": 1.0, "slippage_bps": 5.0, "executed_at": "2024-03-15T09:30:00Z"},
        # Opening entry 2 (SHORT)
        {"trade_id": "trd_open_2", "order_id": "ord_2", "direction": "SELL", "order_type": "MARKET", "realized_pnl": 0.0, "fill_price": 200.0, "fill_quantity": 5.0, "commission": 1.0, "slippage_bps": 5.0, "executed_at": "2024-03-15T09:31:00Z"},
        # Closed exit 1: Win +$30.00
        {"trade_id": "trd_close_1", "order_id": "ord_3", "direction": "SELL", "order_type": "TAKE_PROFIT", "realized_pnl": 30.0, "fill_price": 103.0, "fill_quantity": 10.0, "commission": 1.0, "slippage_bps": 5.0, "executed_at": "2024-03-15T09:35:00Z"},
        # Closed exit 2: Win +$20.00
        {"trade_id": "trd_close_2", "order_id": "ord_4", "direction": "BUY", "order_type": "TAKE_PROFIT", "realized_pnl": 20.0, "fill_price": 196.0, "fill_quantity": 5.0, "commission": 1.0, "slippage_bps": 5.0, "executed_at": "2024-03-15T09:36:00Z"},
        # Closed exit 3: Loss -$15.00
        {"trade_id": "trd_close_3", "order_id": "ord_5", "direction": "SELL", "order_type": "STOP_LOSS", "realized_pnl": -15.0, "fill_price": 98.5, "fill_quantity": 10.0, "commission": 1.0, "slippage_bps": 5.0, "executed_at": "2024-03-15T09:40:00Z"},
        # Closed exit 4: Loss -$10.00
        {"trade_id": "trd_close_4", "order_id": "ord_6", "direction": "BUY", "order_type": "STOP_LOSS", "realized_pnl": -10.0, "fill_price": 202.0, "fill_quantity": 5.0, "commission": 1.0, "slippage_bps": 5.0, "executed_at": "2024-03-15T09:41:00Z"},
    ])

    mock_db = MagicMock()
    mock_db.get_trades_with_orders.return_value = mock_trades
    mock_db.get_snapshots.return_value = pd.DataFrame([
        {"recorded_at": "2024-03-15T09:30:00Z", "max_drawdown_pct": -0.005},
        {"recorded_at": "2024-03-15T09:45:00Z", "max_drawdown_pct": -0.008},
    ])

    service = AlphaAnalyticsService(paper_db=mock_db)
    summary, df_closed, df_snaps = service.compute_intraday_performance()

    # 1. Trade Counts
    assert summary.total_trades_count == 6
    assert summary.closed_trades_count == 4
    assert summary.open_trades_count == 2
    assert summary.winning_trades_count == 2
    assert summary.losing_trades_count == 2
    assert summary.breakeven_trades_count == 0

    # 2. Win Rate = 2 / 4 = 50.0% (NOT 2 / 6 = 33.3%)
    assert summary.win_rate_pct == 50.0

    # 3. PnL & Profit Factor
    # Gross Profit = 30 + 20 = 50.0
    # Gross Loss = 15 + 10 = 25.0
    # Profit Factor = 50.0 / 25.0 = 2.0
    assert summary.gross_profit == 50.0
    assert summary.gross_loss == 25.0
    assert summary.profit_factor == 2.0
    assert summary.net_realized_pnl == 25.0  # 50 - 25

    # 4. Payoff Ratio = Avg Win (25.0) / Avg Loss (12.5) = 2.0
    assert summary.avg_win == 25.0
    assert summary.avg_loss == 12.5
    assert summary.payoff_ratio == 2.0

    # 5. Commission total
    assert summary.total_commission == 6.0  # $1 per trade * 6 trades

    # 6. Drawdown
    assert summary.max_drawdown_pct == pytest.approx(-0.8, rel=1e-2)
    assert summary.circuit_breaker_triggered is False


def test_intraday_performance_empty_trades():
    """Verify safe degradation when no trades are available."""
    mock_db = MagicMock()
    mock_db.get_trades_with_orders.return_value = pd.DataFrame()
    mock_db.get_snapshots.return_value = pd.DataFrame()

    service = AlphaAnalyticsService(paper_db=mock_db)
    summary, df_closed, _ = service.compute_intraday_performance()

    assert summary.total_trades_count == 0
    assert summary.closed_trades_count == 0
    assert summary.win_rate_pct == 0.0
    assert summary.profit_factor == 0.0
    assert df_closed.empty


def test_intraday_performance_circuit_breaker_flag():
    """Verify circuit breaker trigger detection when drawdown exceeds -2.0%."""
    mock_trades = pd.DataFrame([
        {"trade_id": "trd_1", "order_id": "ord_1", "direction": "SELL", "order_type": "CIRCUIT_BREAKER", "realized_pnl": -250.0, "fill_price": 100.0, "fill_quantity": 10.0, "commission": 1.0, "slippage_bps": 5.0, "executed_at": "2024-03-15T09:30:00Z"},
    ])
    mock_db = MagicMock()
    mock_db.get_trades_with_orders.return_value = mock_trades
    mock_db.get_snapshots.return_value = pd.DataFrame([
        {"recorded_at": "2024-03-15T09:30:00Z", "max_drawdown_pct": -0.025},  # -2.5% <= -2.0%
    ])

    service = AlphaAnalyticsService(paper_db=mock_db)
    summary, _, _ = service.compute_intraday_performance()

    assert summary.circuit_breaker_triggered is True
    assert summary.max_drawdown_pct == pytest.approx(-2.5, rel=1e-2)


def test_phase5_backtest_data_contracts():
    """Verify that Phase 5 summary contains required metrics, series, and non-empty values."""
    service = AlphaAnalyticsService()
    data = service.load_phase5_backtest_data()

    if data is not None:
        assert "metrics" in data
        assert "series" in data
        assert "date_range" in data

        m = data["metrics"]
        required_metrics = [
            "ic_mean", "rank_ic_mean", "sharpe", "sortino",
            "calmar", "max_drawdown", "cum_return", "annualized_return",
            "spy_cum_return", "spy_sharpe", "excess_alpha_vs_spy"
        ]
        for metric in required_metrics:
            assert metric in m, f"Missing metric: {metric}"
            assert isinstance(m[metric], (int, float))

        # Series checks
        s = data["series"]
        assert "equity_curve" in s
        assert "ic_series" in s
        assert len(s["equity_curve"]) > 50


def test_regime_matrix_and_ablation():
    """Verify static factor regime matrix and ablation comparison consistency."""
    service = AlphaAnalyticsService()
    regimes = service.get_regime_weight_matrix()
    ablation = service.get_ablation_comparison_matrix()

    assert len(regimes) == 4
    assert "Low Volatility" in regimes["Market Regime"].values
    assert "Crisis / Shock" in regimes["Market Regime"].values

    assert len(ablation) == 4
    assert any("Regime-Adaptive + Risk Parity" in cfg for cfg in ablation["Configuration"])
