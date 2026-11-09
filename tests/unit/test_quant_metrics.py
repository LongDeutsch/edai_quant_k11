"""Unit tests for quantitative financial metrics in quant_edai.quant.metrics."""

import numpy as np
import pandas as pd
import pytest

from quant_edai.quant.metrics import (
    calculate_calmar,
    calculate_cumulative_returns,
    calculate_hit_rate,
    calculate_ic,
    calculate_ic_series,
    calculate_max_drawdown,
    calculate_rank_ic,
    calculate_rolling_sharpe,
    calculate_sharpe,
    calculate_sortino,
)


def test_calculate_ic_perfect_and_zero():
    # Perfect positive correlation
    x = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    y = np.array([2.0, 4.0, 6.0, 8.0, 10.0])
    assert pytest.approx(calculate_ic(x, y), 0.001) == 1.0

    # Perfect negative correlation
    assert pytest.approx(calculate_ic(x, -y), 0.001) == -1.0

    # Flat line / zero variance
    flat = np.array([1.0, 1.0, 1.0, 1.0, 1.0])
    assert calculate_ic(flat, y) == 0.0


def test_calculate_rank_ic_monotonic():
    # Monotonic non-linear relationship: Spearman is 1.0, Pearson is < 1.0
    x = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    y = np.exp(x)
    assert pytest.approx(calculate_rank_ic(x, y), 0.001) == 1.0
    assert calculate_ic(x, y) < 1.0

    # Opposite monotonic
    assert pytest.approx(calculate_rank_ic(x, -y), 0.001) == -1.0


def test_calculate_sharpe_daily_vs_intraday():
    np.random.seed(42)
    # 252 daily returns with positive mean 0.001 (0.1%/day) and daily vol 0.01 (1.0%/day)
    returns = np.random.normal(loc=0.001, scale=0.01, size=252)

    sharpe_daily = calculate_sharpe(returns, rf=0.04, periods_per_year=252)
    assert isinstance(sharpe_daily, float)
    assert sharpe_daily > 0.0

    # Verify scaling factor periods_per_year with rf=0.0:
    sharpe_daily_zero_rf = calculate_sharpe(returns, rf=0.0, periods_per_year=252)
    sharpe_1m_zero_rf = calculate_sharpe(returns, rf=0.0, periods_per_year=98280)
    # Scale difference is sqrt(98280) / sqrt(252) = sqrt(390) ≈ 19.748
    expected_ratio = np.sqrt(98280 / 252)
    assert pytest.approx(sharpe_1m_zero_rf / sharpe_daily_zero_rf, 0.001) == expected_ratio

    # Edge cases
    assert calculate_sharpe([]) == 0.0
    assert calculate_sharpe([0.01]) == 0.0
    assert calculate_sharpe([0.01, 0.01]) == 0.0  # zero variance


def test_calculate_sortino_and_calmar():
    # Symmetric vs purely positive returns
    positive_returns = pd.Series([0.01, 0.02, 0.015, 0.03, 0.02])
    # With all returns above risk-free rate, downside deviation is 0
    sortino_all_pos = calculate_sortino(positive_returns, rf=0.0, periods_per_year=252)
    assert sortino_all_pos == 0.0

    # Mixed returns with downside
    mixed = pd.Series([0.02, -0.01, 0.03, -0.02, 0.015, -0.005])
    sortino_mixed = calculate_sortino(mixed, rf=0.0, periods_per_year=252)
    assert sortino_mixed > 0.0

    # Calmar ratio
    calmar = calculate_calmar(cum_return=0.25, max_drawdown=-0.10, years=1.0)
    assert pytest.approx(calmar, 0.01) == 2.5
    assert calculate_calmar(0.25, 0.0, 1.0) == 0.0


def test_calculate_max_drawdown():
    # Peak at 1.2, drops to 0.9, then recovers to 1.5
    equity = pd.Series([1.0, 1.2, 1.08, 0.96, 0.90, 1.10, 1.30, 1.50])
    mdd, underwater = calculate_max_drawdown(equity)
    # Trough at 0.90 from peak of 1.20: (0.90 - 1.20) / 1.20 = -0.30 / 1.20 = -0.25 (-25%)
    assert pytest.approx(mdd, 0.001) == -0.25
    assert underwater.min() == mdd
    assert underwater.max() == 0.0

    # Monotonically increasing: MDD is 0.0
    mdd_up, _ = calculate_max_drawdown(pd.Series([1.0, 1.1, 1.2, 1.3]))
    assert mdd_up == 0.0


def test_calculate_hit_rate_and_cumulative():
    strat = pd.Series([0.01, -0.02, 0.03, 0.04, -0.01])
    # 3 positive out of 5 -> 60%
    assert pytest.approx(calculate_hit_rate(strat), 0.01) == 0.6

    bench = pd.Series([0.00, 0.01, 0.02, 0.01, 0.00])
    # Excess: [0.01, -0.03, 0.01, 0.03, -0.01] -> 3 positive -> 60%
    assert pytest.approx(calculate_hit_rate(strat, bench), 0.01) == 0.6

    # Cumulative equity
    cum = calculate_cumulative_returns(pd.Series([0.10, -0.10]))
    # 1.0 * 1.1 * 0.9 = 0.99
    assert pytest.approx(cum.iloc[-1], 0.001) == 0.99


def test_calculate_rolling_sharpe_and_ic_series():
    dates = pd.date_range("2024-01-01", periods=100)
    returns = pd.Series(np.random.normal(0.001, 0.01, 100), index=dates)
    rolling = calculate_rolling_sharpe(returns, window=20, rf=0.0)
    assert len(rolling) == 100
    assert not rolling.iloc[-1] == 0.0

    # IC series across dates and tickers
    df = pd.DataFrame({
        "date": ["2024-01-01"] * 5 + ["2024-01-02"] * 5,
        "ticker": ["A", "B", "C", "D", "E"] * 2,
        "signal": [1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 4.0, 3.0, 2.0, 1.0],
        "forward_return": [0.01, 0.02, 0.03, 0.04, 0.05, 0.05, 0.04, 0.03, 0.02, 0.01],
    })
    ic_df = calculate_ic_series(df, date_col="date", signal_col="signal", return_col="forward_return")
    assert len(ic_df) == 2
    assert pytest.approx(ic_df.iloc[0]["ic"], 0.001) == 1.0
    assert pytest.approx(ic_df.iloc[1]["ic"], 0.001) == 1.0
