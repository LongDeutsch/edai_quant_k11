"""Quant Alpha Evaluation, Factor Backtesting, and Trading Module."""

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
    format_sharpe,
    periods_per_year,
    SharpeResult,
)

__all__ = [
    "calculate_ic",
    "calculate_rank_ic",
    "calculate_sharpe",
    "calculate_sortino",
    "calculate_calmar",
    "calculate_max_drawdown",
    "calculate_hit_rate",
    "calculate_cumulative_returns",
    "calculate_rolling_sharpe",
    "calculate_ic_series",
    "format_sharpe",
    "periods_per_year",
    "SharpeResult",
]
