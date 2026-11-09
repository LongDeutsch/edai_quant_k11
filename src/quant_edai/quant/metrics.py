"""Unified financial and quantitative metrics API for Alpha evaluation and paper trading."""

from __future__ import annotations

from typing import Tuple, Union
import numpy as np
import pandas as pd
from scipy import stats


def calculate_ic(
    predicted_alpha: Union[pd.Series, np.ndarray],
    forward_returns: Union[pd.Series, np.ndarray],
) -> float:
    """Calculate Information Coefficient (Pearson correlation) between signal and forward return.

    Parameters
    ----------
    predicted_alpha : pd.Series or np.ndarray
        Signal score or predicted direction probability.
    forward_returns : pd.Series or np.ndarray
        Actual forward realized return (e.g. t+1 return).

    Returns
    -------
    float
        Pearson correlation coefficient in range [-1.0, 1.0]. Returns 0.0 if zero variance.
    """
    s_alpha = pd.Series(predicted_alpha).dropna()
    s_ret = pd.Series(forward_returns).dropna()
    common_idx = s_alpha.index.intersection(s_ret.index)
    if len(common_idx) < 3:
        return 0.0

    x = s_alpha.loc[common_idx].to_numpy(dtype=float)
    y = s_ret.loc[common_idx].to_numpy(dtype=float)

    std_x = np.std(x, ddof=1)
    std_y = np.std(y, ddof=1)
    if std_x == 0.0 or std_y == 0.0 or np.isnan(std_x) or np.isnan(std_y):
        return 0.0

    r, _ = stats.pearsonr(x, y)
    return float(0.0 if np.isnan(r) else r)


def calculate_rank_ic(
    predicted_alpha: Union[pd.Series, np.ndarray],
    forward_returns: Union[pd.Series, np.ndarray],
) -> float:
    """Calculate Rank Information Coefficient (Spearman rank correlation).

    Measures monotonic relationship across cross-sectional equity scores.

    Parameters
    ----------
    predicted_alpha : pd.Series or np.ndarray
        Signal scores.
    forward_returns : pd.Series or np.ndarray
        Actual forward realized returns.

    Returns
    -------
    float
        Spearman correlation coefficient in range [-1.0, 1.0].
    """
    s_alpha = pd.Series(predicted_alpha).dropna()
    s_ret = pd.Series(forward_returns).dropna()
    common_idx = s_alpha.index.intersection(s_ret.index)
    if len(common_idx) < 3:
        return 0.0

    x = s_alpha.loc[common_idx].to_numpy(dtype=float)
    y = s_ret.loc[common_idx].to_numpy(dtype=float)

    if np.all(x == x[0]) or np.all(y == y[0]):
        return 0.0

    rho, _ = stats.spearmanr(x, y)
    return float(0.0 if np.isnan(rho) else rho)


def calculate_sharpe(
    returns: Union[pd.Series, np.ndarray],
    rf: float = 0.04,
    periods_per_year: int = 252,
) -> float:
    """Calculate Annualized Sharpe Ratio with explicit scaling.

    Parameters
    ----------
    returns : pd.Series or np.ndarray
        Periodic returns (e.g. daily EOD returns, or 1-minute intraday returns).
    rf : float
        Annual risk-free rate (default 0.04 = 4.0%).
    periods_per_year : int
        Number of periods per year for annualization:
        - Daily EOD backtesting (Phase 5): periods_per_year = 252 (scale = sqrt(252) ≈ 15.87)
        - 1-minute intraday scalping (Phase 6): periods_per_year = 252 * 390 = 98,280 (scale ≈ 313.50)
        *Important*: Phase 6 intraday MUST NOT blindly reuse sqrt(252) on 1m bar returns.

    Returns
    -------
    float
        Annualized Sharpe ratio.
    """
    s = pd.Series(returns).dropna()
    if len(s) < 2:
        return 0.0

    periodic_rf = rf / periods_per_year
    excess_returns = s - periodic_rf

    std = excess_returns.std(ddof=1)
    if std == 0.0 or np.isnan(std):
        return 0.0

    mean_excess = excess_returns.mean()
    sharpe = (mean_excess / std) * np.sqrt(periods_per_year)
    return float(0.0 if np.isnan(sharpe) else sharpe)


def calculate_sortino(
    returns: Union[pd.Series, np.ndarray],
    rf: float = 0.04,
    periods_per_year: int = 252,
) -> float:
    """Calculate Annualized Sortino Ratio penalizing downside volatility only.

    Parameters
    ----------
    returns : pd.Series or np.ndarray
        Periodic returns.
    rf : float
        Annual risk-free rate.
    periods_per_year : int
        Annualization periods.

    Returns
    -------
    float
        Annualized Sortino ratio.
    """
    s = pd.Series(returns).dropna()
    if len(s) < 2:
        return 0.0

    periodic_rf = rf / periods_per_year
    excess_returns = s - periodic_rf

    downside = np.minimum(0.0, excess_returns.to_numpy(dtype=float))
    downside_variance = np.mean(downside**2)
    if downside_variance <= 0.0:
        return 0.0

    downside_std = np.sqrt(downside_variance)
    mean_excess = excess_returns.mean()
    sortino = (mean_excess / downside_std) * np.sqrt(periods_per_year)
    return float(0.0 if np.isnan(sortino) else sortino)


def calculate_max_drawdown(
    equity_series: Union[pd.Series, np.ndarray],
) -> Tuple[float, pd.Series]:
    """Calculate Maximum Drawdown and full drawdown underwater series.

    Parameters
    ----------
    equity_series : pd.Series or np.ndarray
        Cumulative portfolio equity curve (e.g. starting at 1.0).

    Returns
    -------
    tuple of (float, pd.Series)
        - Scalar Maximum Drawdown (e.g. -0.118 representing an 11.8% peak-to-trough drop).
        - Drawdown underwater series (values <= 0.0) matching index of equity_series.
    """
    s = pd.Series(equity_series).dropna()
    if len(s) == 0:
        return 0.0, pd.Series(dtype=float)

    running_max = s.cummax()
    drawdowns = (s - running_max) / running_max
    # Fill any potential NaNs or division by zero
    drawdowns = drawdowns.fillna(0.0)
    mdd = float(drawdowns.min())
    return mdd, drawdowns


def calculate_calmar(
    cum_return: float,
    max_drawdown: float,
    years: float = 1.0,
) -> float:
    """Calculate Calmar Ratio: Annualized Return / |Max Drawdown|.

    Parameters
    ----------
    cum_return : float
        Cumulative return over the period (e.g. 0.286 for +28.6%).
    max_drawdown : float
        Maximum drawdown (negative value, e.g. -0.118).
    years : float
        Total duration in years.

    Returns
    -------
    float
        Calmar ratio.
    """
    abs_mdd = abs(max_drawdown)
    if abs_mdd == 0.0 or years <= 0.0:
        return 0.0

    ann_return = ((1.0 + cum_return) ** (1.0 / years)) - 1.0
    return float(ann_return / abs_mdd)


def calculate_hit_rate(
    strategy_returns: Union[pd.Series, np.ndarray],
    benchmark_returns: Union[pd.Series, np.ndarray, None] = None,
) -> float:
    """Calculate hit rate (percentage of winning periods or excess wins over benchmark).

    Parameters
    ----------
    strategy_returns : pd.Series or np.ndarray
        Periodic returns of strategy.
    benchmark_returns : pd.Series or np.ndarray, optional
        Benchmark returns for excess win evaluation.

    Returns
    -------
    float
        Hit rate in range [0.0, 1.0].
    """
    s = pd.Series(strategy_returns).dropna()
    if len(s) == 0:
        return 0.0

    if benchmark_returns is None:
        return float((s > 0).mean())

    b = pd.Series(benchmark_returns).dropna()
    common_idx = s.index.intersection(b.index)
    if len(common_idx) == 0:
        return float((s > 0).mean())

    excess = s.loc[common_idx] - b.loc[common_idx]
    return float((excess > 0).mean())


def calculate_cumulative_returns(
    returns: Union[pd.Series, np.ndarray],
    initial_equity: float = 1.0,
) -> pd.Series:
    """Calculate cumulative equity series from periodic returns.

    Parameters
    ----------
    returns : pd.Series or np.ndarray
        Periodic returns (e.g. 0.01 for 1%).
    initial_equity : float
        Starting portfolio capital factor (default 1.0).

    Returns
    -------
    pd.Series
        Equity curve over time.
    """
    s = pd.Series(returns).fillna(0.0)
    return initial_equity * (1.0 + s).cumprod()


def calculate_rolling_sharpe(
    returns: Union[pd.Series, np.ndarray],
    window: int = 63,
    rf: float = 0.04,
    periods_per_year: int = 252,
) -> pd.Series:
    """Calculate rolling annualized Sharpe ratio.

    Parameters
    ----------
    returns : pd.Series or np.ndarray
        Periodic returns.
    window : int
        Rolling window size in periods (default 63 trading days ≈ 3 months).
    rf : float
        Annual risk-free rate.
    periods_per_year : int
        Annualization periods.

    Returns
    -------
    pd.Series
        Rolling Sharpe series.
    """
    s = pd.Series(returns).dropna()
    if len(s) < window:
        return pd.Series(dtype=float, index=s.index)

    periodic_rf = rf / periods_per_year
    excess = s - periodic_rf

    rolling_mean = excess.rolling(window=window, min_periods=window // 2).mean()
    rolling_std = excess.rolling(window=window, min_periods=window // 2).std(ddof=1)

    rolling_sharpe = (rolling_mean / rolling_std) * np.sqrt(periods_per_year)
    return rolling_sharpe.fillna(0.0)


def calculate_ic_series(
    df: pd.DataFrame,
    date_col: str,
    signal_col: str,
    return_col: str,
) -> pd.DataFrame:
    """Calculate daily cross-sectional IC and Rank IC series.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing multiple equities across dates.
    date_col : str
        Name of date column.
    signal_col : str
        Name of predicted alpha signal column.
    return_col : str
        Name of realized forward return column.

    Returns
    -------
    pd.DataFrame
        Table with columns [date_col, 'ic', 'rank_ic'].
    """
    records = []
    for dt, group in df.groupby(date_col):
        clean_group = group[[signal_col, return_col]].dropna()
        if len(clean_group) >= 3:
            ic = calculate_ic(clean_group[signal_col], clean_group[return_col])
            rank_ic = calculate_rank_ic(clean_group[signal_col], clean_group[return_col])
            records.append({date_col: dt, "ic": ic, "rank_ic": rank_ic})
        else:
            records.append({date_col: dt, "ic": 0.0, "rank_ic": 0.0})

    return pd.DataFrame(records)
