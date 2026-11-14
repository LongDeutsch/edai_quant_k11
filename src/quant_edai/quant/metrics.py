"""Unified financial and quantitative metrics API for Alpha evaluation and paper trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union
import numpy as np
import pandas as pd
from scipy import stats

SHARPE_INSUFFICIENT = "insufficient_observations"
SHARPE_ZERO_VOLATILITY = "zero_volatility"

DISPLAY_NO_TRADES = "N/A — No trades"
DISPLAY_ZERO_VOLATILITY = "N/A — Zero volatility"
DISPLAY_INSUFFICIENT = "N/A — Insufficient observations"

# Regular US cash session is 6.5 hours.
_SESSION_MINUTES = 390
_BAR_MINUTES = {
    "1m": 1,
    "1min": 1,
    "5m": 5,
    "5min": 5,
    "10m": 10,
    "10min": 10,
    "15m": 15,
    "15min": 15,
}
_DAILY_TIMEFRAMES = {"d", "1d", "daily", "day", "eod"}


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


def periods_per_year(timeframe: str) -> int:
    """Bar count used to annualize one return series.

    Daily bars use 252 sessions. Intraday bars use a 6.5 hour session
    (390 minutes) times 252 sessions, so 5-minute bars are 78×252 and
    15-minute bars are 26×252.
    """
    tf = (timeframe or "").strip().lower()
    if tf in _DAILY_TIMEFRAMES:
        return 252
    minutes = _BAR_MINUTES.get(tf)
    if minutes is None:
        return 252
    return (_SESSION_MINUTES // minutes) * 252


def periodic_risk_free(rf: float, periods: int) -> float:
    """Convert an annual risk-free rate into the matching per-bar rate.

    The conversion is ``rf / periods``. Annualization multiplies by
    ``sqrt(periods)``, so the two factors stay paired.
    """
    if periods <= 0:
        raise ValueError("periods_per_year must be positive")
    return float(rf) / float(periods)


@dataclass(frozen=True)
class SharpeResult:
    """Annualized Sharpe, or an explicit reason the ratio is undefined."""

    value: Optional[float]
    reason: Optional[str] = None

    @property
    def defined(self) -> bool:
        return self.reason is None and self.value is not None and bool(np.isfinite(self.value))

    def rounded(self, ndigits: int = 2) -> Optional[float]:
        """Round a defined Sharpe. Undefined stays None and is never replaced with 0."""
        if not self.defined:
            return None
        return round(float(self.value), ndigits)


def format_sharpe(
    value: Optional[float],
    reason: Optional[str] = None,
    *,
    trade_count: Optional[int] = None,
    digits: int = 2,
) -> str:
    """Display a Sharpe ratio. A flat book and a zero-variance book stay distinct."""
    if trade_count == 0:
        return DISPLAY_NO_TRADES
    if reason == SHARPE_ZERO_VOLATILITY:
        return DISPLAY_ZERO_VOLATILITY
    if reason == SHARPE_INSUFFICIENT or value is None or not np.isfinite(value):
        return DISPLAY_INSUFFICIENT
    return f"{float(value):.{int(digits)}f}"


def finite_float(value: Any) -> Optional[float]:
    """Finite float, or None when the metric is missing or undefined."""
    if isinstance(value, SharpeResult):
        return float(value.value) if value.defined else None
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    return number


def persisted_sharpe_needs_recompute(sharpe: Any, total_trades: Any, total_return: Any) -> bool:
    """True when a stored Sharpe is the zero-variance numerical failure.

    Valid negative ratios and already-null values are left alone.
    """
    if sharpe is None:
        return False
    try:
        number = float(sharpe)
    except (TypeError, ValueError):
        return True
    if not np.isfinite(number) or abs(number) >= 1e6:
        return True
    trades = 0 if total_trades is None else int(total_trades)
    try:
        ret = 0.0 if total_return is None else float(total_return)
    except (TypeError, ValueError):
        ret = 0.0
    return trades == 0 and number == 0.0 and ret == 0.0


def stamp_recomputed_sharpe(
    metrics: Optional[Dict[str, Any]],
    result: "SharpeResult",
    *,
    prior: Any,
    recomputed_at: str,
) -> Dict[str, Any]:
    """Write the corrected Sharpe onto a stored metrics document.

    Run identity, dates, and every other key already on the document stay in place.
    """
    stamped = dict(metrics or {})
    stamped["sharpe_prior"] = finite_float(prior)
    stamped["sharpe_ratio"] = result.rounded(4)
    stamped["sharpe_reason"] = result.reason
    stamped["sharpe_recomputed_at"] = recomputed_at
    return stamped


def _finite_returns(returns: Union[pd.Series, np.ndarray]) -> np.ndarray:
    values = pd.Series(returns, dtype="float64").to_numpy(dtype=np.float64)
    return values[np.isfinite(values)]


def _excess_std(excess: np.ndarray) -> Tuple[float, Optional[str]]:
    """Sample standard deviation of excess returns.

    A constant series has variance zero. A long constant series can make a
    floating-point ``std`` return a tiny positive number; the range test treats
    that dust as zero variance. Values that truly differ, even by a very small
    amount, keep their standard deviation.
    """
    x = np.asarray(excess, dtype=np.float64)
    n = int(x.size)
    if n < 2:
        return float("nan"), SHARPE_INSUFFICIENT
    lo = float(np.min(x))
    hi = float(np.max(x))
    spread = hi - lo
    magnitude = max(abs(lo), abs(hi))
    ulp_span = float(np.finfo(np.float64).eps) * magnitude * n
    if magnitude == 0.0 or spread <= ulp_span:
        return 0.0, SHARPE_ZERO_VOLATILITY
    mean = float(np.mean(x))
    var = float(np.sum((x - mean) ** 2) / (n - 1))
    if not np.isfinite(var) or var <= 0.0:
        return 0.0, SHARPE_ZERO_VOLATILITY
    std = float(np.sqrt(var))
    if not np.isfinite(std) or std <= ulp_span:
        return 0.0, SHARPE_ZERO_VOLATILITY
    return std, None


def calculate_sharpe(
    returns: Union[pd.Series, np.ndarray],
    rf: float = 0.04,
    periods_per_year: int = 252,
) -> SharpeResult:
    """Annualized Sharpe ratio of periodic returns.

    Parameters
    ----------
    returns : pd.Series or np.ndarray
        Periodic portfolio returns. Cash interest is not added here.
    rf : float
        Annual risk-free rate (default 0.04). Subtracted as ``rf / periods_per_year``.
    periods_per_year : int
        Daily bars use 252. A 5-minute bar uses 78×252 and a 15-minute bar uses 26×252.
        One-minute bars use 390×252. Do not annualize an intraday series with 252.

    Returns
    -------
    SharpeResult
        ``value`` is the annualized ratio, including valid negative ratios.
        ``reason`` is ``insufficient_observations`` or ``zero_volatility`` when the
        ratio is undefined. Undefined is never returned as 0, and a large finite
        ratio is not clipped.
    """
    values = _finite_returns(returns)
    if values.size < 2:
        return SharpeResult(None, SHARPE_INSUFFICIENT)
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")

    excess = values - periodic_risk_free(rf, periods_per_year)
    std, reason = _excess_std(excess)
    if reason is not None:
        return SharpeResult(None, reason)

    sharpe = (float(np.mean(excess)) / std) * float(np.sqrt(periods_per_year))
    if not np.isfinite(sharpe):
        return SharpeResult(None, SHARPE_ZERO_VOLATILITY)
    return SharpeResult(float(sharpe), None)


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
        Rolling Sharpe series. Windows with fewer than two observations or with
        numerically zero variance are NaN, not 0.
    """
    s = pd.Series(returns, dtype="float64").dropna()
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    if len(s) < 2:
        return pd.Series(np.nan, index=s.index, dtype=float)

    excess = s.to_numpy(dtype=np.float64) - periodic_risk_free(rf, periods_per_year)
    out = np.full(len(excess), np.nan)
    min_obs = max(2, int(window) // 2)
    scale = float(np.sqrt(periods_per_year))
    for i in range(min_obs - 1, len(excess)):
        start = 0 if window <= 0 else max(0, i + 1 - int(window))
        chunk = excess[start:i + 1]
        std, reason = _excess_std(chunk)
        if reason is not None:
            continue
        out[i] = (float(np.mean(chunk)) / std) * scale
    return pd.Series(out, index=s.index)


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
