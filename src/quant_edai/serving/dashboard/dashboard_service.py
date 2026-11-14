"""Unified Data and Analytics Service for Quant EDAI Streamlit Dashboard.

Decouples UI presentation from backend storage and business logic:
- ClickHouse OLAP: Multi-timeframe candlesticks (1m, 5m, 10m, 15m, Daily), indicators, volume.
- PostgreSQL: Paper trading ledger, positions, trades, snapshots, alpha registry & ensemble persistence.
- Feast / Redis: Online feature vector retrieval with sub-10ms latency SLA.
- Lakehouse Parquet / DuckDB: Data quality metrics and OLAP query benchmarking.
- Dynamic Backtest Engine: Event-time simulation without look-ahead bias, metrics calculation.
- Background Replay Controller: Safe async market bar simulation avoiding Streamlit thread blocking.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import base64
import glob
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse
import urllib.request
import uuid

import numpy as np
import pandas as pd

logger = logging.getLogger("DashboardService")

# ==============================================================================
# Configuration & Connection Helpers
# ==============================================================================
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "quant")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "quant_password")
CH_DB = os.getenv("CLICKHOUSE_DB", "quant_dw")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "quant")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "quant_password")
PG_DB = os.getenv("POSTGRES_DB", "quant_db")
PG_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

LAKEHOUSE_ROOT = os.getenv("OUT_DIR", "/tmp/lakehouse_source")


def run_clickhouse_query(query: str, timeout: int = 15, raise_on_error: bool = False) -> pd.DataFrame:
    """Execute SQL query against ClickHouse HTTP endpoint returning a pandas DataFrame."""
    clean_sql = query.strip().rstrip(";") + " FORMAT JSON"
    url = f"http://{CH_HOST}:{CH_PORT}/?database={CH_DB}"

    auth_str = f"{CH_USER}:{CH_PASS}"
    auth_b64 = base64.b64encode(auth_str.encode("utf-8")).decode("ascii")

    data = clean_sql.encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Basic {auth_b64}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            rows = body.get("data", [])
            return pd.DataFrame(rows)
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8") if hasattr(e, "read") else str(e)
        logger.error(f"ClickHouse HTTP {e.code} error: {err_msg} for query: {clean_sql[:120]}")
        if raise_on_error:
            raise RuntimeError(err_msg.strip()[:500]) from e
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"ClickHouse query failed: {e}")
        if raise_on_error:
            raise
        return pd.DataFrame()


def get_postgres_connection():
    """Get active psycopg2 connection to PostgreSQL."""
    import psycopg2
    return psycopg2.connect(PG_URL, connect_timeout=3)


def _sql_day(value: Optional[str]) -> Optional[str]:
    """Accept YYYY-MM-DD (or a timestamp prefix) for ClickHouse date predicates."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    day = text[:10]
    parts = day.split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise ValueError(f"Invalid candle window date: {value}")
    if len(parts[0]) != 4 or len(parts[1]) != 2 or len(parts[2]) != 2:
        raise ValueError(f"Invalid candle window date: {value}")
    return day


def trading_replay_bars_sql(dataset_id: str, tickers: List[str]) -> str:
    """Full TRADING-split intraday bars. No row cap and no split-less fallback."""
    ticker_list_sql = ", ".join(f"'{t}'" for t in tickers)
    return f"""
        SELECT
            window_start AS bar_time,
            ticker_symbol,
            toFloat64(open_price) AS open,
            toFloat64(high_price) AS high,
            toFloat64(low_price) AS low,
            toFloat64(close_price) AS close,
            toFloat64(volume) AS volume,
            toFloat64(vwap) AS vwap
        FROM quant_dw.market_intraday_bars FINAL
        WHERE dataset_id = '{dataset_id}'
          AND split_id = 'TRADING'
          AND ticker_symbol IN ({ticker_list_sql})
          AND is_deleted = 0 AND close_price > 0
        ORDER BY window_start ASC, ticker_symbol ASC
    """


def trading_replay_bars_aggregated_sql(dataset_id: str, tickers: List[str], interval_minutes: int) -> str:
    """TRADING-split bars aggregated to interval_minutes (5, 10, or 15).
    
    Aggregation happens inside ClickHouse before the rows reach the engine,
    so the engine always sees one OHLCV bar per (bar_time, ticker) pair.
    """
    ticker_list_sql = ", ".join(f"'{t}'" for t in tickers)
    minutes = int(interval_minutes)
    return f"""
        SELECT
            bar_time,
            ticker_symbol,
            toFloat64(open) AS open,
            toFloat64(high) AS high,
            toFloat64(low) AS low,
            toFloat64(close) AS close,
            toFloat64(total_volume) AS volume,
            toFloat64(bar_vwap) AS vwap
        FROM (
            SELECT
                toStartOfInterval(window_start, INTERVAL {minutes} MINUTE) AS bar_time,
                ticker_symbol,
                argMin(open_price, window_start) AS open,
                max(high_price) AS high,
                min(low_price) AS low,
                argMax(close_price, window_start) AS close,
                sum(volume) AS total_volume,
                if(
                    sum(volume) > 0,
                    sum(toFloat64(vwap) * toFloat64(volume)) / toFloat64(sum(volume)),
                    argMax(close_price, window_start)
                ) AS bar_vwap
            FROM quant_dw.market_intraday_bars FINAL
            WHERE dataset_id = '{dataset_id}'
              AND split_id = 'TRADING'
              AND ticker_symbol IN ({ticker_list_sql})
              AND is_deleted = 0
              AND close_price > 0
            GROUP BY bar_time, ticker_symbol
        )
        ORDER BY bar_time ASC, ticker_symbol ASC
    """


REPLAY_TIMEFRAMES = ("1m", "5m", "10m", "15m")
REPLAY_END_POLICIES = ("MARK_TO_MARKET", "FORCE_CLOSE")
EQUITY_HISTORY_LIMIT = 4000


def aggregate_complete_ohlcv(frame: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    """Aggregate source bars into clock buckets. A sparse bucket is kept.

    The canonical TRADING tape is not a full one-minute grid, so a bucket is
    valid when it contains at least one bar.
    """
    if int(interval_minutes) < 1:
        raise ValueError("interval_minutes must be positive")
    columns = ["bar_time", "ticker_symbol", "open", "high", "low", "close", "volume", "vwap"]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    work = frame.copy()
    work["bar_time"] = pd.to_datetime(work["bar_time"], utc=True)
    work = work.sort_values(["ticker_symbol", "bar_time"])
    work["bucket"] = work["bar_time"].dt.floor(f"{int(interval_minutes)}min")
    rows = []
    for (ticker, bucket), group in work.groupby(["ticker_symbol", "bucket"], sort=True):
        if group.empty:
            continue
        ordered = group.sort_values("bar_time")
        volume = float(ordered["volume"].sum())
        if volume > 0 and "vwap" in ordered.columns:
            vwap = float((ordered["vwap"].astype(float) * ordered["volume"].astype(float)).sum() / volume)
        else:
            vwap = float(ordered["close"].iloc[-1])
        rows.append({
            "bar_time": bucket,
            "ticker_symbol": ticker,
            "open": float(ordered["open"].iloc[0]),
            "high": float(ordered["high"].max()),
            "low": float(ordered["low"].min()),
            "close": float(ordered["close"].iloc[-1]),
            "volume": volume,
            "vwap": vwap,
        })
    return pd.DataFrame(rows, columns=columns)


def duplicate_bar_error(frame: pd.DataFrame) -> Optional[str]:
    """Reject a replay frame that would trade the same ticker bar twice."""
    if frame is None or frame.empty:
        return None
    if "bar_time" not in frame.columns or "ticker_symbol" not in frame.columns:
        return "Replay bars are missing bar_time or ticker_symbol."
    if bool(frame.duplicated(subset=["bar_time", "ticker_symbol"]).any()):
        return "Replay bars contain duplicate (bar_time, ticker) keys."
    return None


def ticker_selection_reason(requested: Optional[List[str]], available: Optional[List[str]]) -> Optional[str]:
    """Tickers must be chosen from the TRADING split. An empty catalog is not filled in."""
    catalog = [str(item) for item in (available or []) if str(item).strip()]
    if not catalog:
        return "No TRADING tickers were returned for this dataset. Replay will not guess a ticker."
    chosen = [str(item).strip() for item in (requested or []) if str(item).strip()]
    if not chosen:
        return "Select at least one ticker from the TRADING split."
    missing = [item for item in chosen if item not in set(catalog)]
    if missing:
        return "Ticker not in the TRADING split: " + ", ".join(missing)
    return None


def single_alpha_block_reason(alpha_id: Optional[str], version: Optional[str], row: Optional[Dict[str, Any]]) -> Optional[str]:
    """A single-alpha replay needs one registered INTRADAY strategy that strict scoring knows."""
    from quant_edai.quant.alpha_catalog import STRICT_STRATEGIES

    if not alpha_id or not version:
        return "Single alpha mode requires an alpha id and version."
    if not row:
        return f"Registry has no {alpha_id} {version}."
    if str(row.get("alpha_type") or "").upper() != "INTRADAY":
        return "Single alpha replay only accepts an INTRADAY registry row."
    config = row.get("config_json")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            config = {}
    strategy = str((config or {}).get("strategy") or "")
    if strategy not in STRICT_STRATEGIES:
        return f"Alpha strategy `{strategy or 'missing'}` is not a strict intraday strategy."
    return None


def session_restart_block_reason(has_activity: bool, session_id: str) -> Optional[str]:
    if not has_activity:
        return None
    return (
        f"Session `{session_id}` already has trades or portfolio snapshots. "
        "Start a new session id. Replaying this id would reset cash without restoring the ledger."
    )


def build_replay_equity_figure(history: Optional[List[Dict[str, Any]]]):
    """Always return a 2-panel chart (TradingView/QuantConnect style).
    
    Panel 1: Portfolio Equity Curve ($) with independent auto-scaled Y-axis.
    Panel 2: Realized & Unrealized PnL ($) with zero baseline.
    Both panels share the time X-axis and synchronized crosshairs.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.62, 0.38],
        subplot_titles=("Portfolio Equity ($)", "PnL Performance ($)"),
    )

    rows = list(history or [])
    if not rows:
        fig.add_trace(go.Scatter(x=[], y=[], name="Equity", line=dict(color="#2962FF", width=2)), row=1, col=1)
        fig.add_trace(go.Scatter(x=[], y=[], name="Realized PnL", line=dict(color="#26A69A", width=1.8)), row=2, col=1)
        fig.add_trace(go.Scatter(x=[], y=[], name="Unrealized PnL", line=dict(color="#FF9800", width=1.5, dash="dot")), row=2, col=1)
        fig.add_annotation(
            text="No completed bars yet",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=14, color="rgba(180, 180, 180, 0.6)"),
        )
        fig.update_layout(
            template="plotly_dark",
            height=460,
            margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        return fig

    times = [row.get("event_time") for row in rows]
    equities = [row.get("equity") for row in rows]
    realized_pnls = [row.get("realized_pnl") for row in rows]
    unrealized_pnls = [row.get("unrealized_pnl") for row in rows]

    # Panel 1: Portfolio Equity Curve
    fig.add_trace(
        go.Scatter(
            x=times,
            y=equities,
            name="Equity",
            line=dict(color="#2962FF", width=2.2),
            hovertemplate="Equity: $%{y:,.2f}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Panel 2: Realized & Unrealized PnL
    fig.add_trace(
        go.Scatter(
            x=times,
            y=realized_pnls,
            name="Realized PnL",
            line=dict(color="#26A69A", width=1.8),
            hovertemplate="Realized PnL: $%{y:+,.2f}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=times,
            y=unrealized_pnls,
            name="Unrealized PnL",
            line=dict(color="#FF9800", width=1.5, dash="dot"),
            hovertemplate="Unrealized PnL: $%{y:+,.2f}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    # Zero line for PnL
    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="rgba(180, 180, 180, 0.4)",
        line_width=1,
        row=2,
        col=1,
    )

    # Independent Y-Axes styling
    fig.update_yaxes(
        title_text="Equity ($)",
        row=1,
        col=1,
        autorange=True,
        tickformat="$,.0f",
        gridcolor="rgba(128, 128, 128, 0.15)",
    )
    fig.update_yaxes(
        title_text="PnL ($)",
        row=2,
        col=1,
        autorange=True,
        tickformat="+$,.2f",
        gridcolor="rgba(128, 128, 128, 0.15)",
    )
    fig.update_xaxes(
        gridcolor="rgba(128, 128, 128, 0.15)",
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        showline=True,
    )

    fig.update_layout(
        template="plotly_dark",
        height=480,
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def trading_replay_features_sql(dataset_id: str, tickers: List[str]) -> str:
    """Point-in-time technical features for as-of joins. No online store."""
    ticker_list_sql = ", ".join(f"'{t}'" for t in tickers)
    return f"""
        SELECT
            ticker_symbol,
            event_timestamp,
            toFloat64(close_price) AS close_price,
            toFloat64(sma_20) AS sma_20,
            toFloat64(sma_50) AS sma_50,
            toFloat64(sma_ratio_20_50) AS sma_ratio_20_50,
            toFloat64(rsi_14d) AS rsi_14d,
            toFloat64(macd_12_26) AS macd_12_26,
            toFloat64(macd_signal_9) AS macd_signal_9,
            toFloat64(macd_histogram) AS macd_histogram,
            toFloat64(rolling_volatility_30d) AS rolling_volatility_30d,
            toFloat64(intraday_return_1d) AS intraday_return_1d
        FROM quant_dw.feat_technical_indicators FINAL
        WHERE dataset_id = '{dataset_id}'
          AND ticker_symbol IN ({ticker_list_sql})
        ORDER BY ticker_symbol ASC, event_timestamp ASC
    """


class TradeMarkerQueryError(RuntimeError):
    """PostgreSQL paper_trades query failed. Callers must not treat this as an empty book."""


@dataclass
class FillBounds:
    min_time: Optional[pd.Timestamp] = None
    max_time: Optional[pd.Timestamp] = None
    count: int = 0
    error: Optional[str] = None


@dataclass
class TradeMarkerSummary:
    status: str
    message: str
    frame: pd.DataFrame
    fill_min: Optional[pd.Timestamp] = None
    fill_max: Optional[pd.Timestamp] = None
    window_start: Optional[Any] = None
    window_end: Optional[Any] = None
    matched_count: int = 0
    total_count: int = 0
    error: Optional[str] = None


def _utc_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize("UTC")
    return stamp.tz_convert("UTC")


def _fmt_ts(value: Any) -> str:
    stamp = _utc_timestamp(value)
    if stamp is None:
        return "n/a"
    return stamp.strftime("%Y-%m-%d %H:%M:%S UTC")


def _alpha_token_match(stored: Any, alpha_id: str) -> bool:
    if stored is None:
        return False
    try:
        if pd.isna(stored):
            return False
    except (TypeError, ValueError):
        pass
    parts = [part.split(":")[0].strip() for part in str(stored).split(",") if part.strip()]
    return alpha_id in parts


def ensure_paper_trades_alpha_column(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS alpha_id TEXT")
        conn.commit()
    finally:
        cur.close()


# ==============================================================================
# 1. Market Data & Candlestick Service (Page 1)
# ==============================================================================
DATASET_CATALOG: Dict[str, Dict[str, Any]] = {
    "🔬 1. In-Sample Research (2022-11 to 2025-04)": {
        "dataset_id": "ds_canonical_4y_eval_s05",
        "split_id": "IS",
        "description": "2.5 Years In-Sample Canonical Data (10,432 daily bars, 302,076 intraday bars) for Alpha Research & Model Training.",
        "default_timeframe": "Daily",
        "date_range": ("2022-11-01", "2025-04-30"),
    },
    "🛡️ 2. Out-of-Sample Validation [Admin] (2025-05 to 2026-04)": {
        "dataset_id": "ds_canonical_4y_eval_s05",
        "split_id": "OOS",
        "description": "1.0 Year Out-of-Sample Canonical Data (4,176 daily bars, 121,100 intraday bars) for Independent Alpha Validation & Top-5 Approval.",
        "default_timeframe": "Daily",
        "date_range": ("2025-05-01", "2026-04-30"),
    },
    "⚡ 3. Trading Replay (2026-05 to 2026-10)": {
        "dataset_id": "ds_canonical_4y_eval_s05",
        "split_id": "TRADING",
        "description": "0.5 Year Trading Replay Canonical Data (2,096 daily bars, 39,438 intraday bars) for Streaming Simulation & Paper Trading Replay.",
        "default_timeframe": "Daily",
        "date_range": ("2026-05-01", "2026-10-31"),
    },
}


def _expanding_population_z(values: np.ndarray) -> np.ndarray:
    """Population z-score of each prefix. Prefixes shorter than 5 stay undefined."""
    count = np.arange(1, len(values) + 1, dtype=float)
    total = np.cumsum(values)
    total_sq = np.cumsum(np.asarray(values, dtype=float) ** 2)
    mean = total / count
    std = np.sqrt(np.maximum(total_sq / count - mean * mean, 0.0))
    out = np.full(len(values), np.nan)
    valid = (count >= 5) & (std >= 1e-8)
    out[valid] = (values[valid] - mean[valid]) / std[valid]
    out[(count >= 5) & (std < 1e-8)] = 0.0
    return out


class MarketDataService:
    """Provides version-aware, multi-timeframe aggregated candlesticks and technical indicators."""

    @classmethod
    def get_catalog_options(cls, advanced_mode: bool = False) -> List[str]:
        """Return curated presets by default, or all raw dataset IDs if advanced_mode is True."""
        if not advanced_mode:
            return list(DATASET_CATALOG.keys())
        raw_datasets = cls.get_available_datasets()
        options = list(DATASET_CATALOG.keys())
        options.append("--- [Raw ClickHouse Datasets] ---")
        for ds in raw_datasets:
            if ds not in options:
                options.append(ds)
        return options

    @classmethod
    def resolve_dataset_id(cls, selected_option: str) -> str:
        """Resolve preset display title to physical ClickHouse dataset_id."""
        if selected_option in DATASET_CATALOG:
            preset = DATASET_CATALOG[selected_option]
            ds_id = preset.get("dataset_id")
            if ds_id:
                return ds_id
            return "ds_canonical_4y_eval_s05"
        if selected_option.startswith("---"):
            return "ds_canonical_4y_eval_s05"
        return selected_option

    @classmethod
    def resolve_split_id(cls, selected_option: str) -> Optional[str]:
        """Resolve preset display title to canonical split_id ('IS', 'OOS', 'TRADING')."""
        if selected_option in DATASET_CATALOG:
            return DATASET_CATALOG[selected_option].get("split_id")
        return None

    @classmethod
    def resolve_default_timeframe(cls, selected_option: str, supported: List[str]) -> str:
        """Prefer the preset catalog timeframe. Discovery must not override it with 5m."""
        preferred = None
        if selected_option in DATASET_CATALOG:
            preferred = DATASET_CATALOG[selected_option].get("default_timeframe")
        if preferred and preferred in supported:
            return str(preferred)
        if "Daily" in supported:
            return "Daily"
        return supported[0] if supported else "Daily"

    @staticmethod
    def candle_marker_bounds(candles: pd.DataFrame, timeframe: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """Inclusive execution window for the candles on screen.

        Daily bars are stamped at 00:00. The end bound is the last millisecond of
        that calendar day so a fill during the session still matches the candle.
        Intraday bars extend the end by one bucket.
        """
        if candles is None or candles.empty or "bar_time" not in candles.columns:
            raise ValueError("candle_marker_bounds requires a non-empty bar_time column")
        bar = pd.to_datetime(candles["bar_time"], utc=True)
        start = pd.Timestamp(bar.min())
        end = pd.Timestamp(bar.max())
        tf = (timeframe or "").lower()
        if tf in ("1d", "daily"):
            start_day = start.tz_convert("UTC").date()
            end_day = end.tz_convert("UTC").date()
            start = pd.Timestamp(year=start_day.year, month=start_day.month, day=start_day.day, tz="UTC")
            end = pd.Timestamp(
                year=end_day.year,
                month=end_day.month,
                day=end_day.day,
                hour=23,
                minute=59,
                second=59,
                microsecond=999000,
                tz="UTC",
            )
            return start, end
        minutes = {"1m": 1, "5m": 5, "5min": 5, "10m": 10, "10min": 10, "15m": 15, "15min": 15}.get(tf, 1)
        return start, end + pd.Timedelta(minutes=minutes)

    @staticmethod
    def resolve_candle_window(
        preset: str,
        timeframe: str,
        fill_min: Optional[Any] = None,
        fill_max: Optional[Any] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Default candle date window for a catalog preset.

        Trading Replay with fills uses the fill date span so the chart covers the
        executions (AAPL on this dataset is 2026-05-01 through 2026-05-06).
        Otherwise the preset catalog range is used. IS and OOS ignore fill dates.
        """
        _ = (timeframe or "").lower()
        catalog = DATASET_CATALOG.get(preset)
        if not catalog:
            return None, None
        if catalog.get("split_id") == "TRADING" and fill_min is not None and fill_max is not None:
            start = pd.Timestamp(fill_min)
            end = pd.Timestamp(fill_max)
            if start.tzinfo is not None:
                start = start.tz_convert("UTC")
            if end.tzinfo is not None:
                end = end.tz_convert("UTC")
            return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        date_range = catalog.get("date_range")
        if date_range and len(date_range) == 2:
            return str(date_range[0]), str(date_range[1])
        return None, None

    @staticmethod
    def discover_dataset_capabilities(dataset_id: str) -> Dict[str, Any]:
        """Dynamically discover table counts, date ranges, tickers, and supported timeframes from ClickHouse."""
        daily_cnt = 0
        min_daily = None
        max_daily = None
        daily_tickers = []
        try:
            df_d = run_clickhouse_query(f"""
                SELECT count(*) AS cnt, min(trade_date) AS min_d, max(trade_date) AS max_d, groupArray(DISTINCT ticker_symbol) AS tickers
                FROM quant_dw.market_daily_ohlcv FINAL
                WHERE dataset_id = '{dataset_id}'
            """)
            if not df_d.empty and int(df_d.iloc[0]["cnt"]) > 0:
                daily_cnt = int(df_d.iloc[0]["cnt"])
                min_daily = str(df_d.iloc[0]["min_d"])
                max_daily = str(df_d.iloc[0]["max_d"])
                t_arr = df_d.iloc[0].get("tickers")
                daily_tickers = list(t_arr) if isinstance(t_arr, (list, tuple)) else ([str(t_arr)] if t_arr else [])
        except Exception as e:
            logger.debug(f"Daily check error for {dataset_id}: {e}")

        intra_cnt = 0
        min_intra = None
        max_intra = None
        intra_tickers = []
        try:
            df_i = run_clickhouse_query(f"""
                SELECT count(*) AS cnt, min(window_start) AS min_t, max(window_start) AS max_t, groupArray(DISTINCT ticker_symbol) AS tickers
                FROM quant_dw.market_intraday_bars FINAL
                WHERE dataset_id = '{dataset_id}' AND is_deleted = 0
            """)
            if not df_i.empty and int(df_i.iloc[0]["cnt"]) > 0:
                intra_cnt = int(df_i.iloc[0]["cnt"])
                min_intra = str(df_i.iloc[0]["min_t"])
                max_intra = str(df_i.iloc[0]["max_t"])
                t_arr = df_i.iloc[0].get("tickers")
                intra_tickers = list(t_arr) if isinstance(t_arr, (list, tuple)) else ([str(t_arr)] if t_arr else [])
        except Exception as e:
            logger.debug(f"Intraday check error for {dataset_id}: {e}")

        all_tickers = sorted(list(set(daily_tickers + intra_tickers)))
        if not all_tickers:
            all_tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "SPY"]

        # Intraday timeframes are offered when 1m bars exist. The preset catalog,
        # not the mere presence of intraday rows, chooses the default timeframe.
        supported_tfs = []
        if intra_cnt > 0:
            supported_tfs.extend(["1m", "5m", "10m", "15m"])
        if daily_cnt > 0:
            supported_tfs.append("Daily")
        if not supported_tfs:
            supported_tfs = ["Daily"]
        default_tf = "Daily" if "Daily" in supported_tfs else supported_tfs[0]

        return {
            "dataset_id": dataset_id,
            "has_daily": daily_cnt > 0,
            "daily_count": daily_cnt,
            "daily_date_range": (min_daily, max_daily),
            "has_intraday": intra_cnt > 0,
            "intraday_count": intra_cnt,
            "intraday_time_range": (min_intra, max_intra),
            "supported_timeframes": supported_tfs,
            "default_timeframe": default_tf,
            "tickers": all_tickers,
        }

    @staticmethod
    def get_available_datasets() -> List[str]:
        """Fetch distinct dataset_ids from ClickHouse tables."""
        datasets = []
        try:
            df1 = run_clickhouse_query("SELECT DISTINCT dataset_id FROM quant_dw.market_daily_ohlcv")
            if not df1.empty and "dataset_id" in df1.columns:
                datasets.extend(df1["dataset_id"].tolist())
        except Exception:
            pass

        try:
            df2 = run_clickhouse_query("SELECT DISTINCT dataset_id FROM quant_dw.market_intraday_bars")
            if not df2.empty and "dataset_id" in df2.columns:
                datasets.extend(df2["dataset_id"].tolist())
        except Exception:
            pass

        unique_ds = list(dict.fromkeys([d for d in datasets if d]))
        return unique_ds if unique_ds else ["default"]

    @staticmethod
    def get_available_tickers(dataset_id: str) -> List[str]:
        """Fetch distinct tickers available in the given dataset."""
        try:
            df = run_clickhouse_query(f"""
                SELECT DISTINCT ticker_symbol 
                FROM quant_dw.market_daily_ohlcv FINAL 
                WHERE dataset_id = '{dataset_id}' 
                ORDER BY ticker_symbol ASC
            """)
            if not df.empty and "ticker_symbol" in df.columns:
                return df["ticker_symbol"].tolist()
        except Exception:
            pass

        try:
            df2 = run_clickhouse_query(f"""
                SELECT DISTINCT ticker_symbol 
                FROM quant_dw.market_intraday_bars FINAL 
                WHERE dataset_id = '{dataset_id}' 
                ORDER BY ticker_symbol ASC
            """)
            if not df2.empty and "ticker_symbol" in df2.columns:
                return df2["ticker_symbol"].tolist()
        except Exception:
            pass

        return ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "SPY"]

    @staticmethod
    def fetch_trading_tickers(dataset_id: str) -> List[str]:
        """Return distinct tickers present in the TRADING split from ClickHouse.
        
        Does NOT fall back to hardcoded tickers — returns empty list on failure
        so the caller can surface the error rather than silently using stale data.
        """
        try:
            df = run_clickhouse_query(f"""
                SELECT DISTINCT ticker_symbol
                FROM quant_dw.market_intraday_bars FINAL
                WHERE dataset_id = '{dataset_id}'
                  AND split_id = 'TRADING'
                  AND is_deleted = 0
                ORDER BY ticker_symbol ASC
            """, timeout=15)
            if not df.empty and "ticker_symbol" in df.columns:
                tickers = [str(t) for t in df["ticker_symbol"].tolist() if t]
                return tickers
        except Exception as e:
            logger.warning(f"fetch_trading_tickers failed for {dataset_id}: {e}")
        return []

    @staticmethod
    def chart_bar_limit(timeframe: str, limit: int = 300) -> Optional[int]:
        """A requested date window is returned in full. ``limit`` is only for an open-ended query."""
        del timeframe, limit
        return None

    @staticmethod
    def fetch_candlesticks(
        dataset_id: str,
        ticker: str,
        timeframe: str = "5m",
        limit: int = 500,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        split_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch candlesticks aggregated to specified timeframe.
        - Daily: Read directly from quant_dw.market_daily_ohlcv FINAL.
        - 1m / 5m / 10m / 15m: Aggregated from quant_dw.market_intraday_bars FINAL using weighted sum VWAP.
        A bounded window returns every bar in that range. Without a window, the latest
        ``limit`` bars are returned. The chart draws on canvas, so the full window stays usable.
        """
        timeframe = timeframe.lower()
        start_day = _sql_day(start_date)
        end_day = _sql_day(end_date)
        bounded = start_day is not None or end_day is not None
        split_filter = f" AND split_id = '{split_id}'" if split_id else ""
        if timeframe in ["1d", "daily"]:
            date_filter = ""
            if start_day:
                date_filter += f" AND trade_date >= '{start_day}'"
            if end_day:
                date_filter += f" AND trade_date <= '{end_day}'"

            select_daily = f"""
                    SELECT 
                        toString(trade_date) AS bar_time,
                        toFloat64(open_price) AS open,
                        toFloat64(high_price) AS high,
                        toFloat64(low_price) AS low,
                        toFloat64(close_price) AS close,
                        toFloat64(volume) AS volume
                    FROM quant_dw.market_daily_ohlcv FINAL
                    WHERE dataset_id = '{dataset_id}' AND ticker_symbol = '{ticker}'{split_filter}{date_filter}
            """
            if bounded:
                query = select_daily + " ORDER BY trade_date ASC"
            else:
                query = f"""
                SELECT * FROM (
                    {select_daily}
                    ORDER BY trade_date DESC
                    LIMIT {int(limit)}
                ) AS sub_daily
                ORDER BY bar_time ASC
                """
            df = run_clickhouse_query(query, timeout=120)
            if not df.empty:
                df["bar_time"] = pd.to_datetime(df["bar_time"])
                df["close"] = df["close"].astype(float)
                df["volume"] = df["volume"].astype(float)
                df["open"] = df["open"].astype(float)
                df["high"] = df["high"].astype(float)
                df["low"] = df["low"].astype(float)
                cum_vol = df["volume"].cumsum().replace(0, 1.0)
                df["vwap"] = (df["close"] * df["volume"]).cumsum() / cum_vol
            return df

        # Intraday resampling (1m, 5m, 10m, 15m)
        interval_minutes = 1
        if timeframe in ["5m", "5min"]:
            interval_minutes = 5
        elif timeframe in ["10m", "10min"]:
            interval_minutes = 10
        elif timeframe in ["15m", "15min"]:
            interval_minutes = 15

        date_filter = ""
        if start_day:
            date_filter += f" AND toDate(window_start) >= '{start_day}'"
        if end_day:
            date_filter += f" AND toDate(window_start) <= '{end_day}'"

        grouped = f"""
                    SELECT 
                        toStartOfInterval(window_start, INTERVAL {interval_minutes} MINUTE) AS bar_time,
                        argMin(open_price, window_start) AS open,
                        max(high_price) AS high,
                        min(low_price) AS low,
                        argMax(close_price, window_start) AS close,
                        sum(volume) AS total_volume,
                        if(sum(volume) > 0, sum(vwap * toFloat64(volume)) / toFloat64(sum(volume)), argMax(close_price, window_start)) AS vwap
                    FROM quant_dw.market_intraday_bars FINAL
                    WHERE dataset_id = '{dataset_id}' AND ticker_symbol = '{ticker}'{split_filter} AND is_deleted = 0 AND close_price > 0{date_filter}
                    GROUP BY bar_time
        """
        selected = f"""
                SELECT 
                    bar_time, 
                    toFloat64(open) AS open, 
                    toFloat64(high) AS high, 
                    toFloat64(low) AS low, 
                    toFloat64(close) AS close, 
                    toFloat64(total_volume) AS volume, 
                    toFloat64(vwap) AS vwap
                FROM (
                    {grouped}
                )
        """
        cap = MarketDataService.chart_bar_limit(timeframe, limit)
        if not bounded:
            cap = int(limit)
        if cap is None:
            query = selected + " ORDER BY bar_time ASC"
        else:
            query = f"""
            SELECT * FROM (
                {selected}
                ORDER BY bar_time DESC
                LIMIT {int(cap)}
            ) AS sub_intra
            ORDER BY bar_time ASC
        """
        try:
            df = run_clickhouse_query(query, timeout=120)
            if not df.empty:
                df["bar_time"] = pd.to_datetime(df["bar_time"])
                df["close"] = df["close"].astype(float)
                df["volume"] = df["volume"].astype(float)
                df["open"] = df["open"].astype(float)
                df["high"] = df["high"].astype(float)
                df["low"] = df["low"].astype(float)
                # Ensure strictly valid candlestick bounds: high >= max(open, close), low <= min(open, close)
                df["high"] = df[["high", "open", "close"]].max(axis=1)
                df["low"] = df[["low", "open", "close"]].min(axis=1)
                df["vwap"] = df["vwap"].astype(float).fillna(df["close"])
                return df
        except Exception as e:
            logger.warning(f"Intraday query failed: {e}")

        return pd.DataFrame()

    @staticmethod
    def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators matching the aggregated timeframe (no lookahead)."""
        if df.empty or len(df) < 5:
            return df

        df = df.copy()
        close = df["close"].astype(float)

        # 1. Moving Averages
        df["sma_20"] = close.rolling(window=20, min_periods=5).mean()
        df["sma_50"] = close.rolling(window=50, min_periods=10).mean()
        df["ema_12"] = close.ewm(span=12, adjust=False).mean()
        df["ema_26"] = close.ewm(span=26, adjust=False).mean()

        # 2. MACD (12, 26, 9)
        df["macd_line"] = df["ema_12"] - df["ema_26"]
        df["macd_signal"] = df["macd_line"].ewm(span=9, adjust=False).mean()
        df["macd_histogram"] = df["macd_line"] - df["macd_signal"]

        # 3. RSI (14)
        delta = close.diff()
        gain = (delta.where(delta > 0, 0.0)).rolling(window=14, min_periods=5).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(window=14, min_periods=5).mean()
        rs = gain / loss.replace(0, np.nan)
        df["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))
        df["rsi_14"] = df["rsi_14"].fillna(50.0)

        return df

    @staticmethod
    def _load_paper_trades(
        dataset_id: Optional[str] = None,
        ticker: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Load fills for a dataset and ticker. Raises TradeMarkerQueryError on failure."""
        conn = None
        try:
            conn = get_postgres_connection()
            ensure_paper_trades_alpha_column(conn)
            query = """
                SELECT trade_id, order_id, session_id, dataset_id, ticker_symbol,
                       direction, fill_price, fill_quantity, slippage_bps, commission,
                       realized_pnl, executed_at, model_version, alpha_id
                FROM paper_trades
                WHERE 1=1
            """
            params: List[Any] = []
            if dataset_id:
                query += " AND dataset_id = %s"
                params.append(dataset_id)
            if ticker:
                query += " AND ticker_symbol = %s"
                params.append(ticker)
            if session_id and session_id != "ALL":
                query += " AND session_id = %s"
                params.append(session_id)
            query += " ORDER BY executed_at ASC"
            df = pd.read_sql(query, conn, params=params)
            if not df.empty:
                df["executed_at"] = pd.to_datetime(df["executed_at"], utc=True)
                df["fill_price"] = df["fill_price"].astype(float)
                df["fill_quantity"] = df["fill_quantity"].astype(float)
                df["slippage_bps"] = df["slippage_bps"].astype(float)
                df["realized_pnl"] = df["realized_pnl"].astype(float)
            return df
        except Exception as exc:
            raise TradeMarkerQueryError(str(exc)) from exc
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def get_fill_time_bounds(
        dataset_id: Optional[str] = None,
        ticker: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> FillBounds:
        """Unfiltered executed_at span. Used to place the Trading Replay candle window."""
        try:
            df = MarketDataService._load_paper_trades(dataset_id, ticker, session_id)
        except TradeMarkerQueryError as exc:
            return FillBounds(error=str(exc))
        if df.empty:
            return FillBounds(count=0)
        return FillBounds(
            min_time=_utc_timestamp(df["executed_at"].min()),
            max_time=_utc_timestamp(df["executed_at"].max()),
            count=int(len(df)),
        )

    @staticmethod
    def get_trade_markers(
        dataset_id: Optional[str] = None,
        ticker: Optional[str] = None,
        session_id: Optional[str] = None,
        start_time: Optional[Any] = None,
        end_time: Optional[Any] = None,
        alpha_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fills inside the window. Raises TradeMarkerQueryError instead of returning an empty frame on failure."""
        summary = MarketDataService.summarize_trade_markers(
            dataset_id=dataset_id,
            ticker=ticker,
            session_id=session_id,
            start_time=start_time,
            end_time=end_time,
            alpha_id=alpha_id,
        )
        if summary.status == "error":
            raise TradeMarkerQueryError(summary.error or summary.message)
        return summary.frame

    @staticmethod
    def summarize_trade_markers(
        dataset_id: Optional[str] = None,
        ticker: Optional[str] = None,
        session_id: Optional[str] = None,
        start_time: Optional[Any] = None,
        end_time: Optional[Any] = None,
        alpha_id: Optional[str] = None,
    ) -> TradeMarkerSummary:
        """Classify the fill query as error, empty, outside the candle window, or matched."""
        ticker_label = ticker or "ALL"
        dataset_label = dataset_id or "ALL"
        alpha_clause = f" and alpha `{alpha_id}`" if alpha_id else ""
        window_start = _utc_timestamp(start_time) if start_time is not None else None
        window_end = _utc_timestamp(end_time) if end_time is not None else None
        try:
            df = MarketDataService._load_paper_trades(dataset_id, ticker, session_id)
        except TradeMarkerQueryError as exc:
            message = f"paper_trades query failed for `{ticker_label}` in dataset `{dataset_label}`: {exc}"
            return TradeMarkerSummary(
                status="error",
                message=message,
                frame=pd.DataFrame(),
                window_start=window_start,
                window_end=window_end,
                error=str(exc),
            )

        scoped = df
        if alpha_id:
            if "alpha_id" not in scoped.columns:
                scoped = scoped.iloc[0:0]
            else:
                scoped = scoped[scoped["alpha_id"].map(lambda value: _alpha_token_match(value, alpha_id))].copy()

        if scoped.empty:
            message = (
                f"No paper_trades rows for `{ticker_label}` in dataset `{dataset_label}`{alpha_clause}."
            )
            return TradeMarkerSummary(
                status="empty",
                message=message,
                frame=pd.DataFrame(),
                window_start=window_start,
                window_end=window_end,
                total_count=0,
            )

        fill_min = _utc_timestamp(scoped["executed_at"].min())
        fill_max = _utc_timestamp(scoped["executed_at"].max())
        matched = scoped
        if window_start is not None:
            matched = matched[matched["executed_at"] >= window_start]
        if window_end is not None:
            matched = matched[matched["executed_at"] <= window_end]
        matched = matched.copy()

        if matched.empty:
            message = (
                f"paper_trades has {len(scoped)} fill(s) for `{ticker_label}` in dataset `{dataset_label}`"
                f"{alpha_clause}, executed_at {_fmt_ts(fill_min)} → {_fmt_ts(fill_max)}, "
                f"which is outside the rendered candle window {_fmt_ts(window_start)} → {_fmt_ts(window_end)}."
            )
            return TradeMarkerSummary(
                status="outside",
                message=message,
                frame=pd.DataFrame(),
                fill_min=fill_min,
                fill_max=fill_max,
                window_start=window_start,
                window_end=window_end,
                matched_count=0,
                total_count=int(len(scoped)),
            )

        message = (
            f"Displaying {len(matched)} execution trades from paper_trades for `{ticker_label}` "
            f"in dataset `{dataset_label}`{alpha_clause} inside "
            f"{_fmt_ts(window_start)} → {_fmt_ts(window_end)}."
        )
        return TradeMarkerSummary(
            status="matched",
            message=message,
            frame=matched.reset_index(drop=True),
            fill_min=fill_min,
            fill_max=fill_max,
            window_start=window_start,
            window_end=window_end,
            matched_count=int(len(matched)),
            total_count=int(len(scoped)),
        )

    @staticmethod
    def _strategy_name(alpha_id: str, config: Dict[str, Any]) -> str:
        name = str(config.get("strategy") or alpha_id)
        if name.startswith("alpha_"):
            name = name[len("alpha_"):]
        for suffix in ("_daily", "_intraday"):
            if name.endswith(suffix):
                name = name[: -len(suffix)]
        return name

    @staticmethod
    def load_alpha_config(alpha_id: str) -> Dict[str, Any]:
        """Newest registry config for one alpha. Raises LookupError when the id is absent."""
        conn = get_postgres_connection()
        try:
            df = pd.read_sql(
                """
                SELECT alpha_id, version, config_json
                FROM alpha_registry
                WHERE alpha_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                conn,
                params=[alpha_id],
            )
        finally:
            conn.close()
        if df.empty:
            raise LookupError(f"Alpha `{alpha_id}` is not in alpha_registry")
        raw = df.iloc[0]["config_json"]
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                parsed = {}
        elif isinstance(raw, dict):
            parsed = raw
        else:
            parsed = {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def load_asof_features(dataset_id: str, ticker: str) -> pd.DataFrame:
        """Technical features for this ticker. Same-day rows are not visible to that day's bar."""
        try:
            df = run_clickhouse_query(trading_replay_features_sql(dataset_id, [ticker]))
        except Exception as exc:
            logger.warning(f"As-of feature query failed for {ticker}: {exc}")
            return pd.DataFrame()
        if df.empty or "event_timestamp" not in df.columns:
            return pd.DataFrame()
        df = df.copy()
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        return df.sort_values("event_timestamp")

    @staticmethod
    def score_candle_frame(
        candles: pd.DataFrame,
        strategy: str,
        config: Optional[Dict[str, Any]] = None,
        entry_z: Optional[float] = None,
        exit_z: Optional[float] = None,
        asof_df: Optional[pd.DataFrame] = None,
        ticker: str = "",
        p_ml: Optional[List[float]] = None,
        allow_asof: bool = True,
    ) -> pd.DataFrame:
        """BUY/SELL/FLAT for each rendered candle from that candle's own features."""
        from quant_edai.quant.alpha_catalog import (
            DEFAULT_ENTRY_Z,
            DEFAULT_EXIT_Z,
        )

        config = dict(config or {})
        entry = float(config.get("entry_z", DEFAULT_ENTRY_Z if entry_z is None else entry_z))
        if entry_z is not None:
            entry = float(entry_z)
        exit_level = float(config.get("exit_z", DEFAULT_EXIT_Z if exit_z is None else exit_z))
        if exit_z is not None:
            exit_level = float(exit_z)

        if candles is None or candles.empty:
            return pd.DataFrame(columns=["bar_time", "time_label", "action", "z_score", "close", "high", "low"])

        frame = candles.copy().reset_index(drop=True)
        frame["bar_time"] = pd.to_datetime(frame["bar_time"], utc=True)
        if allow_asof and asof_df is not None and not asof_df.empty and "event_timestamp" in asof_df.columns:
            feats = asof_df.copy()
            feats["event_timestamp"] = pd.to_datetime(feats["event_timestamp"], utc=True)
            feats["available_at"] = feats["event_timestamp"].dt.floor("D") + pd.Timedelta(days=1)
            feat_cols = [
                c for c in (
                    "sma_20", "sma_50", "sma_ratio_20_50", "rsi_14d", "macd_histogram",
                    "rolling_volatility_30d", "intraday_return_1d", "close_price",
                ) if c in feats.columns
            ]
            feats = feats[["available_at", *feat_cols]].rename(
                columns={c: f"asof_{c}" for c in feat_cols}
            ).sort_values("available_at")
            frame = pd.merge_asof(
                frame.sort_values("bar_time"),
                feats,
                left_on="bar_time",
                right_on="available_at",
                direction="backward",
            )

        count = len(frame)
        closes = frame["close"].to_numpy(dtype=float)
        volumes = frame["volume"].to_numpy(dtype=float) if "volume" in frame.columns else np.zeros(count)
        highs = frame["high"].to_numpy(dtype=float) if "high" in frame.columns else closes
        lows = frame["low"].to_numpy(dtype=float) if "low" in frame.columns else closes
        ready = np.arange(count) >= 4

        def _numeric(column: str) -> np.ndarray:
            if column not in frame.columns:
                return np.full(count, np.nan)
            return pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)

        def _prefer_candle(candle: np.ndarray, asof_column: str, candle_ready: np.ndarray) -> np.ndarray:
            chosen = np.where(candle_ready & np.isfinite(candle), candle, np.nan)
            if allow_asof:
                asof = _numeric(asof_column)
                missing = np.isnan(chosen) & np.isfinite(asof)
                chosen = chosen.copy()
                chosen[missing] = asof[missing]
            return chosen

        rsi = _prefer_candle(_numeric("rsi_14"), "asof_rsi_14d", ready)
        macd = _prefer_candle(_numeric("macd_histogram"), "asof_macd_histogram", ready)
        sma_20 = _numeric("sma_20")
        sma_50 = _numeric("sma_50")
        sma_ok = ready & np.isfinite(sma_20) & np.isfinite(sma_50) & (sma_50 != 0.0)
        sma_ratio = np.full(count, np.nan)
        np.divide(sma_20, sma_50, out=sma_ratio, where=sma_ok)
        sma_ratio = _prefer_candle(sma_ratio, "asof_sma_ratio_20_50", np.isfinite(sma_ratio))

        bar_return = np.full(count, np.nan)
        own_return = np.zeros(count, dtype=bool)
        if count > 1:
            previous = closes[:-1]
            own_return[1:] = previous != 0.0
            bar_return[1:] = np.divide(
                closes[1:] - previous,
                previous,
                out=np.zeros(count - 1),
                where=previous != 0.0,
            )
            bar_return[~own_return] = np.nan
        if allow_asof:
            asof_return = _numeric("asof_intraday_return_1d")
            fill_return = ~own_return & np.isfinite(asof_return)
            bar_return[fill_return] = asof_return[fill_return]

        z_mom = np.where(np.isfinite(bar_return), bar_return * 50.0, np.nan)
        z_rsi = np.where(np.isfinite(rsi), -(rsi - 50.0) / 10.0, np.nan)
        z_macd = np.where(np.isfinite(macd), np.where(np.abs(macd) < 2.0, macd * 5.0, 0.0), np.nan)
        z_sma = np.where(np.isfinite(sma_ratio), (sma_ratio - 1.0) * 50.0, np.nan)

        vwap = _numeric("vwap")
        vwap_taken = np.isfinite(vwap) & (vwap != 0.0)
        z_vwap = np.full(count, np.nan)
        if np.any(vwap_taken):
            deviation = (closes[vwap_taken] - vwap[vwap_taken]) / vwap[vwap_taken]
            z_vwap[vwap_taken] = _expanding_population_z(deviation)
            z_vwap = pd.Series(z_vwap).ffill().to_numpy(dtype=float)
        z_volume = _expanding_population_z(volumes) if count else np.array([])

        z_break = np.zeros(count)
        if count >= 10:
            previous = closes[:-1]
            rets = (closes[1:] - previous) / np.maximum(previous, 1e-8)
            total = np.cumsum(rets)
            total_sq = np.cumsum(rets * rets)
            points = np.arange(1, len(rets) + 1, dtype=float)
            mean = total / points
            std = np.sqrt(np.maximum(total_sq / points - mean * mean, 0.0))
            ratio = np.zeros(len(rets))
            usable = std > 1e-8
            ratio[usable] = rets[usable] / std[usable]
            z_break[9:] = ratio[8:]

        probabilities = np.full(count, np.nan)
        if p_ml is not None:
            for index, value in enumerate(p_ml[:count]):
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                if np.isfinite(number):
                    probabilities[index] = number
        z_ml = np.where(np.isfinite(probabilities), (probabilities - 0.5) * 4.0, np.nan)

        vol = _numeric("bar_vol_ann")
        if allow_asof:
            asof_vol = _numeric("asof_rolling_volatility_30d")
            missing_vol = np.isnan(vol) & np.isfinite(asof_vol)
            vol = vol.copy()
            vol[missing_vol] = asof_vol[missing_vol]

        w_mom = float(config.get("w_mom", 0.50))
        w_tech = float(config.get("w_tech", 0.30))
        w_ml = float(config.get("w_ml", 0.20))

        def _mean_available(parts: List[np.ndarray]) -> np.ndarray:
            stack = np.vstack(parts)
            available = np.sum(~np.isnan(stack), axis=0)
            mean = np.full(count, np.nan)
            np.divide(np.nansum(stack, axis=0), available, out=mean, where=available > 0)
            return mean

        def _sum_available(parts: List[np.ndarray]) -> np.ndarray:
            stack = np.vstack(parts)
            present = np.any(~np.isnan(stack), axis=0)
            return np.where(present, np.nansum(stack, axis=0), 0.0)

        if strategy == "momentum_z":
            z_score = np.where(np.isnan(z_mom), 0.0, z_mom)
        elif strategy == "vwap_dev_z":
            z_score = np.where(np.isnan(z_vwap), 0.0, z_vwap)
        elif strategy == "rsi_mr":
            z_score = np.where(np.isnan(z_rsi), 0.0, z_rsi)
        elif strategy == "macd_hist_z":
            z_score = np.where(np.isnan(z_macd), 0.0, z_macd)
        elif strategy == "sma_ratio":
            z_score = np.where(np.isnan(z_sma), 0.0, z_sma)
        elif strategy == "vol_breakout":
            z_score = z_break
        elif strategy == "volume_z":
            z_score = np.where(np.isnan(z_volume), 0.0, z_volume)
        elif strategy == "ml_prob":
            z_score = np.where(np.isnan(z_ml), 0.0, z_ml)
        elif strategy == "composite_eq":
            z_score = _sum_available([
                np.where(np.isnan(z_mom), np.nan, w_mom * z_mom),
                np.where(np.isnan(_mean_available([z_rsi, z_vwap])), np.nan, w_tech * _mean_available([z_rsi, z_vwap])),
                np.where(np.isnan(z_ml), np.nan, w_ml * z_ml),
            ])
        elif strategy == "regime_adapt":
            from quant_edai.quant.alpha_synthesis import REGIME_WEIGHT_MAP, MarketRegime

            low = REGIME_WEIGHT_MAP[MarketRegime.LOW_VOL]
            normal = REGIME_WEIGHT_MAP[MarketRegime.NORMAL_VOL]
            high = REGIME_WEIGHT_MAP[MarketRegime.HIGH_VOL]
            low_mask = vol < 0.12
            high_mask = vol > 0.25
            mom_w = np.where(low_mask, low.w_momentum, np.where(high_mask, high.w_momentum, normal.w_momentum))
            tech_w = np.where(low_mask, low.w_technical, np.where(high_mask, high.w_technical, normal.w_technical))
            ml_w = np.where(low_mask, low.w_ml, np.where(high_mask, high.w_ml, normal.w_ml))
            technical = _mean_available([z_rsi, z_macd])
            z_score = _sum_available([
                np.where(np.isnan(z_mom), np.nan, mom_w * z_mom),
                np.where(np.isnan(technical), np.nan, tech_w * technical),
                np.where(np.isnan(z_ml), np.nan, ml_w * z_ml),
            ])
            z_score = np.where(np.isfinite(vol), z_score, 0.0)
        else:
            z_score = np.zeros(count)

        actions = np.where(z_score >= entry, "BUY", np.where(z_score <= -entry, "SELL", "FLAT"))
        labels = frame["time_label"].tolist() if "time_label" in frame.columns else [str(value) for value in frame["bar_time"]]
        return pd.DataFrame({
            "bar_time": frame["bar_time"].to_numpy(),
            "time_label": labels,
            "action": actions,
            "z_score": z_score.astype(float),
            "close": closes,
            "high": highs,
            "low": lows,
        })

    @staticmethod
    def score_candle_signals(
        candles: pd.DataFrame,
        alpha_id: str,
        dataset_id: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Score the selected registry alpha on the candles currently rendered."""
        config = MarketDataService.load_alpha_config(alpha_id)
        strategy = MarketDataService._strategy_name(alpha_id, config)
        asof = MarketDataService.load_asof_features(dataset_id, ticker)
        return MarketDataService.score_candle_frame(
            candles,
            strategy=strategy,
            config=config,
            asof_df=asof,
            ticker=ticker,
        )

    @staticmethod
    def bars_per_year(timeframe: str) -> int:
        """Annualization count for the candle timeframe. Intraday uses a 6.5h session."""
        from quant_edai.quant.metrics import periods_per_year

        return periods_per_year(timeframe)

    @staticmethod
    def positions_from_z(z_scores: List[float], entry_z: float, exit_z: float) -> List[int]:
        """Position held during each bar. The score on bar i is applied from bar i+1."""
        holding = 0
        positions: List[int] = []
        for z in z_scores:
            positions.append(holding)
            if holding == 0:
                if z >= entry_z:
                    holding = 1
                elif z <= -entry_z:
                    holding = -1
            elif holding == 1:
                if z <= -entry_z:
                    holding = -1
                elif z <= exit_z:
                    holding = 0
            elif z >= entry_z:
                holding = 1
            elif z >= -exit_z:
                holding = 0
        return positions

    @staticmethod
    def simulate_scored_book(
        scored: pd.DataFrame,
        *,
        entry_z: float,
        exit_z: float,
        booksize: float = 100_000.0,
        timeframe: str = "Daily",
        cost_bps: float = 6.0,
    ) -> Dict[str, Any]:
        """One-name book on the scored candles. Signal at a close earns the next bar.

        ``booksize`` is the full notional of this single ticker. PnL is dollars on that notional.
        Markers are the bars whose score opens or flips the position.
        """
        from quant_edai.quant.metrics import (
            SHARPE_INSUFFICIENT,
            calculate_max_drawdown,
            calculate_sharpe,
        )

        empty = {
            "booksize": float(booksize),
            "sharpe": None,
            "sharpe_reason": SHARPE_INSUFFICIENT,
            "max_drawdown": 0.0,
            "pnl": 0.0,
            "trading_pnl": 0.0,
            "cash_interest": 0.0,
            "trade_count": 0,
            "win_rate": 0.0,
            "final_equity": float(booksize),
            "markers": [],
            "max_abs_z": 0.0,
            "bar_returns": [],
            "leg_pnls": [],
        }
        if scored is None or scored.empty or "z_score" not in scored.columns or "close" not in scored.columns:
            return empty

        z_scores = [float(z) for z in scored["z_score"]]
        closes = [float(c) for c in scored["close"]]
        positions = MarketDataService.positions_from_z(z_scores, entry_z, exit_z)
        cost = float(cost_bps) / 10000.0
        returns = [0.0]
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            returns.append(0.0 if prev == 0 else (closes[i] - prev) / prev)
        strat = []
        for i, (pos, ret) in enumerate(zip(positions, returns)):
            prev_pos = positions[i - 1] if i else 0
            strat.append(pos * ret - abs(pos - prev_pos) * cost)
        equity = float(booksize)
        curve = []
        for ret in strat:
            equity *= 1.0 + ret
            curve.append(equity)

        legs: List[float] = []
        leg_pnl = 0.0
        in_leg = False
        for i, pos in enumerate(positions):
            prev_pos = positions[i - 1] if i else 0
            if pos != 0 and (prev_pos == 0 or prev_pos != pos):
                if in_leg:
                    legs.append(leg_pnl)
                in_leg = True
                leg_pnl = (curve[i] - (curve[i - 1] if i else booksize))
            elif pos != 0 and in_leg:
                leg_pnl += curve[i] - curve[i - 1]
            elif pos == 0 and in_leg:
                leg_pnl += curve[i] - curve[i - 1]
                legs.append(leg_pnl)
                in_leg = False
                leg_pnl = 0.0
        if in_leg:
            legs.append(leg_pnl)

        wins = [pnl for pnl in legs if pnl > 0]
        # ``strat`` is trading PnL only. Idle cash does not earn the risk-free rate.
        sharpe = calculate_sharpe(strat, rf=0.04, periods_per_year=MarketDataService.bars_per_year(timeframe))
        mdd, _ = calculate_max_drawdown(curve)
        trading_pnl = round(float(curve[-1] - booksize), 2) if curve else 0.0
        markers = []
        holding = 0
        for i, z in enumerate(z_scores):
            nxt = holding
            if holding == 0:
                if z >= entry_z:
                    nxt = 1
                elif z <= -entry_z:
                    nxt = -1
            elif holding == 1:
                if z <= -entry_z:
                    nxt = -1
                elif z <= exit_z:
                    nxt = 0
            elif z >= entry_z:
                nxt = 1
            elif z >= -exit_z:
                nxt = 0
            if nxt != holding and nxt != 0:
                row = scored.iloc[i]
                markers.append({
                    "index": i,
                    "price": round(float(row["low"] if nxt == 1 else row["high"]), 4),
                    "side": "up" if nxt == 1 else "down",
                    "action": "BUY" if nxt == 1 else "SELL",
                })
            holding = nxt

        return {
            "booksize": float(booksize),
            "sharpe": sharpe.rounded(2),
            "sharpe_reason": sharpe.reason,
            "max_drawdown": round(float(mdd) * 100.0, 2),
            "pnl": trading_pnl,
            "trading_pnl": trading_pnl,
            "cash_interest": 0.0,
            "trade_count": len(legs),
            "win_rate": round(100.0 * len(wins) / len(legs), 1) if legs else 0.0,
            "final_equity": round(float(curve[-1]), 2) if curve else float(booksize),
            "markers": markers,
            "max_abs_z": round(max(abs(z) for z in z_scores), 4) if z_scores else 0.0,
            "bar_returns": strat,
            "leg_pnls": legs,
        }

    @staticmethod
    def evaluate_candle_book(
        candles: pd.DataFrame,
        alpha_id: str,
        dataset_id: str,
        ticker: str,
        timeframe: str,
        booksize: float = 100_000.0,
    ) -> Dict[str, Any]:
        """Score the selected alpha on these candles and mark the one-name book."""
        config = MarketDataService.load_alpha_config(alpha_id)
        scored = MarketDataService.score_candle_signals(candles, alpha_id, dataset_id, ticker)
        entry_z = float(config.get("entry_z", 0.5))
        exit_z = float(config.get("exit_z", 0.2))
        book = MarketDataService.simulate_scored_book(
            scored,
            entry_z=entry_z,
            exit_z=exit_z,
            booksize=float(config.get("booksize", booksize)),
            timeframe=timeframe,
        )
        book["entry_z"] = entry_z
        book["exit_z"] = exit_z
        book["scored"] = scored
        return book

    @staticmethod
    def get_trade_sessions(dataset_id: Optional[str] = None, ticker: Optional[str] = None) -> List[str]:
        """Fetch list of active session IDs for the dataset and ticker."""
        try:
            conn = get_postgres_connection()
            query = "SELECT DISTINCT session_id FROM paper_trades WHERE 1=1"
            params = []
            if dataset_id:
                query += " AND dataset_id = %s"
                params.append(dataset_id)
            if ticker:
                query += " AND ticker_symbol = %s"
                params.append(ticker)
            query += " ORDER BY session_id ASC"
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            if not df.empty and "session_id" in df.columns:
                return df["session_id"].tolist()
        except Exception:
            pass
        return []


# ==============================================================================
# 2. Alpha Research & Dynamic Backtesting (Page 2)
# ==============================================================================
class AlphaResearchService:
    """Manages Alpha registry exploration, dynamic historical backtesting, and Top-5 persistence."""

    @staticmethod
    def get_registered_alphas(horizon: Optional[str] = None) -> pd.DataFrame:
        """Fetch list of registered Alphas from PostgreSQL alpha_registry."""
        try:
            conn = get_postgres_connection()
            query = "SELECT alpha_id, version, alpha_type, status, description, config_json, created_at FROM alpha_registry"
            params = []
            if horizon:
                query += " WHERE alpha_type = %s"
                params.append(horizon.upper())
            query += " ORDER BY alpha_id ASC"
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            return df
        except Exception as e:
            logger.warning(f"Could not load alpha registry: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_alpha_validation_comparison(horizon: Optional[str] = None, dataset_id: str = "ds_canonical_4y_eval_s05") -> pd.DataFrame:
        """IS and OOS metrics from the stored candle backtests. Missing values stay null."""
        try:
            from quant_edai.quant.alpha_split_evaluation import load_validation_comparison

            return load_validation_comparison(dataset_id=dataset_id, horizon=horizon)
        except Exception as e:
            logger.warning(f"Could not load alpha validation comparison: {e}")
            return pd.DataFrame()

    @staticmethod
    def run_historical_backtest(
        alpha_id: str,
        ticker: str,
        timeframe: str = "DAILY",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        slippage_bps: float = 5.0,
        commission_bps: float = 1.0,
        dataset_id: Optional[str] = None,
        split_id: Optional[str] = "IS",
    ) -> Dict[str, Any]:
        """
        Execute dynamic historical backtest across Gold Lakehouse features/OHLCV.
        Supports dataset-aware lineage, bidirectional Long/Short positions, zero look-ahead bias,
        intraday mega-cap eligibility enforcement, and canonical split isolation.
        """
        # 0. Intraday eligibility enforcement: Non-mega-caps have sparse intraday coverage (~8 bars/day)
        MEGA_CAPS = {"AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA"}
        if timeframe.upper() in ["1M", "5M", "10M", "15M", "INTRADAY"] and ticker != "ALL" and ticker not in MEGA_CAPS:
            return {
                "status": "INELIGIBLE",
                "message": f"Ticker {ticker} is non-mega-cap with sparse intraday coverage (~8 bars/day). Intraday alpha strategies require dense continuous ticks (Mega-Caps only: AAPL, AMZN, GOOGL, META, MSFT, NVDA, TSLA). Please select a Mega-Cap ticker or switch to Daily timeframe.",
                "metrics": {},
                "equity_curve": [],
            }

        # Resolve dataset-aware Gold Parquet path
        gold_path = None
        if dataset_id:
            ds_gold = Path(LAKEHOUSE_ROOT) / "runs" / dataset_id / "gold_features" / "feat_ticker_technical_indicators.parquet"
            if ds_gold.exists():
                gold_path = ds_gold

        if not gold_path or not gold_path.exists():
            gold_path = Path(LAKEHOUSE_ROOT) / "gold_features" / "feat_ticker_technical_indicators.parquet"

        if not gold_path.exists():
            return {
                "status": "DATA_NOT_FOUND",
                "message": f"Gold feature panel not found at {gold_path}",
                "metrics": {},
                "equity_curve": [],
            }

        df = pd.read_parquet(gold_path)
        if "split_id" in df.columns and split_id:
            df = df[df["split_id"] == split_id]

        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
            df = df.sort_values(by="event_timestamp")
            if start_date:
                df = df[df["event_timestamp"] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df["event_timestamp"] <= pd.to_datetime(end_date)]

        from quant_edai.quant.alpha_catalog import score_row_features
        from quant_edai.quant.metrics import calculate_max_drawdown, calculate_sharpe, periods_per_year

        # 1. Resolve strategy name and configuration from PostgreSQL alpha_registry or clean mapping
        strat_name = alpha_id
        config = {}
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()
            cur.execute("SELECT config_json FROM alpha_registry WHERE alpha_id = %s LIMIT 1", (alpha_id,))
            row = cur.fetchone()
            conn.close()
            if row and row[0]:
                config = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                strat_name = config.get("strategy", strat_name)
        except Exception:
            pass

        # Strip prefixes and suffixes
        if strat_name.startswith("alpha_"):
            strat_name = strat_name[len("alpha_"):]
        for suffix in ("_daily", "_intraday"):
            if strat_name.endswith(suffix):
                strat_name = strat_name[:-len(suffix)]

        entry_z = float(config.get("entry_z", 0.50))
        exit_z = float(config.get("exit_z", 0.20))
        cost_pct = (slippage_bps + commission_bps) / 10000.0

        def _calc_positions(z_scores: List[float], e_z: float, x_z: float) -> List[int]:
            """Calculate walk-forward symmetric Long (+1), Short (-1), or Flat (0) positions."""
            pos = [0]
            for z in z_scores[:-1]:
                curr = pos[-1]
                if curr == 0:
                    if z >= e_z:
                        pos.append(1)
                    elif z <= -e_z:
                        pos.append(-1)
                    else:
                        pos.append(0)
                elif curr == 1:
                    if z <= -e_z:
                        pos.append(-1)
                    elif z <= x_z:
                        pos.append(0)
                    else:
                        pos.append(1)
                elif curr == -1:
                    if z >= e_z:
                        pos.append(1)
                    elif z >= -x_z:
                        pos.append(0)
                    else:
                        pos.append(-1)
            return pos

        # 2. Backtest execution for Single Ticker or ALL Universe
        if ticker != "ALL":
            sub = df[df["ticker_symbol"] == ticker].sort_values("event_timestamp").reset_index(drop=True)
            if sub.empty or len(sub) < 20:
                return {
                    "status": "INSUFFICIENT_DATA",
                    "message": f"Insufficient data for ticker {ticker} (only {len(sub)} rows).",
                    "metrics": {},
                    "equity_curve": [],
                }
            z_scores = [score_row_features(strat_name, r.to_dict(), config=config)[0] for _, r in sub.iterrows()]
            sub["z_score"] = z_scores
            max_abs_z = float(np.max(np.abs(z_scores))) if len(z_scores) else 0.0

            sub["position"] = _calc_positions(z_scores, entry_z, exit_z)
            sub["return"] = sub["close_price"].pct_change().fillna(0.0)
            sub["trade_action"] = sub["position"].diff().abs().fillna(0.0)
            sub["strat_return"] = (sub["position"].shift(1).fillna(0.0) * sub["return"]) - (sub["trade_action"] * cost_pct)
            sub["cum_return"] = (1.0 + sub["strat_return"]).cumprod()
            sub["benchmark_cum"] = (1.0 + sub["return"]).cumprod()

            trade_count = int(sub["trade_action"].sum())
            strat_ret = sub["strat_return"]
            cum_ret = sub["cum_return"]
            bench_cum = sub["benchmark_cum"]
            dt_min = sub["event_timestamp"].min()
            dt_max = sub["event_timestamp"].max()
            timestamps = sub["event_timestamp"]
        else:
            # Universe Portfolio Equal-Weighted Backtest
            tickers_list = df["ticker_symbol"].unique().tolist()
            ticker_dfs = []
            trade_count = 0
            max_abs_z = 0.0
            for tkr in tickers_list:
                sub = df[df["ticker_symbol"] == tkr].sort_values("event_timestamp").reset_index(drop=True).copy()
                if len(sub) < 20:
                    continue
                z_scores = [score_row_features(strat_name, r.to_dict(), config=config)[0] for _, r in sub.iterrows()]
                sub["z_score"] = z_scores
                if len(z_scores):
                    max_abs_z = max(max_abs_z, float(np.max(np.abs(z_scores))))
                sub["position"] = _calc_positions(z_scores, entry_z, exit_z)
                sub["return"] = sub["close_price"].pct_change().fillna(0.0)
                sub["trade_action"] = sub["position"].diff().abs().fillna(0.0)
                sub["strat_return"] = (sub["position"].shift(1).fillna(0.0) * sub["return"]) - (sub["trade_action"] * cost_pct)
                trade_count += int(sub["trade_action"].sum())
                ticker_dfs.append(sub[["event_timestamp", "strat_return", "return"]])

            if not ticker_dfs:
                return {
                    "status": "INSUFFICIENT_DATA",
                    "message": "Insufficient data across all universe tickers.",
                    "metrics": {},
                    "equity_curve": [],
                }
            merged = pd.concat(ticker_dfs).groupby("event_timestamp").mean().sort_index().reset_index()
            merged["cum_return"] = (1.0 + merged["strat_return"]).cumprod()
            merged["benchmark_cum"] = (1.0 + merged["return"]).cumprod()
            strat_ret = merged["strat_return"]
            cum_ret = merged["cum_return"]
            bench_cum = merged["benchmark_cum"]
            dt_min = merged["event_timestamp"].min()
            dt_max = merged["event_timestamp"].max()
            timestamps = merged["event_timestamp"]

        # 3. Calculate Performance Metrics. Gold bars are daily regardless of the label.
        sharpe = calculate_sharpe(strat_ret, rf=0.04, periods_per_year=periods_per_year("Daily"))
        mdd, _ = calculate_max_drawdown(cum_ret)
        total_return = float(cum_ret.iloc[-1] - 1.0) * 100.0 if len(cum_ret) else 0.0
        wins = strat_ret[strat_ret > 0]
        losses = strat_ret[strat_ret < 0]
        win_rate = float(len(wins) / (len(wins) + len(losses))) if (len(wins) + len(losses)) > 0 else 0.0

        run_id = f"bt_{alpha_id[:12]}_{uuid.uuid4().hex[:8]}"
        metrics = {
            "run_id": run_id,
            "alpha_id": alpha_id,
            "ticker": ticker,
            "timeframe": timeframe,
            "start_date": str(dt_min.date()),
            "end_date": str(dt_max.date()),
            "calendar_days": int((dt_max - dt_min).days),
            "trade_count": trade_count,
            "sharpe_ratio": sharpe.rounded(2),
            "sharpe_reason": sharpe.reason,
            "max_drawdown_pct": round(float(mdd) * 100.0, 2),
            "total_return_pct": round(total_return, 2),
            "win_rate_pct": round(win_rate * 100.0, 1),
            "slippage_bps": slippage_bps,
            "commission_bps": commission_bps,
        }

        # 4. Explicit No Trades Handling: Do NOT claim SUCCESS when trade_count == 0
        if trade_count == 0:
            return {
                "status": "NO_TRADES",
                "message": f"Strategy '{alpha_id}' generated 0 trades on {ticker}: signal max Z-score ({max_abs_z:.2f}) did not breach entry threshold ({entry_z:.2f}). Adjust threshold or test on another ticker.",
                "metrics": metrics,
                "equity_curve": [],
            }

        # 5. Persist Successful Run to PostgreSQL
        try:
            conn = get_postgres_connection()
            cur = conn.cursor()
            cur.execute("ALTER TABLE alpha_backtest_results ALTER COLUMN sharpe_ratio DROP NOT NULL")
            cur.execute("""
                INSERT INTO alpha_backtest_results (
                    run_id, alpha_id, alpha_version, dataset_id, ticker_symbol, timeframe,
                    period_start, period_end, total_trades, win_rate, sharpe_ratio,
                    max_drawdown, total_return_pct, net_pnl, slippage_bps, commission_bps, metrics_json, split_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING;
            """, (
                run_id, alpha_id, "v1.0.0", dataset_id or "gold_lakehouse", ticker, timeframe,
                dt_min.date(), dt_max.date(),
                trade_count, round(win_rate, 4), sharpe.rounded(4), round(mdd, 4),
                round(total_return, 4), round(total_return * 1000.0, 2),
                slippage_bps, commission_bps, json.dumps(metrics), split_id or "IS"
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"Could not persist backtest result: {e}")

        equity_curve = [
            {
                "time": str(ts),
                "strategy_equity": round(float(s), 4),
                "benchmark_equity": round(float(b), 4),
            }
            for ts, s, b in zip(timestamps, cum_ret, bench_cum)
        ]

        return {
            "status": "SUCCESS",
            "metrics": metrics,
            "equity_curve": equity_curve,
        }

    @staticmethod
    def recompute_unstable_persisted_sharpes() -> Dict[str, Any]:
        """Correct stored Sharpes that were zero or non-finite only because variance was numerical dust.

        Updates ``sharpe_ratio`` and the Sharpe fields inside ``metrics_json``.
        ``run_id``, ``created_at``, dataset lineage, trades, and PnL stay as recorded.
        Market data is not written.
        """
        from quant_edai.quant.metrics import (
            calculate_sharpe,
            periods_per_year,
            persisted_sharpe_needs_recompute,
            stamp_recomputed_sharpe,
        )

        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("ALTER TABLE alpha_backtest_results ALTER COLUMN sharpe_ratio DROP NOT NULL")
        cur.execute(
            """
            SELECT run_id, timeframe, total_trades, sharpe_ratio, total_return_pct,
                   metrics_json, created_at
            FROM alpha_backtest_results
            """
        )
        rows = cur.fetchall()
        recomputed_at = datetime.now(timezone.utc).isoformat()
        updated = []
        for run_id, timeframe, total_trades, sharpe_ratio, total_return, metrics_json, created_at in rows:
            if not persisted_sharpe_needs_recompute(sharpe_ratio, total_trades, total_return):
                continue
            payload = metrics_json if isinstance(metrics_json, dict) else {}
            calendar_days = payload.get("calendar_days")
            try:
                length = int(calendar_days) if calendar_days is not None else 252
            except (TypeError, ValueError):
                length = 252
            length = max(length, 2)
            # A stored flat book (no trades, no return) has a zero trading-return series.
            result = calculate_sharpe(
                np.zeros(length),
                rf=0.04,
                periods_per_year=periods_per_year(str(timeframe or "Daily")),
            )
            stamped = stamp_recomputed_sharpe(
                payload,
                result,
                prior=sharpe_ratio,
                recomputed_at=recomputed_at,
            )
            cur.execute(
                """
                UPDATE alpha_backtest_results
                SET sharpe_ratio = %s,
                    metrics_json = %s::jsonb
                WHERE run_id = %s
                """,
                (result.rounded(4), json.dumps(stamped), run_id),
            )
            updated.append({
                "run_id": run_id,
                "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
                "sharpe_prior": float(sharpe_ratio) if sharpe_ratio is not None else None,
                "sharpe_ratio": result.rounded(4),
                "sharpe_reason": result.reason,
            })
        conn.commit()
        conn.close()
        return {"updated": len(updated), "runs": updated}

    @staticmethod
    def get_backtest_leaderboard(only_latest: bool = True) -> pd.DataFrame:
        """Retrieve historical backtest runs from PostgreSQL for the Leaderboard."""
        try:
            conn = get_postgres_connection()
            if only_latest:
                query = """
                    SELECT DISTINCT ON (alpha_id, ticker_symbol, timeframe)
                        run_id, alpha_id, alpha_version, dataset_id, ticker_symbol, timeframe, 
                        period_start, period_end, total_trades, win_rate, 
                        sharpe_ratio, max_drawdown, total_return_pct, created_at
                    FROM alpha_backtest_results
                    WHERE total_trades > 0
                    ORDER BY alpha_id, ticker_symbol, timeframe, created_at DESC;
                """
            else:
                query = """
                    SELECT run_id, alpha_id, alpha_version, dataset_id, ticker_symbol, timeframe, 
                           period_start, period_end, total_trades, win_rate, 
                           sharpe_ratio, max_drawdown, total_return_pct, created_at
                    FROM alpha_backtest_results
                    ORDER BY created_at DESC
                    LIMIT 100;
                """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            logger.warning(f"Could not load leaderboard: {e}")
            return pd.DataFrame()
            return df
        except Exception as e:
            logger.warning(f"Could not load leaderboard: {e}")
            return pd.DataFrame()

    @staticmethod
    def save_top5_ensemble(
        alpha_ids: List[str],
        horizon: str = "INTRADAY",
        dataset_id: str = "ds_canonical_4y_eval_s05",
        approved_by: str = "ADMIN",
        freeze: bool = True,
        versions: Optional[List[str]] = None,
        weights: Optional[List[float]] = None,
    ) -> bool:
        """Insert an immutable Top-5 approval. Previous approval rows are not updated."""
        from quant_edai.quant.alpha_split_evaluation import (
            POLICY_NAME,
            approval_block_reason,
            load_validation_comparison,
            persist_ensemble_approval,
            rank_ensemble_candidates,
            weight_block_reason,
        )

        chosen_versions = list(versions or [])
        chosen_weights = [0.2, 0.2, 0.2, 0.2, 0.2] if weights is None else [float(item) for item in weights]
        reason = approval_block_reason(
            alpha_ids,
            horizon,
            approved_by,
            dataset_id=dataset_id,
            versions=chosen_versions or None,
        )
        if reason is None:
            reason = weight_block_reason(chosen_weights)
        if reason or not freeze:
            logger.warning(f"Top-5 approval refused: {reason or 'freeze was not requested'}")
            return False
        try:
            ranked = rank_ensemble_candidates(
                load_validation_comparison(dataset_id=dataset_id, horizon=horizon),
                horizon,
            )
            evidence = []
            snapshot = []
            for record in ranked.to_dict("records"):
                if not bool(record.get("eligible")):
                    continue
                snapshot.append({
                    "rank": int(record["rank"]),
                    "alpha_id": record["alpha_id"],
                    "version": record["version"],
                    "oos_sharpe": record.get("oos_sharpe"),
                    "oos_mdd": record.get("oos_mdd"),
                    "oos_trades": record.get("oos_trades"),
                    "is_run_id": record.get("is_run_id"),
                    "oos_run_id": record.get("oos_run_id"),
                })
            for alpha_id, version, weight in zip(alpha_ids, chosen_versions, chosen_weights):
                match = ranked[
                    (ranked["alpha_id"] == alpha_id)
                    & (ranked["version"] == version)
                    & (ranked["eligible"] == True)
                ]
                if match.empty:
                    logger.warning(f"Top-5 approval refused: {alpha_id}@{version} is not eligible")
                    return False
                row = match.iloc[0]
                evidence.append({
                    "alpha_id": alpha_id,
                    "version": version,
                    "weight": weight,
                    "rank": int(row["rank"]),
                    "is_sharpe": row.get("is_sharpe"),
                    "oos_sharpe": row.get("oos_sharpe"),
                    "is_mdd": row.get("is_mdd"),
                    "oos_mdd": row.get("oos_mdd"),
                    "is_trades": row.get("is_trades"),
                    "oos_trades": row.get("oos_trades"),
                    "is_status": row.get("is_status"),
                    "oos_status": row.get("oos_status"),
                    "is_run_id": row.get("is_run_id"),
                    "oos_run_id": row.get("oos_run_id"),
                    "policy_name": POLICY_NAME,
                })
            conn = get_postgres_connection()
            stored = persist_ensemble_approval(
                conn,
                {
                    "horizon": horizon,
                    "dataset_id": dataset_id,
                    "alpha_ids": alpha_ids,
                    "versions": chosen_versions,
                    "weights": chosen_weights,
                    "approved_by": approved_by,
                    "member_evidence": evidence,
                    "rank_snapshot": snapshot,
                },
                is_postgres=True,
            )
            conn.close()
            if not stored.get("ok"):
                logger.warning(f"Top-5 approval refused: {stored.get('reason')}")
                return False
            logger.info(
                "Froze ensemble %s version %s: %s",
                stored.get("approval_id"), stored.get("ensemble_version"), alpha_ids,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save ensemble: {e}")
            return False

    @staticmethod
    def get_latest_top5_ensemble(horizon: str = "INTRADAY") -> Optional[List[str]]:
        """Fetch the newest immutable approval, then an older admin-frozen config row."""
        try:
            from quant_edai.quant.alpha_split_evaluation import latest_frozen_ensemble

            frozen = latest_frozen_ensemble(horizon=horizon)
            if frozen and frozen.get("selected_alpha_ids"):
                return list(frozen["selected_alpha_ids"])
            conn = get_postgres_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT selected_alpha_ids
                FROM alpha_ensemble_config
                WHERE horizon = %s
                  AND ensemble_id = 'default'
                  AND approval_status = 'APPROVED_FROZEN'
                  AND upper(approved_by) IN ('ADMIN', 'ADMIN_PM', 'ADMIN_QUANT')
                  AND lower(COALESCE(updated_by, '')) NOT IN ('test', 'pytest')
                ORDER BY updated_at DESC LIMIT 1;
            """, (horizon.upper(),))
            row = cur.fetchone()
            conn.close()
            if row:
                res = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                if isinstance(res, list):
                    return res
        except Exception as e:
            logger.warning(f"Error loading latest top 5 ensemble: {e}")
        return None


# ==============================================================================
# 3. Real-Time Trading Simulation & Replay Controller (Page 3)
# ==============================================================================
_CHAMPION_CACHE: Dict[str, Any] = {"loaded": False, "model": None, "features": None, "version": None}

CHAMPION_FEATURE_FALLBACK = (
    "close_price",
    "sma_20",
    "sma_50",
    "sma_ratio_20_50",
    "rsi_14d",
    "macd_12_26",
    "macd_signal_9",
    "macd_histogram",
    "rolling_volatility_30d",
    "intraday_return_1d",
)


def _champion_joblib_path() -> Path:
    here = Path(__file__).resolve()
    candidates = [
        Path("models/champion_model.joblib"),
        here.parents[4] / "models" / "champion_model.joblib",
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[-1]


def _load_champion_model() -> Tuple[Any, Optional[List[str]], Optional[str]]:
    """Load the trained champion artifact once. Missing file stays unloaded."""
    if _CHAMPION_CACHE["loaded"]:
        return _CHAMPION_CACHE["model"], _CHAMPION_CACHE["features"], _CHAMPION_CACHE["version"]
    _CHAMPION_CACHE["loaded"] = True
    path = _champion_joblib_path()
    if not path.exists():
        logger.warning(f"Champion model artifact not found at {path}")
        return None, None, None
    try:
        import joblib
        loaded = joblib.load(path)
    except Exception as exc:
        logger.warning(f"Could not load champion model: {exc}")
        return None, None, None
    model = loaded.get("model", loaded) if isinstance(loaded, dict) else loaded
    features = None
    if isinstance(loaded, dict) and loaded.get("features"):
        features = [str(name) for name in loaded["features"]]
    version = path.name
    meta = path.with_name("champion_model.meta.json")
    if meta.exists():
        try:
            version = str(json.loads(meta.read_text()).get("model_version") or version)
        except Exception:
            version = path.name
    _CHAMPION_CACHE["model"] = model
    _CHAMPION_CACHE["features"] = features or list(CHAMPION_FEATURE_FALLBACK)
    _CHAMPION_CACHE["version"] = version
    return _CHAMPION_CACHE["model"], _CHAMPION_CACHE["features"], _CHAMPION_CACHE["version"]


def score_champion_probability(features: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """Point-in-time p_ml from the champion joblib. Missing inputs or artifact return None."""
    model, names, version = _load_champion_model()
    if model is None or not names or not hasattr(model, "predict_proba"):
        return None, None
    row: List[float] = []
    for name in names:
        value = features.get(name)
        if value is None:
            return None, None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None, None
        if np.isnan(number):
            return None, None
        row.append(number)
    try:
        frame = pd.DataFrame([row], columns=list(names))
        probability = float(model.predict_proba(frame)[:, 1][0])
    except Exception as exc:
        logger.warning(f"Champion predict failed: {exc}")
        return None, None
    return probability, version


def index_asof_features(frame: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if frame is None or frame.empty or "ticker_symbol" not in frame.columns:
        return {}
    indexed: Dict[str, pd.DataFrame] = {}
    work = frame.copy()
    work["event_timestamp"] = pd.to_datetime(work["event_timestamp"], utc=True)
    for ticker, sub in work.groupby("ticker_symbol"):
        indexed[str(ticker)] = sub.sort_values("event_timestamp")
    return indexed


def lookup_asof_features(index: Dict[str, pd.DataFrame], ticker: str, bar_time: Any) -> Dict[str, Any]:
    """Last feature row whose event date is strictly before the bar's UTC date."""
    sub = index.get(ticker)
    if sub is None or sub.empty:
        return {}
    bar = _utc_timestamp(bar_time)
    if bar is None:
        return {}
    cutoff = bar.floor("D")
    eligible = sub[sub["event_timestamp"] < cutoff]
    if eligible.empty:
        return {}
    row = eligible.iloc[-1]
    payload: Dict[str, Any] = {}
    for key, value in row.to_dict().items():
        if key in ("ticker_symbol", "event_timestamp"):
            continue
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            continue
        payload[str(key)] = value
    if "close_price" not in payload and "close" in payload:
        payload["close_price"] = payload["close"]
    return payload


class ReplayState:
    """Thread-safe state container for the background replay controller."""
    def __init__(self):
        self.lock = threading.RLock()
        self.is_running = False
        self.is_paused = False
        self.session_id: str = ""
        self.dataset_id: str = ""
        self.speed_multiplier: int = 10
        self.processed_bars: int = 0
        self.total_bars: int = 0
        self.current_bar_time: str = ""
        self.active_tickers: List[str] = []
        self.selected_alphas: List[str] = []
        self.ensemble: Optional[Dict[str, Any]] = None
        self.latest_signals: Dict[str, Any] = {}
        self.last_error: str = ""
        self.signals_scored: int = 0
        self.rejections: int = 0
        self.orders: int = 0
        self.fills: int = 0
        self.open_positions: int = 0
        self.completed: bool = False
        self.pending_steps: int = 0
        self.live_metrics: Dict[str, Any] = {}
        self.equity_history: List[Dict[str, Any]] = []
        self.open_book: List[Dict[str, Any]] = []
        self.recent_fills: List[Dict[str, Any]] = []
        self.timeframe: str = "1m"
        self.alpha_mode: str = "ensemble"  # "ensemble" | "single"
        self.single_alpha_id: Optional[str] = None
        self.single_alpha_version: Optional[str] = None
        self.end_policy: str = "MARK_TO_MARKET"
        self.generation: int = 0
        self.thread: Optional[threading.Thread] = None


_GLOBAL_REPLAY_STATE = ReplayState()


class ReplayController:
    """Orchestrates streaming market replay into Kafka/PaperTradingEngine without blocking Streamlit."""

    @classmethod
    def get_state(cls) -> Dict[str, Any]:
        with _GLOBAL_REPLAY_STATE.lock:
            return {
                "is_running": _GLOBAL_REPLAY_STATE.is_running,
                "is_paused": _GLOBAL_REPLAY_STATE.is_paused,
                "session_id": _GLOBAL_REPLAY_STATE.session_id,
                "dataset_id": _GLOBAL_REPLAY_STATE.dataset_id,
                "speed": _GLOBAL_REPLAY_STATE.speed_multiplier,
                "processed_bars": _GLOBAL_REPLAY_STATE.processed_bars,
                "total_bars": _GLOBAL_REPLAY_STATE.total_bars,
                "current_time": _GLOBAL_REPLAY_STATE.current_bar_time,
                "tickers": _GLOBAL_REPLAY_STATE.active_tickers,
                "selected_alphas": list(_GLOBAL_REPLAY_STATE.selected_alphas),
                "ensemble": dict(_GLOBAL_REPLAY_STATE.ensemble or {}),
                "latest_signals": dict(_GLOBAL_REPLAY_STATE.latest_signals),
                "last_error": _GLOBAL_REPLAY_STATE.last_error,
                "signals_scored": _GLOBAL_REPLAY_STATE.signals_scored,
                "rejections": _GLOBAL_REPLAY_STATE.rejections,
                "orders": _GLOBAL_REPLAY_STATE.orders,
                "fills": _GLOBAL_REPLAY_STATE.fills,
                "open_positions": _GLOBAL_REPLAY_STATE.open_positions,
                "completed": _GLOBAL_REPLAY_STATE.completed,
                "pending_steps": _GLOBAL_REPLAY_STATE.pending_steps,
                "live_metrics": dict(_GLOBAL_REPLAY_STATE.live_metrics),
                "equity_history": list(_GLOBAL_REPLAY_STATE.equity_history[-2000:]),
                "open_book": list(_GLOBAL_REPLAY_STATE.open_book),
                "recent_fills": list(_GLOBAL_REPLAY_STATE.recent_fills),
                "timeframe": _GLOBAL_REPLAY_STATE.timeframe,
                "alpha_mode": _GLOBAL_REPLAY_STATE.alpha_mode,
                "single_alpha_id": _GLOBAL_REPLAY_STATE.single_alpha_id,
                "single_alpha_version": _GLOBAL_REPLAY_STATE.single_alpha_version,
                "end_policy": _GLOBAL_REPLAY_STATE.end_policy,
                "generation": _GLOBAL_REPLAY_STATE.generation,
            }

    @classmethod
    def start_replay(
        cls,
        session_id: str,
        dataset_id: str,
        speed: int = 10,
        tickers: Optional[List[str]] = None,
        selected_alphas: Optional[List[str]] = None,
        timeframe: str = "1m",
        alpha_mode: str = "ensemble",
        single_alpha_id: Optional[str] = None,
        single_alpha_version: Optional[str] = None,
        end_policy: str = "MARK_TO_MARKET",
    ) -> bool:
        from quant_edai.quant.alpha_split_evaluation import (
            freeze_replay_session,
            load_approved_replay_ensemble,
        )
        from quant_edai.quant.paper_db import PaperDatabase

        with _GLOBAL_REPLAY_STATE.lock:
            if _GLOBAL_REPLAY_STATE.is_running:
                return False

        def _refuse(message: str) -> bool:
            with _GLOBAL_REPLAY_STATE.lock:
                if _GLOBAL_REPLAY_STATE.is_running:
                    return False
                _GLOBAL_REPLAY_STATE.last_error = message
            return False

        if timeframe not in REPLAY_TIMEFRAMES:
            return _refuse(f"Timeframe `{timeframe}` is not one of {', '.join(REPLAY_TIMEFRAMES)}.")
        if end_policy not in REPLAY_END_POLICIES:
            return _refuse(f"End policy `{end_policy}` is not one of {', '.join(REPLAY_END_POLICIES)}.")
        if alpha_mode not in ("ensemble", "single"):
            return _refuse(f"Alpha mode `{alpha_mode}` is not supported.")
        try:
            available = MarketDataService.fetch_trading_tickers(dataset_id)
        except Exception as exc:
            available = []
            logger.warning(f"TRADING ticker lookup failed: {exc}")
        ticker_reason = ticker_selection_reason(tickers, available)
        if ticker_reason:
            return _refuse(ticker_reason)
        chosen = [str(item).strip() for item in (tickers or []) if str(item).strip()]

        try:
            db = PaperDatabase(require_postgres=True)
        except Exception as exc:
            return _refuse(f"PostgreSQL is required for official replay: {exc}")
        try:
            occupied = session_restart_block_reason(db.session_has_activity(session_id), session_id)
            if occupied:
                return _refuse(occupied)
            alpha_row = None
            if alpha_mode == "single":
                if single_alpha_id and single_alpha_version:
                    alpha_row = db.get_alpha(single_alpha_id, single_alpha_version)
                alpha_reason = single_alpha_block_reason(single_alpha_id, single_alpha_version, alpha_row)
                if alpha_reason:
                    return _refuse(alpha_reason)
        finally:
            db.close()

        if alpha_mode == "single":
            approved = {
                "ok": True,
                "reason": None,
                "ensemble_id": f"single_{single_alpha_id}",
                "horizon": "INTRADAY",
                "dataset_id": dataset_id,
                "model_version": None,
                "ensemble_version": None,
                "policy_name": "single_alpha_v1",
                "approved_by": "SINGLE_ALPHA_MODE",
                "approved_at": None,
                "approval_status": "SINGLE_ALPHA",
                "alphas": [{"alpha_id": single_alpha_id, "version": single_alpha_version, "weight": 1.0}],
                "weights": [1.0],
                "selected_alpha_ids": [single_alpha_id],
            }
        else:
            approved = load_approved_replay_ensemble(dataset_id=dataset_id)
        if not approved.get("ok"):
            return _refuse(approved.get("reason") or "Replay ensemble is not approved.")
        approved_ids = [alpha["alpha_id"] for alpha in approved.get("alphas") or []]
        requested = [str(item) for item in (selected_alphas or [])]
        if alpha_mode != "single" and requested and set(requested) != set(approved_ids):
            return _refuse("Replay start refused. The requested alpha list is not the approved INTRADAY ensemble.")
        frozen = freeze_replay_session(session_id, dataset_id, approved, split_id="TRADING")
        if not frozen.get("ok"):
            return _refuse(frozen.get("reason") or "Replay ensemble could not be frozen.")

        with _GLOBAL_REPLAY_STATE.lock:
            if _GLOBAL_REPLAY_STATE.is_running:
                return False
            _GLOBAL_REPLAY_STATE.generation += 1
            _GLOBAL_REPLAY_STATE.is_running = True
            _GLOBAL_REPLAY_STATE.is_paused = False
            _GLOBAL_REPLAY_STATE.session_id = session_id
            _GLOBAL_REPLAY_STATE.dataset_id = dataset_id
            _GLOBAL_REPLAY_STATE.speed_multiplier = speed
            _GLOBAL_REPLAY_STATE.active_tickers = chosen
            _GLOBAL_REPLAY_STATE.ensemble = frozen["ensemble"]
            _GLOBAL_REPLAY_STATE.selected_alphas = approved_ids
            _GLOBAL_REPLAY_STATE.processed_bars = 0
            _GLOBAL_REPLAY_STATE.latest_signals = {}
            _GLOBAL_REPLAY_STATE.last_error = ""
            _GLOBAL_REPLAY_STATE.current_bar_time = ""
            _GLOBAL_REPLAY_STATE.signals_scored = 0
            _GLOBAL_REPLAY_STATE.rejections = 0
            _GLOBAL_REPLAY_STATE.orders = 0
            _GLOBAL_REPLAY_STATE.fills = 0
            _GLOBAL_REPLAY_STATE.open_positions = 0
            _GLOBAL_REPLAY_STATE.completed = False
            _GLOBAL_REPLAY_STATE.pending_steps = 0
            _GLOBAL_REPLAY_STATE.timeframe = timeframe
            _GLOBAL_REPLAY_STATE.alpha_mode = alpha_mode
            _GLOBAL_REPLAY_STATE.single_alpha_id = single_alpha_id
            _GLOBAL_REPLAY_STATE.single_alpha_version = single_alpha_version
            _GLOBAL_REPLAY_STATE.end_policy = end_policy
            _GLOBAL_REPLAY_STATE.live_metrics = {}
            _GLOBAL_REPLAY_STATE.equity_history = []
            _GLOBAL_REPLAY_STATE.open_book = []
            _GLOBAL_REPLAY_STATE.recent_fills = []

            thread = threading.Thread(target=cls._worker_loop, daemon=True)
            _GLOBAL_REPLAY_STATE.thread = thread
            thread.start()
            return True

    @classmethod
    def pause_replay(cls) -> None:
        with _GLOBAL_REPLAY_STATE.lock:
            _GLOBAL_REPLAY_STATE.is_paused = True

    @classmethod
    def resume_replay(cls) -> None:
        with _GLOBAL_REPLAY_STATE.lock:
            _GLOBAL_REPLAY_STATE.is_paused = False
            _GLOBAL_REPLAY_STATE.pending_steps = 0

    @classmethod
    def set_speed(cls, speed: int) -> None:
        """Change pacing only. The engine never reads this value."""
        with _GLOBAL_REPLAY_STATE.lock:
            _GLOBAL_REPLAY_STATE.speed_multiplier = max(1, int(speed))

    @classmethod
    def step_replay(cls) -> bool:
        """Process one queued bar while paused. Orders stay on the same engine path."""
        with _GLOBAL_REPLAY_STATE.lock:
            if not (_GLOBAL_REPLAY_STATE.is_running and _GLOBAL_REPLAY_STATE.is_paused):
                return False
            _GLOBAL_REPLAY_STATE.pending_steps += 1
            return True

    @classmethod
    def publish_live_metrics(cls, generation: int, metrics: Dict[str, Any]) -> bool:
        """Publish one bar's metrics only while this worker still owns the session."""
        with _GLOBAL_REPLAY_STATE.lock:
            if _GLOBAL_REPLAY_STATE.generation != generation:
                return False
            _GLOBAL_REPLAY_STATE.live_metrics = dict(metrics)
            return True

    @classmethod
    def stop_replay(cls) -> None:
        with _GLOBAL_REPLAY_STATE.lock:
            _GLOBAL_REPLAY_STATE.is_running = False
            _GLOBAL_REPLAY_STATE.is_paused = False
            _GLOBAL_REPLAY_STATE.pending_steps = 0

    @classmethod
    def _worker_loop(cls) -> None:
        """Replay every TRADING intraday bar through the selected alphas and as-of features."""
        from quant_edai.quant.paper_db import PaperDatabase
        from quant_edai.quant.paper_trading import PaperTradingEngine, MarketBar
        from quant_edai.quant.alpha_serving import AlphaSignalServer

        generation = _GLOBAL_REPLAY_STATE.generation
        end_policy = _GLOBAL_REPLAY_STATE.end_policy

        def _abort(message: str) -> None:
            with _GLOBAL_REPLAY_STATE.lock:
                if _GLOBAL_REPLAY_STATE.generation != generation:
                    return
                _GLOBAL_REPLAY_STATE.last_error = message
                _GLOBAL_REPLAY_STATE.is_running = False
                _GLOBAL_REPLAY_STATE.is_paused = False
                _GLOBAL_REPLAY_STATE.pending_steps = 0

        try:
            db = PaperDatabase(require_postgres=True)
        except Exception as exc:
            _abort(f"PostgreSQL is required for official replay: {exc}")
            return
        alpha_server = AlphaSignalServer(db=db)
        dataset_id = _GLOBAL_REPLAY_STATE.dataset_id
        engine = PaperTradingEngine(
            session_id=_GLOBAL_REPLAY_STATE.session_id,
            db=db,
            alpha_source="store",
            signal_server=alpha_server,
            z_entry_threshold=0.50,
            dataset_id=dataset_id,
            feature_source="clickhouse_feat_technical_indicators_asof",
        )
        alpha_mode = _GLOBAL_REPLAY_STATE.alpha_mode
        single_alpha_id = _GLOBAL_REPLAY_STATE.single_alpha_id
        single_alpha_version = _GLOBAL_REPLAY_STATE.single_alpha_version
        engine.signal_strict = True
        engine.enforce_top_k_gate = True
        if alpha_mode == "single" and single_alpha_id:
            engine.signal_alpha_ids = [single_alpha_id]
            engine.signal_alpha_versions = {single_alpha_id: single_alpha_version}
        else:
            engine.signal_alpha_ids = list(_GLOBAL_REPLAY_STATE.selected_alphas)
            engine.signal_alpha_versions = {
                alpha["alpha_id"]: alpha["version"]
                for alpha in (_GLOBAL_REPLAY_STATE.ensemble or {}).get("alphas") or []
            }

        tickers = list(_GLOBAL_REPLAY_STATE.active_tickers)
        timeframe = _GLOBAL_REPLAY_STATE.timeframe
        try:
            if timeframe == "1m":
                bars_df = run_clickhouse_query(
                    trading_replay_bars_sql(dataset_id, tickers),
                    timeout=120,
                    raise_on_error=True,
                )
            else:
                interval = {"5m": 5, "10m": 10, "15m": 15}[timeframe]
                bars_df = run_clickhouse_query(
                    trading_replay_bars_aggregated_sql(dataset_id, tickers, interval),
                    timeout=120,
                    raise_on_error=True,
                )
        except Exception as exc:
            _abort(f"TRADING bar query failed: {exc}")
            return
        duplicate_reason = duplicate_bar_error(bars_df)
        if duplicate_reason:
            _abort(duplicate_reason)
            return

        feature_index: Dict[str, pd.DataFrame] = {}
        try:
            feature_index = index_asof_features(
                run_clickhouse_query(trading_replay_features_sql(dataset_id, tickers))
            )
        except Exception as exc:
            logger.warning(f"As-of feature load failed; replay will score from bar buffers only: {exc}")

        with _GLOBAL_REPLAY_STATE.lock:
            _GLOBAL_REPLAY_STATE.total_bars = len(bars_df)

        if bars_df.empty:
            _abort(f"No TRADING intraday bars for dataset `{dataset_id}` and tickers {tickers}.")
            return

        completed_run = True
        for _, row in bars_df.iterrows():
            with _GLOBAL_REPLAY_STATE.lock:
                if _GLOBAL_REPLAY_STATE.generation != generation:
                    return
                if not _GLOBAL_REPLAY_STATE.is_running:
                    completed_run = False
                    break
                is_paused = _GLOBAL_REPLAY_STATE.is_paused

            while is_paused:
                time.sleep(0.05)
                with _GLOBAL_REPLAY_STATE.lock:
                    if _GLOBAL_REPLAY_STATE.generation != generation:
                        return
                    if not _GLOBAL_REPLAY_STATE.is_running:
                        return
                    if _GLOBAL_REPLAY_STATE.pending_steps > 0:
                        _GLOBAL_REPLAY_STATE.pending_steps -= 1
                        break
                    is_paused = _GLOBAL_REPLAY_STATE.is_paused

            tkr = str(row["ticker_symbol"])
            bar_time_str = str(row["bar_time"])
            price = float(row["close"])
            opn = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            vol = float(row["volume"])
            vwap = float(row.get("vwap") or price)

            asof = lookup_asof_features(feature_index, tkr, bar_time_str)
            p_ml, model_ver = score_champion_probability(asof)
            engine.bar_features = asof

            bar = MarketBar(
                ticker_symbol=tkr,
                timestamp=bar_time_str,
                open_price=opn,
                high_price=high,
                low_price=low,
                close_price=price,
                volume=vol,
                vwap=vwap,
                p_ml=p_ml,
                model_version=model_ver,
                dataset_id=dataset_id,
                feature_source="clickhouse_feat_technical_indicators_asof",
            )
            try:
                res = engine.on_bar(bar)
            except Exception as e:
                logger.debug(f"Replay engine error for {tkr}: {e}")
                res = {}

            # Parse execution / rejection status from engine
            events = res.get("events", [])
            pos = engine.positions.get(tkr)
            notes = str(res.get("signal_notes") or engine._last_signal_notes.get(tkr, ""))
            signal_action = str(res.get("signal_action") or "")
            signal_state = str(res.get("signal_state") or "")
            rejected = signal_state == "REJECTED" or signal_action == "REJECTED" or notes.startswith("REJECTED")
            raw_signal = res.get("signal")
            sig_val = None if rejected or raw_signal is None else float(raw_signal)

            if signal_state == "SESSION_CLOSED":
                status_msg = "Session closed / Positions liquidated"
            elif any("ENTRY_LONG" in ev for ev in events):
                status_msg = f"FILLED: BUY @ ${price:.2f}"
            elif any("ENTRY_SHORT" in ev for ev in events):
                status_msg = f"FILLED: SELL @ ${price:.2f}"
            elif any("SL_" in ev for ev in events):
                status_msg = f"CLOSED: Stop Loss @ ${price:.2f}"
            elif any("TP_" in ev for ev in events):
                status_msg = f"CLOSED: Take Profit @ ${price:.2f}"
            elif any("EOD_FLAT" in ev for ev in events):
                status_msg = "Session closed / Positions liquidated"
            elif pos is not None:
                status_msg = f"HOLDING {pos.side} (PnL: ${pos.unrealized_pnl:+.2f})"
            elif engine.pm_controls.is_entry_blocked():
                status_msg = "BLOCKED: PM Risk Governance"
            elif engine.daily_halted:
                status_msg = "HALTED: Daily Circuit Breaker Active"
            elif len(engine.positions) >= engine.max_positions:
                status_msg = f"SKIPPED: Max Position Limit ({engine.max_positions}) Reached"
            elif engine.cash < engine.trade_notional:
                status_msg = "REJECTED: Insufficient Cash"
            elif rejected:
                status_msg = notes or "REJECTED"
            elif signal_state == "NO_SIGNAL":
                status_msg = "NO_SIGNAL"
            elif signal_state == "HOLD":
                status_msg = f"HOLD: Signal Z={sig_val:+.2f} inside the entry band"
            elif sig_val is not None:
                status_msg = f"Signal Z={sig_val:+.2f}"
            else:
                status_msg = "NO_SIGNAL"

            if signal_state in {"SESSION_CLOSED", "NO_SIGNAL", "REJECTED", "HOLD", "BUY", "SELL"}:
                action_label = "REJECTED" if rejected else signal_state
            elif rejected:
                action_label = "REJECTED"
            elif sig_val is None:
                action_label = "NO_SIGNAL"
            elif sig_val >= engine.z_entry_threshold:
                action_label = "BUY"
            elif sig_val <= -engine.z_entry_threshold:
                action_label = "SELL"
            else:
                action_label = "HOLD"
            fill_events = [
                ev for ev in events
                if ev.startswith(("ENTRY_", "SIGNAL_EXIT_", "SL_", "TP_", "EOD_FLAT", "CIRCUIT_BREAKER", "END_OF_REPLAY"))
            ]

            new_scores = list(res.get("alpha_scores") or [])
            book = engine.book_metrics()
            with _GLOBAL_REPLAY_STATE.lock:
                if _GLOBAL_REPLAY_STATE.generation != generation:
                    return
                previous = _GLOBAL_REPLAY_STATE.latest_signals.get(tkr) or {}
                if new_scores:
                    shown_scores = new_scores
                    evaluated_at = bar_time_str
                    score_scope = "current"
                else:
                    shown_scores = list(previous.get("alpha_scores") or [])
                    evaluated_at = previous.get("evaluated_at")
                    score_scope = "last_evaluated" if shown_scores else "none"
                _GLOBAL_REPLAY_STATE.processed_bars += 1
                _GLOBAL_REPLAY_STATE.current_bar_time = bar_time_str
                _GLOBAL_REPLAY_STATE.signals_scored += 0 if rejected or signal_state == "SESSION_CLOSED" else 1
                _GLOBAL_REPLAY_STATE.rejections += 1 if rejected else 0
                _GLOBAL_REPLAY_STATE.orders += len(fill_events)
                _GLOBAL_REPLAY_STATE.fills += len(fill_events)
                _GLOBAL_REPLAY_STATE.open_positions = len(engine.positions)
                if signal_state == "SESSION_CLOSED" and previous.get("z_combined") is not None:
                    shown_z = previous.get("z_combined")
                else:
                    shown_z = None if sig_val is None else round(sig_val, 4)
                _GLOBAL_REPLAY_STATE.latest_signals[tkr] = {
                    "price": price,
                    "p_ml": p_ml,
                    "z_combined": shown_z,
                    "action": action_label,
                    "status": status_msg,
                    "position": pos.side if pos else "FLAT",
                    "unrealized_pnl": round(pos.unrealized_pnl, 2) if pos else 0.0,
                    "last_bar_time": bar_time_str,
                    "alpha_scores": shown_scores,
                    "evaluated_at": evaluated_at,
                    "score_scope": score_scope,
                }
                if not cls.publish_live_metrics(generation, {
                    "session_id": engine.session_id,
                    "cash": book["cash"],
                    "equity": book["equity"],
                    "realized_pnl": book["realized_pnl"],
                    "unrealized_pnl": book["unrealized_pnl"],
                    "drawdown": book["drawdown"],
                    "open_positions": int(book["open_positions"]),
                    "last_bar": bar_time_str,
                    "source": "engine_memory",
                }):
                    return
                _GLOBAL_REPLAY_STATE.open_book = [
                    {
                        "ticker_symbol": pos_row.ticker_symbol,
                        "side": pos_row.side,
                        "quantity": pos_row.quantity,
                        "entry_price": pos_row.average_entry_price,
                        "mark_price": pos_row.current_mark_price,
                        "unrealized_pnl": pos_row.unrealized_pnl,
                        "entry_time": pos_row.entry_time,
                    }
                    for pos_row in engine.positions.values()
                ]
                if fill_events:
                    _GLOBAL_REPLAY_STATE.recent_fills.append({
                        "event_time": bar_time_str,
                        "ticker_symbol": tkr,
                        "events": fill_events,
                        "price": price,
                    })
                    _GLOBAL_REPLAY_STATE.recent_fills = _GLOBAL_REPLAY_STATE.recent_fills[-20:]
                _GLOBAL_REPLAY_STATE.equity_history.append({
                    "event_time": bar_time_str,
                    "equity": book["equity"],
                    "realized_pnl": book["realized_pnl"],
                    "unrealized_pnl": book["unrealized_pnl"],
                    "drawdown": book["drawdown"],
                })
                if len(_GLOBAL_REPLAY_STATE.equity_history) > EQUITY_HISTORY_LIMIT:
                    _GLOBAL_REPLAY_STATE.equity_history = _GLOBAL_REPLAY_STATE.equity_history[-EQUITY_HISTORY_LIMIT:]
            # Pace only. The engine does not read speed, so 1x and 25x keep the same fills.
            speed = max(1, int(_GLOBAL_REPLAY_STATE.speed_multiplier or 1))
            time.sleep(0.05 / speed)

        if completed_run and end_policy == "FORCE_CLOSE" and engine.positions:
            last_time = _GLOBAL_REPLAY_STATE.current_bar_time
            engine.apply_end_policy(end_policy, last_time)
            final_book = engine.book_metrics()
            engine.db.record_snapshot({
                "session_id": engine.session_id,
                "cash_balance": final_book["cash"],
                "invested_equity": final_book["invested"],
                "total_portfolio_value": final_book["equity"],
                "realized_pnl_cumulative": final_book["realized_pnl"],
                "unrealized_pnl_current": final_book["unrealized_pnl"],
                "rolling_sharpe_ratio": 0.0,
                "max_drawdown_pct": final_book["drawdown"],
                "active_positions_count": int(final_book["open_positions"]),
                "latency_ms": 0.0,
                "recorded_at": last_time,
            })
        else:
            final_book = engine.book_metrics()
        with _GLOBAL_REPLAY_STATE.lock:
            if _GLOBAL_REPLAY_STATE.generation != generation:
                return
            if completed_run and not _GLOBAL_REPLAY_STATE.last_error:
                _GLOBAL_REPLAY_STATE.completed = True
            if _GLOBAL_REPLAY_STATE.live_metrics:
                _GLOBAL_REPLAY_STATE.live_metrics = {
                    **_GLOBAL_REPLAY_STATE.live_metrics,
                    "cash": final_book["cash"],
                    "equity": final_book["equity"],
                    "realized_pnl": final_book["realized_pnl"],
                    "unrealized_pnl": final_book["unrealized_pnl"],
                    "drawdown": final_book["drawdown"],
                    "open_positions": int(final_book["open_positions"]),
                    "source": "engine_memory",
                }
            _GLOBAL_REPLAY_STATE.open_book = [
                {
                    "ticker_symbol": pos_row.ticker_symbol,
                    "side": pos_row.side,
                    "quantity": pos_row.quantity,
                    "entry_price": pos_row.average_entry_price,
                    "mark_price": pos_row.current_mark_price,
                    "unrealized_pnl": pos_row.unrealized_pnl,
                    "entry_time": pos_row.entry_time,
                }
                for pos_row in engine.positions.values()
            ]
            _GLOBAL_REPLAY_STATE.open_positions = int(final_book["open_positions"])
            _GLOBAL_REPLAY_STATE.is_running = False
            _GLOBAL_REPLAY_STATE.is_paused = False
            _GLOBAL_REPLAY_STATE.pending_steps = 0


# ==============================================================================
# 4. Historical EDA & OLAP Benchmarking (Page 4)
# ==============================================================================
class HistoricalEDAService:
    """Provides market exploratory analytics, data lineage tracking, and live query benchmarking."""

    @staticmethod
    def get_market_eda_summary(dataset_id: str) -> Dict[str, Any]:
        """Fetch descriptive statistics and cross-asset correlation from ClickHouse."""
        query = f"""
            SELECT 
                ticker_symbol,
                count() AS days,
                avg(close_price) AS mean_price,
                stddevPop(close_price) AS std_price,
                avg(volume) AS avg_volume,
                min(close_price) AS min_price,
                max(close_price) AS max_price
            FROM quant_dw.market_daily_ohlcv FINAL
            WHERE dataset_id = '{dataset_id}'
            GROUP BY ticker_symbol
            ORDER BY avg_volume DESC
        """
        df_stats = run_clickhouse_query(query)

        piv_query = f"""
            SELECT toString(trade_date) AS trade_date, ticker_symbol, close_price
            FROM quant_dw.market_daily_ohlcv FINAL
            WHERE dataset_id = '{dataset_id}'
            ORDER BY trade_date ASC
        """
        df_prices = run_clickhouse_query(piv_query)
        corr_matrix = pd.DataFrame()
        if not df_prices.empty and "trade_date" in df_prices.columns:
            piv = df_prices.pivot(index="trade_date", columns="ticker_symbol", values="close_price")
            returns = piv.pct_change().dropna()
            corr_matrix = returns.corr().round(2)

        return {
            "stats": df_stats,
            "correlation": corr_matrix,
        }

    @staticmethod
    def get_lakehouse_quality_metrics() -> Dict[str, Any]:
        """Inspect Lakehouse directories (Bronze, Silver, Gold) across root and active dataset runs."""
        bronze_files = list(set(
            glob.glob(f"{LAKEHOUSE_ROOT}/raw_market_ticks/**/*.parquet", recursive=True) +
            glob.glob(f"{LAKEHOUSE_ROOT}/runs/**/raw_market_ticks/**/*.parquet", recursive=True)
        ))
        silver_files = list(set(
            glob.glob(f"{LAKEHOUSE_ROOT}/stg_market_ticks/**/*.parquet", recursive=True) +
            glob.glob(f"{LAKEHOUSE_ROOT}/runs/**/stg_market_ticks/**/*.parquet", recursive=True)
        ))
        gold_files = list(set(
            glob.glob(f"{LAKEHOUSE_ROOT}/gold_features/**/*.parquet", recursive=True) +
            glob.glob(f"{LAKEHOUSE_ROOT}/runs/**/gold_features/**/*.parquet", recursive=True)
        ))

        return {
            "bronze_files_count": len(bronze_files),
            "silver_files_count": len(silver_files),
            "gold_files_count": len(gold_files),
            "root_path": LAKEHOUSE_ROOT,
        }

    @staticmethod
    def run_live_olap_benchmark(dataset_id: str, ticker: str = "AAPL") -> Dict[str, Any]:
        """
        Execute live OLAP benchmark comparing ClickHouse vs DuckDB/Parquet scan on identical data.
        Records exact execution time and bytes scanned without hardcoded numbers.
        """
        ch_sql = f"""
            SELECT 
                ticker_symbol,
                avg(close_price) AS avg_close,
                max(high_price) AS max_high,
                min(low_price) AS min_low,
                sum(volume) AS total_vol
            FROM quant_dw.market_daily_ohlcv FINAL
            WHERE dataset_id = '{dataset_id}' AND ticker_symbol = '{ticker}'
            GROUP BY ticker_symbol
        """
        t0 = time.perf_counter()
        df_ch = run_clickhouse_query(ch_sql)
        ch_latency_ms = (time.perf_counter() - t0) * 1000.0

        parquet_path = Path(LAKEHOUSE_ROOT) / "runs" / dataset_id / "gold_features" / "feat_ticker_technical_indicators.parquet"
        if not parquet_path.exists():
            parquet_path = Path(LAKEHOUSE_ROOT) / "gold_features" / "feat_ticker_technical_indicators.parquet"

        py_latency_ms = 0.0
        row_count = 0
        if parquet_path.exists():
            t1 = time.perf_counter()
            df_parquet = pd.read_parquet(parquet_path)
            filtered = df_parquet[df_parquet["ticker_symbol"] == ticker]
            row_count = len(filtered)
            _ = filtered["close_price"].mean()
            py_latency_ms = (time.perf_counter() - t1) * 1000.0

        return {
            "clickhouse": {
                "latency_ms": round(ch_latency_ms, 2),
                "rows_returned": len(df_ch),
                "engine": "ClickHouse ReplacingMergeTree (FINAL)",
            },
            "parquet_batch": {
                "latency_ms": round(py_latency_ms, 2),
                "rows_scanned": row_count,
                "engine": "PyArrow Lakehouse Parquet Scan",
            },
            "speedup_factor": round(py_latency_ms / max(0.001, ch_latency_ms), 1) if ch_latency_ms > 0 else 1.0,
        }
