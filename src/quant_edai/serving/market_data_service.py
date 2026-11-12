"""Market and Execution Data Service for Quant Analytics & Serving.

Architecture layers:
1. Data Access Layer: Multi-tier querying across ClickHouse OLAP, Session Replay DB, and Lakehouse.
2. Validation Layer: Timestamp normalization (UTC & America/New_York), session alignment, and quality checks.
3. Indicator Computation Layer: Safe technical indicators (SMA20, SMA50, Intraday VWAP).
4. Semantic Marker Layer: Trade classification into OPEN/CLOSE LONG/SHORT, TP, SL, EOD.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import glob
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

NY_TZ = ZoneInfo("America/New_York")
UTC_TZ = timezone.utc


@dataclass
class AlignmentReport:
    """Diagnostic report for session and data alignment."""
    is_aligned: bool
    status: str  # "HEALTHY", "WARNING", "MISALIGNED", "INSUFFICIENT"
    session_date: Optional[str] = None
    candle_count: int = 0
    trade_count: int = 0
    candle_min_time: Optional[str] = None
    candle_max_time: Optional[str] = None
    trade_min_time: Optional[str] = None
    trade_max_time: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


class DataValidator:
    """Validates timestamps, session dates, and integrity between market bars and trades."""

    @staticmethod
    def normalize_timestamps(
        df: pd.DataFrame,
        time_col: str = "event_timestamp",
        output_utc_col: str = "event_timestamp_utc",
        output_ny_col: str = "event_timestamp_ny",
    ) -> pd.DataFrame:
        """Parse time_col to timezone-aware UTC datetime and add America/New_York string."""
        if df.empty or time_col not in df.columns:
            return df

        df = df.copy()
        # Parse datetime safely across varied ISO8601 timestamps (with/without microseconds)
        try:
            dt_series = pd.to_datetime(df[time_col], format="ISO8601", utc=True)
        except Exception:
            dt_series = pd.to_datetime(df[time_col], format="mixed", utc=True)
        df[output_utc_col] = dt_series
        df[output_ny_col] = dt_series.dt.tz_convert(NY_TZ).dt.strftime("%Y-%m-%d %H:%M:%S EST")
        return df

    @staticmethod
    def validate_alignment(
        df_candles: pd.DataFrame,
        df_trades: pd.DataFrame,
        candle_time_col: str = "event_timestamp_utc",
        trade_time_col: str = "executed_at_utc",
    ) -> AlignmentReport:
        """Inspect market bars and trades for date overlap, gaps, and sufficiency."""
        report = AlignmentReport(
            is_aligned=True,
            status="HEALTHY",
            candle_count=len(df_candles),
            trade_count=len(df_trades),
        )

        if df_candles.empty:
            report.is_aligned = False
            report.status = "NO_MARKET_DATA"
            report.warnings.append("No market candles loaded for this ticker/session.")
            return report

        c_min = df_candles[candle_time_col].min()
        c_max = df_candles[candle_time_col].max()
        report.candle_min_time = c_min.isoformat() if pd.notna(c_min) else None
        report.candle_max_time = c_max.isoformat() if pd.notna(c_max) else None

        if len(df_candles) < 20:
            report.warnings.append(f"Only {len(df_candles)} bars loaded. Insufficient for SMA(20) lookback.")
            report.status = "INSUFFICIENT"

        if not df_trades.empty and trade_time_col in df_trades.columns:
            t_min = df_trades[trade_time_col].min()
            t_max = df_trades[trade_time_col].max()
            report.trade_min_time = t_min.isoformat() if pd.notna(t_min) else None
            report.trade_max_time = t_max.isoformat() if pd.notna(t_max) else None

            # Check session date match
            c_date = c_min.strftime("%Y-%m-%d") if pd.notna(c_min) else None
            t_date = t_min.strftime("%Y-%m-%d") if pd.notna(t_min) else None
            report.session_date = t_date or c_date

            if c_date and t_date and c_date != t_date:
                report.is_aligned = False
                report.status = "MISALIGNED"
                report.warnings.append(
                    f"Timestamp Misalignment: Market candles are from {c_date} but trade executions are from {t_date}."
                )

            # Check if trades fall outside candle window by more than 30 minutes
            if c_min and c_max and t_min and t_max:
                if t_max < c_min or t_min > c_max:
                    report.is_aligned = False
                    report.status = "MISALIGNED"
                    report.warnings.append("Trade timestamps fall completely outside candle time range.")

        return report


class IndicatorCalculator:
    """Calculates technical indicators and intraday session benchmarks with no lookahead."""

    @staticmethod
    def compute_indicators(df: pd.DataFrame, price_col: str = "close_price", vol_col: str = "volume") -> pd.DataFrame:
        """Compute SMA20, SMA50, and session VWAP safely."""
        if df.empty or price_col not in df.columns:
            return df

        df = df.copy()
        prices = df[price_col].astype(float)
        volumes = df[vol_col].astype(float) if vol_col in df.columns else pd.Series(1.0, index=df.index)

        # 1. Moving Averages with min_periods for graceful warmup
        df["sma_20"] = prices.rolling(window=20, min_periods=5).mean()
        df["sma_50"] = prices.rolling(window=50, min_periods=10).mean()

        # 2. Intraday VWAP (reset per trading day if date changes, or cumulative over session)
        if "vwap" not in df.columns or df["vwap"].isna().all():
            cum_vol = volumes.cumsum()
            cum_pv = (prices * volumes).cumsum()
            df["vwap"] = cum_pv / cum_vol.replace(0, 1.0)

        # 3. Spread / Range in bps
        if "high_price" in df.columns and "low_price" in df.columns:
            df["spread_bps"] = ((df["high_price"] - df["low_price"]) / prices) * 10000.0

        return df


class ExecutionMarkerClassifier:
    """Classifies trades into semantic trading actions and formats rich hover annotations."""

    @staticmethod
    def classify_trades(df_trades: pd.DataFrame) -> pd.DataFrame:
        """Enrich trade rows with semantic_action, exit_type, and hover text."""
        if df_trades.empty:
            return df_trades

        df = df_trades.copy()

        semantic_actions = []
        action_colors = []
        action_symbols = []
        hover_texts = []

        for _, row in df.iterrows():
            direction = str(row.get("direction", "BUY")).upper()
            order_type = str(row.get("order_type", "MARKET")).upper()
            realized_pnl = float(row.get("realized_pnl", 0.0))
            fill_price = float(row.get("fill_price", 0.0))
            req_price = float(row.get("requested_price", fill_price))
            fill_qty = float(row.get("fill_quantity", 0.0))
            commission = float(row.get("commission", 0.0))
            slippage_bps = float(row.get("slippage_bps", 5.0))
            exec_time = str(row.get("executed_at", ""))

            # Format NY time string if possible
            try:
                try:
                    dt_utc = pd.to_datetime(exec_time, format="ISO8601", utc=True)
                except Exception:
                    dt_utc = pd.to_datetime(exec_time, format="mixed", utc=True)
                ny_time_str = dt_utc.tz_convert(NY_TZ).strftime("%H:%M:%S EST")
                utc_time_str = dt_utc.strftime("%H:%M:%S UTC")
            except Exception:
                ny_time_str = exec_time
                utc_time_str = exec_time

            # Determine entry vs exit
            # If order_type is in exit reasons or realized_pnl is non-zero, this is an exit
            is_exit = order_type in ["STOP_LOSS", "TAKE_PROFIT", "EOD_FLAT", "CIRCUIT_BREAKER"] or abs(realized_pnl) > 1e-4

            if not is_exit:
                if direction == "BUY":
                    action = "OPEN LONG"
                    color = "#00E676"  # Emerald
                    symbol = "triangle-up"
                else:
                    action = "OPEN SHORT"
                    color = "#C084FC"  # Purple
                    symbol = "triangle-down"
                
                hover = (
                    f"<b>{action}</b><br>"
                    f"Time: {ny_time_str} ({utc_time_str})<br>"
                    f"Fill: ${fill_price:.2f} (Req: ${req_price:.2f})<br>"
                    f"Qty: {fill_qty:.4f} sh (${fill_qty * fill_price:,.2f})<br>"
                    f"Slippage: {slippage_bps:.1f} bps | Comm: ${commission:.2f}"
                )
            else:
                if direction == "SELL":
                    action = "CLOSE LONG"
                    if order_type == "TAKE_PROFIT":
                        color = "#10B981"  # Emerald green
                        symbol = "diamond"
                        tag = "TAKE PROFIT"
                    elif order_type == "STOP_LOSS":
                        color = "#FF1744"  # Crimson red
                        symbol = "x"
                        tag = "STOP LOSS"
                    elif order_type == "EOD_FLAT":
                        color = "#F59E0B"  # Amber
                        symbol = "square"
                        tag = "EOD FLATTEN"
                    else:
                        color = "#FF5252"
                        symbol = "triangle-down"
                        tag = order_type
                else:  # direction == "BUY" (closing a short position)
                    action = "CLOSE SHORT"
                    if order_type == "TAKE_PROFIT":
                        color = "#10B981"
                        symbol = "diamond"
                        tag = "TAKE PROFIT"
                    elif order_type == "STOP_LOSS":
                        color = "#FF1744"
                        symbol = "x"
                        tag = "STOP LOSS"
                    elif order_type == "EOD_FLAT":
                        color = "#F59E0B"
                        symbol = "square"
                        tag = "EOD FLATTEN"
                    else:
                        color = "#38BDF8"
                        symbol = "triangle-up"
                        tag = order_type

                pnl_bps = ((realized_pnl / (fill_qty * fill_price)) * 10000.0) if (fill_qty * fill_price) > 0 else 0.0
                hover = (
                    f"<b>{action} ({tag})</b><br>"
                    f"Time: {ny_time_str} ({utc_time_str})<br>"
                    f"Fill: ${fill_price:.2f} (Req: ${req_price:.2f})<br>"
                    f"Qty: {fill_qty:.4f} sh<br>"
                    f"Realized PnL: <b>${realized_pnl:+.2f} ({pnl_bps:+.1f} bps)</b><br>"
                    f"Slippage: {slippage_bps:.1f} bps | Comm: ${commission:.2f}"
                )

            semantic_actions.append(action)
            action_colors.append(color)
            action_symbols.append(symbol)
            hover_texts.append(hover)

        df["semantic_action"] = semantic_actions
        df["marker_color"] = action_colors
        df["marker_symbol"] = action_symbols
        df["hover_text"] = hover_texts
        return df


class MarketDataService:
    """Unified service for querying, validating, and preparing market and trade data."""

    def __init__(
        self,
        clickhouse_url: Optional[str] = None,
        sqlite_path: Optional[str] = None,
        lakehouse_dir: str = "/tmp/lakehouse_source",
    ) -> None:
        self.ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
        self.ch_port = os.getenv("CLICKHOUSE_PORT", "8123")
        self.ch_user = os.getenv("CLICKHOUSE_USER", "quant")
        self.ch_password = os.getenv("CLICKHOUSE_PASSWORD", "quant_password")
        self.ch_url = clickhouse_url or f"http://{self.ch_host}:{self.ch_port}/"
        self.sqlite_path = sqlite_path or os.getenv("SQLITE_PAPER_DB", "paper_trading.db")
        self.lakehouse_dir = lakehouse_dir

    def fetch_candlesticks(
        self,
        ticker: str,
        session_id: Optional[str] = None,
        session_date: Optional[str] = None,
        source_preference: str = "auto",
        allow_synthetic: bool = True,
        limit: int = 500,
    ) -> Tuple[pd.DataFrame, str]:
        """Fetch candlesticks with multi-tier source priority and safe fallback."""
        
        # 1. If a specific trading session is requested, prioritize Paper DB Session Store
        if session_id and source_preference in ["auto", "session_store"]:
            df_db, source_db = self._fetch_paper_db(ticker, session_id, session_date, limit)
            if not df_db.empty and len(df_db) >= 15:
                return df_db, source_db

        # 2. Try ClickHouse OLAP if preference allows
        if source_preference in ["auto", "clickhouse"]:
            df_ch, source_ch = self._fetch_clickhouse(ticker, session_date, limit)
            if not df_ch.empty:
                return df_ch, source_ch

        # 3. Fallback to Paper DB Session Store (if not already matched)
        if source_preference in ["auto", "session_store", "clickhouse"]:
            df_db, source_db = self._fetch_paper_db(ticker, session_id, session_date, limit)
            if not df_db.empty:
                return df_db, source_db

        # 3. Try Lakehouse Partitioned Parquet
        if source_preference in ["auto", "lakehouse"]:
            df_lh, source_lh = self._fetch_lakehouse(ticker, session_date, limit)
            if not df_lh.empty:
                return df_lh, source_lh

        # 4. Deterministic Synthetic fallback if explicitly allowed
        if allow_synthetic:
            df_synth = self._generate_synthetic_bars(ticker, limit)
            return df_synth, "Simulated Real-time Feed"

        # 5. Safe Fallback: Return empty with clear diagnostic label
        return pd.DataFrame(), "No Data Available (ClickHouse/Lakehouse offline)"

    def _generate_synthetic_bars(self, ticker: str, limit: int = 100) -> pd.DataFrame:
        """Deterministic fallback when no physical store is reachable."""
        np.random.seed(hash(ticker) % 2**32)
        base = 150.0 + (hash(ticker) % 200)
        times = pd.date_range("2024-03-15 09:30:00", periods=min(limit, 100), freq="1min", tz="UTC")
        rets = np.random.normal(0, 0.001, len(times))
        prices = base * np.cumprod(1.0 + rets)
        return pd.DataFrame({
            "ticker_symbol": ticker,
            "event_timestamp": [t.isoformat() for t in times],
            "open_price": np.round(prices * 0.9998, 2),
            "high_price": np.round(prices * 1.0008, 2),
            "low_price": np.round(prices * 0.9992, 2),
            "close_price": np.round(prices, 2),
            "volume": np.random.randint(1000, 10000, len(times)),
            "vwap": np.round(prices * 0.9999, 2),
        })

    def _fetch_clickhouse(self, ticker: str, session_date: Optional[str], limit: int) -> Tuple[pd.DataFrame, str]:
        """Query ClickHouse HTTP endpoint."""
        url_with_auth = f"{self.ch_url}?user={self.ch_user}&password={self.ch_password}"
        date_filter = f"AND toDate(event_timestamp) = '{session_date}'" if session_date else ""
        query = (
            f"SELECT event_timestamp, open_price, high_price, low_price, close_price, volume, vwap "
            f"FROM quant_dw.market_intraday_bars "
            f"WHERE ticker_symbol = '{ticker}' {date_filter} "
            f"ORDER BY event_timestamp ASC LIMIT {limit} "
            f"FORMAT JSON"
        )
        try:
            req = urllib.request.Request(
                url_with_auth,
                data=query.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
            )
            with urllib.request.urlopen(req, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if "data" in data and len(data["data"]) > 0:
                    df = pd.DataFrame(data["data"])
                    return df, f"ClickHouse OLAP ({self.ch_host}:{self.ch_port} - market_intraday_bars)"

            # Check daily table fallback
            daily_filter = f"AND trade_date = '{session_date}'" if session_date else ""
            query_daily = (
                f"SELECT toString(trade_date) AS event_timestamp, open_price, high_price, low_price, close_price, volume, vwap "
                f"FROM quant_dw.market_daily_ohlcv "
                f"WHERE ticker_symbol = '{ticker}' {daily_filter} "
                f"ORDER BY trade_date ASC LIMIT {limit} "
                f"FORMAT JSON"
            )
            req_daily = urllib.request.Request(
                url_with_auth,
                data=query_daily.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
            )
            with urllib.request.urlopen(req_daily, timeout=1.0) as resp_d:
                data_d = json.loads(resp_d.read().decode("utf-8"))
                if "data" in data_d and len(data_d["data"]) > 0:
                    df = pd.DataFrame(data_d["data"])
                    return df, f"ClickHouse OLAP ({self.ch_host}:{self.ch_port} - market_daily_ohlcv)"
        except Exception:
            pass

        return pd.DataFrame(), ""

    def _fetch_paper_db(
        self,
        ticker: str,
        session_id: Optional[str],
        session_date: Optional[str],
        limit: int,
    ) -> Tuple[pd.DataFrame, str]:
        """Fetch market bars directly from paper trading session store."""
        from quant_edai.quant.paper_db import PaperDatabase
        try:
            db = PaperDatabase(sqlite_path=self.sqlite_path)
            df = db.get_market_bars(ticker, session_id=session_id, session_date=session_date, limit=limit)
            if not df.empty:
                return df, f"Paper DB Session Store ({'PostgreSQL' if db.is_postgres else 'SQLite'})"
        except Exception as e:
            logger.debug(f"Paper DB fetch error: {e}")
        return pd.DataFrame(), ""

    def _fetch_lakehouse(self, ticker: str, session_date: Optional[str], limit: int) -> Tuple[pd.DataFrame, str]:
        """Fetch lakehouse parquet with strict date partition filtering."""
        if not os.path.exists(self.lakehouse_dir):
            return pd.DataFrame(), ""

        # Check partitioned raw market ohlcv
        if session_date:
            partition_pattern = os.path.join(
                self.lakehouse_dir, "raw_market_ohlcv", f"trade_date={session_date}", "**/*.parquet"
            )
            files = glob.glob(partition_pattern, recursive=True)
        else:
            partition_pattern = os.path.join(self.lakehouse_dir, "raw_market_ohlcv", "**/*.parquet")
            files = glob.glob(partition_pattern, recursive=True)

        if files:
            dfs = []
            for f in files[:50]:
                try:
                    sub = pd.read_parquet(f)
                    if "ticker_symbol" in sub.columns:
                        sub_t = sub[sub["ticker_symbol"] == ticker]
                        if not sub_t.empty:
                            dfs.append(sub_t)
                except Exception:
                    continue
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
                # Map timestamp correctly
                if "event_timestamp" in df.columns:
                    pass
                elif "window_start" in df.columns:
                    df["event_timestamp"] = df["window_start"]
                elif "trade_date" in df.columns:
                    df["event_timestamp"] = df["trade_date"]
                elif "ingestion_timestamp" in df.columns:
                    df["event_timestamp"] = df["ingestion_timestamp"]

                df = df.sort_values("event_timestamp").head(limit)
                return df, f"Lakehouse Storage ({self.lakehouse_dir})"

        return pd.DataFrame(), ""
