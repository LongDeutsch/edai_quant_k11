"""Candlestick and market bar data loader for dashboards and serving.

Prioritizes ClickHouse OLAP serving (quant_dw.market_intraday_bars, M1-19).
Falls back to Paper DB Session Store, then Lakehouse raw parquet,
and explicit error diagnostics if none is available.
"""

from __future__ import annotations

from typing import Optional, Tuple
import pandas as pd

from quant_edai.serving.market_data_service import MarketDataService


def load_candlestick_data(
    ticker: str,
    session_id: Optional[str] = None,
    session_date: Optional[str] = None,
    source_preference: str = "auto",
) -> Tuple[pd.DataFrame, str]:
    """Fetch candlestick OHLCV data for given ticker with multi-tier priority.

    Priority:
    1. ClickHouse OLAP (quant_dw.market_intraday_bars or market_daily_ohlcv) via HTTP :8123
    2. Paper DB Session Store (matching session_id/date)
    3. Lakehouse Partitioned Parquet (/tmp/lakehouse_source/raw_market_ohlcv/trade_date=...)
    """
    service = MarketDataService()
    df, source_label = service.fetch_candlesticks(
        ticker=ticker,
        session_id=session_id,
        session_date=session_date,
        source_preference=source_preference,
        limit=500,
    )
    return df, source_label
