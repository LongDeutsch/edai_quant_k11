"""Feast Feature View Definitions for US Equities Market Intelligence."""

from datetime import timedelta
import os
from pathlib import Path

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from quant_edai.ml.feast.entities import ticker_symbol

# Locate source Parquet file produced by DP3
DEFAULT_PARQUET_PATH = os.path.join(
    os.getenv("OUT_DIR", "/tmp/lakehouse_source"),
    "gold_features",
    "feat_ticker_technical_indicators.parquet",
)
if not os.path.exists(DEFAULT_PARQUET_PATH):
    # Fallback to local evidence or root path
    fallback = "evidence/phase3/feat_ticker_technical_indicators.parquet"
    if os.path.exists(fallback):
        DEFAULT_PARQUET_PATH = os.path.abspath(fallback)

source_technical_indicators = FileSource(
    name="src_ticker_technical_indicators",
    path=DEFAULT_PARQUET_PATH,
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# FeatureView: Technical Indicators (MVP Core features strictly matching Gold DDL)
fv_ticker_technical_indicators = FeatureView(
    name="fv_ticker_technical_indicators",
    entities=[ticker_symbol],
    ttl=timedelta(days=365),
    schema=[
        Field(name="close_price", dtype=Float64, description="Closing price"),
        Field(name="sma_20", dtype=Float64, description="20-day Simple Moving Average"),
        Field(name="sma_50", dtype=Float64, description="50-day Simple Moving Average"),
        Field(name="sma_ratio_20_50", dtype=Float64, description="Ratio of 20 SMA to 50 SMA"),
        Field(name="rsi_14d", dtype=Float64, description="14-day Relative Strength Index"),
        Field(name="macd_12_26", dtype=Float64, description="MACD line (12 EMA - 26 EMA)"),
        Field(name="macd_signal_9", dtype=Float64, description="MACD 9-day Signal line"),
        Field(name="macd_histogram", dtype=Float64, description="MACD histogram"),
        Field(name="rolling_volatility_30d", dtype=Float64, description="30-day realized volatility"),
        Field(name="intraday_return_1d", dtype=Float64, description="Close-to-close daily return"),
    ],
    online=True,
    source=source_technical_indicators,
    tags={"team": "quant_research", "layer": "gold", "tier": "mvp_core"},
)
