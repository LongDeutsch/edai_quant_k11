"""Unit & integration tests for Phase 2: Canonical 4Y Lakehouse Integration & Data Quality Verification."""

from __future__ import annotations

import json
from pathlib import Path
import pytest
import pandas as pd
import numpy as np

from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
from quant_edai.processing.spark.resample_job import SparkResampleJob
from quant_edai.processing.features.feature_calculator import compute_technical_indicators, run_feature_calculation
from quant_edai.serving.clickhouse_sync import ClickHouseSyncManager, align_to_schema


CANONICAL_DIR = Path("/tmp/lakehouse_source/runs/ds_canonical_4y_pilot")


@pytest.fixture(scope="module")
def canonical_pilot_dir():
    """Verifies that ds_canonical_4y_pilot exists; if not, skips tests."""
    if not CANONICAL_DIR.exists():
        pytest.skip(f"Canonical pilot directory {CANONICAL_DIR} does not exist. Run Phase 1 generator first.")
    return CANONICAL_DIR


def test_bronze_to_silver_deduplication(canonical_pilot_dir):
    """Verifies that Bronze raw ticks are cleanly deduplicated into Silver stg_market_ticks."""
    raw_ticks_path = canonical_pilot_dir / "raw_market_ticks"
    df_raw = pd.read_parquet(raw_ticks_path)
    total_raw = len(df_raw)

    dedup_job = DeduplicationAndSchemaEvolutionJob(
        source_dir=str(canonical_pilot_dir),
        target_dir=str(canonical_pilot_dir / "stg_market_ohlcv"),
        dataset_id="ds_canonical_4y_pilot",
    )
    stats = dedup_job.run_sim()

    assert stats["total_raw_rows"] == total_raw
    assert 0.015 <= stats["duplicate_elimination_rate"] <= 0.025
    assert stats["duplicates_removed"] > 0
    assert stats["deduplicated_rows"] == total_raw - stats["duplicates_removed"]

    # Verify silver ticks parquet exists and is deduplicated
    silver_ticks_path = canonical_pilot_dir / "stg_market_ticks"
    assert silver_ticks_path.exists()
    df_silver = pd.read_parquet(silver_ticks_path)
    assert len(df_silver) == stats["deduplicated_rows"]
    assert df_silver["trade_id"].nunique() == len(df_silver)

    # Verify schema evolution handling
    assert "quote_condition" in df_silver.columns
    assert "bid_price" in df_silver.columns
    assert "ask_price" in df_silver.columns
    assert df_silver["bid_price"].notna().all()
    assert df_silver["ask_price"].notna().all()
    assert (df_silver["price"] > 0).all()
    assert (df_silver["size"] > 0).all()


def test_silver_to_gold_resampling_ohlcv(canonical_pilot_dir):
    """Verifies that 1m and Daily Gold OHLCV bars match raw Silver ticks 100% exactly."""
    resample_job = SparkResampleJob(
        source_ticks_dir=str(canonical_pilot_dir / "stg_market_ticks"),
        gold_1m_dir=str(canonical_pilot_dir / "gold_market_bars_1m"),
        gold_daily_dir=str(canonical_pilot_dir / "gold_market_bars_daily"),
        dataset_id="ds_canonical_4y_pilot",
    )
    stats = resample_job.run_sim()

    assert stats["bars_daily_generated"] == 1044 * 16  # 16,704
    assert stats["trading_days"] == 1044
    assert stats["tickers_count"] == 16
    assert stats["bars_1m_generated"] > 70000

    # Reconcile random sample: NVDA on 2024-01-03
    df_ticks = pd.read_parquet(canonical_pilot_dir / "stg_market_ticks")
    df_daily = pd.read_parquet(canonical_pilot_dir / "gold_market_bars_daily")

    sample_ticker = "NVDA"
    sample_date = "2024-01-03"

    t_grp = df_ticks[(df_ticks["ticker_symbol"] == sample_ticker) & (df_ticks["trade_date"] == sample_date)].sort_values("event_timestamp")
    d_row = df_daily[(df_daily["ticker_symbol"] == sample_ticker) & (df_daily["trade_date"] == sample_date)].iloc[0]

    assert round(float(t_grp.iloc[0]["price"]), 2) == d_row["open_price"]
    assert round(float(t_grp.iloc[-1]["price"]), 2) == d_row["close_price"]
    assert round(float(t_grp["price"].max()), 2) == d_row["high_price"]
    assert round(float(t_grp["price"].min()), 2) == d_row["low_price"]
    assert int(t_grp["size"].sum()) == d_row["volume"]

    # Verify split_id is preserved
    assert "split_id" in df_daily.columns
    assert set(df_daily["split_id"].unique()) == {"IS", "OOS", "TRADING"}


def test_intraday_aggregation_consistency(canonical_pilot_dir):
    """Verifies that 5-minute bars derived from 1-minute bars match direct tick aggregations."""
    df_1m = pd.read_parquet(canonical_pilot_dir / "gold_market_bars_1m")
    df_ticks = pd.read_parquet(canonical_pilot_dir / "stg_market_ticks")

    df_1m["ts"] = pd.to_datetime(df_1m["window_start"], utc=True)
    df_ticks["ts"] = pd.to_datetime(df_ticks["event_timestamp"], utc=True)

    # Check for MSFT on 2023-05-10
    sample_ticker = "MSFT"
    sample_date = "2023-05-10"
    m_1m = df_1m[(df_1m["ticker_symbol"] == sample_ticker) & (df_1m["trade_date"] == sample_date)].sort_values("ts")
    m_t = df_ticks[(df_ticks["ticker_symbol"] == sample_ticker) & (df_ticks["trade_date"] == sample_date)].sort_values("ts")

    m_1m["bar_5m"] = m_1m["ts"].dt.floor("5min")
    m_t["bar_5m"] = m_t["ts"].dt.floor("5min")

    for b_time, grp_1m in m_1m.groupby("bar_5m"):
        grp_t = m_t[m_t["bar_5m"] == b_time].sort_values("ts")
        assert grp_1m.iloc[0]["open_price"] == round(float(grp_t.iloc[0]["price"]), 2)
        assert grp_1m.iloc[-1]["close_price"] == round(float(grp_t.iloc[-1]["price"]), 2)
        assert grp_1m["high_price"].max() == round(float(grp_t["price"].max()), 2)
        assert grp_1m["low_price"].min() == round(float(grp_t["price"].min()), 2)
        assert grp_1m["volume"].sum() == int(grp_t["size"].sum())


def test_feature_calculation_and_split_isolation(canonical_pilot_dir):
    """Verifies feature computation on canonical gold daily OHLCV and split preservation."""
    gold_daily_path = canonical_pilot_dir / "gold_market_bars_daily"
    target_feat_path = canonical_pilot_dir / "gold_features" / "feat_ticker_technical_indicators.parquet"

    metrics = run_feature_calculation(
        source_path=str(gold_daily_path),
        target_path=str(target_feat_path),
        engine="sim",
    )
    assert metrics["output_rows"] == 16704
    assert target_feat_path.exists()

    df_feat = pd.read_parquet(target_feat_path)
    assert {"sma_20", "sma_50", "rsi_14d", "macd_histogram", "rolling_volatility_30d"}.issubset(set(df_feat.columns))
    assert df_feat["close_price"].notna().all()
    assert "split_id" in df_feat.columns


def test_schema_alignment_and_split_derivation():
    """Verifies align_to_schema in clickhouse_sync properly derives split_id."""
    df_sample = pd.DataFrame({
        "ticker_symbol": ["AAPL", "AAPL", "AAPL"],
        "trade_date": ["2023-01-15", "2025-08-20", "2026-07-10"],
        "open_price": [150.0, 160.0, 170.0],
        "high_price": [152.0, 162.0, 172.0],
        "low_price": [149.0, 159.0, 169.0],
        "close_price": [151.0, 161.0, 171.0],
        "volume": [1000, 2000, 3000],
    })

    out_df, report = align_to_schema(df_sample, "market_daily_ohlcv", dataset_id="ds_test")
    assert "split_id" in out_df.columns
    assert list(out_df["split_id"]) == ["IS", "OOS", "TRADING"]
    assert (out_df["dataset_id"] == "ds_test").all()
