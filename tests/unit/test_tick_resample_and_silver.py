"""Unit tests for Phase B: Silver Spark Dedup & Gold Resample Pipeline."""

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
from quant_edai.processing.spark.resample_job import SparkResampleJob


@pytest.fixture(scope="module")
def staged_tick_data(tmp_path_factory):
    """Generates synthetic Bronze ticks crossing schema evolution boundary."""
    out_dir = tmp_path_factory.mktemp("lakehouse_phase_b")
    gen = QuantDataGenerator(
        seed=101,
        mode="smoke",
        scale=0.01,
        out_dir=str(out_dir),
        start_date="2024-05-25",
        end_date="2024-06-05",  # crosses 2024-06-01
    )
    stats = gen.run()
    return out_dir / "runs" / stats["dataset_id"]


def test_silver_spark_dedup_and_schema_unification(staged_tick_data, tmp_path):
    """Verifies that Silver Dedup removes 2% duplicate trade_id and normalizes pre-boundary ticks."""
    target_silver_ohlcv = tmp_path / "stg_market_ohlcv"
    dedup_job = DeduplicationAndSchemaEvolutionJob(
        source_dir=str(staged_tick_data),
        target_dir=str(target_silver_ohlcv),
    )
    stats = dedup_job.run()

    # 1. Duplicates removed
    assert stats["duplicates_removed"] > 0
    assert 0.01 <= stats["duplicate_elimination_rate"] <= 0.035

    # 2. Inspect Silver Ticks (stg_market_ticks)
    silver_ticks_path = staged_tick_data / "stg_market_ticks"
    assert silver_ticks_path.exists()
    df_silver_ticks = pd.read_parquet(silver_ticks_path)

    # Check 100% uniqueness of trade_id
    assert df_silver_ticks["trade_id"].duplicated().sum() == 0

    # Check that schema evolution is normalized (no null quote_condition or bid/ask)
    assert df_silver_ticks["quote_condition"].isna().sum() == 0
    assert df_silver_ticks["bid_price"].isna().sum() == 0
    assert df_silver_ticks["ask_price"].isna().sum() == 0
    assert (df_silver_ticks["bid_price"] <= df_silver_ticks["ask_price"]).all()

    # 3. Check backward-compatible stg_market_ohlcv
    assert target_silver_ohlcv.exists()
    df_silver_ohlcv = pd.read_parquet(target_silver_ohlcv)
    assert df_silver_ohlcv.duplicated(subset=["ticker_symbol", "trade_date"]).sum() == 0
    assert "vwap" in df_silver_ohlcv.columns
    assert "trade_count" in df_silver_ohlcv.columns


def test_gold_spark_resample_bars_and_consistency(staged_tick_data, tmp_path):
    """Verifies Gold 1m and Daily bar resampling and mathematical consistency."""
    gold_1m_dir = tmp_path / "gold_market_bars_1m"
    gold_daily_dir = tmp_path / "gold_market_bars_daily"

    resample_job = SparkResampleJob(
        source_ticks_dir=str(staged_tick_data / "stg_market_ticks"),
        gold_1m_dir=str(gold_1m_dir),
        gold_daily_dir=str(gold_daily_dir),
    )
    stats = resample_job.run()

    assert stats["bars_1m_generated"] > 0
    assert stats["bars_daily_generated"] > 0

    df_1m = pd.read_parquet(gold_1m_dir)
    df_daily = pd.read_parquet(gold_daily_dir)

    # Verify column schemas
    assert {"bar_id", "ticker_symbol", "bar_timestamp", "trade_date", "open_price", "high_price", "low_price", "close_price", "volume", "vwap", "trade_count"}.issubset(set(df_1m.columns))
    assert {"trade_id", "ticker_symbol", "trade_date", "open_price", "high_price", "low_price", "close_price", "volume", "vwap", "trade_count"}.issubset(set(df_daily.columns))

    # Verify Mathematical Consistency: Sum(1m volume) == Daily volume for each ticker and date
    df_1m_agg = df_1m.groupby(["ticker_symbol", "trade_date"], observed=False).agg(
        sum_vol=("volume", "sum"),
        max_high=("high_price", "max"),
        min_low=("low_price", "min"),
        trade_count_sum=("trade_count", "sum"),
    ).reset_index()

    merged = df_daily.merge(df_1m_agg, on=["ticker_symbol", "trade_date"], how="inner")
    assert len(merged) > 0

    # 1. Volume conservation
    assert (merged["volume"] == merged["sum_vol"]).all()
    # 2. Trade count conservation
    assert (merged["trade_count"] == merged["trade_count_sum"]).all()
    # 3. High/Low bound conservation
    assert (merged["high_price"] == merged["max_high"]).all()
    assert (merged["low_price"] == merged["min_low"]).all()
