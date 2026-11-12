"""Unit tests for Tick-First Architecture QuantDataGenerator."""

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator


def test_tick_generator_smoke(tmp_path):
    """Verifies Tick-First generation, schema evolution, skew, duplicates and cardinality."""
    out_dir = tmp_path / "lakehouse_ticks"
    gen = QuantDataGenerator(
        seed=42,
        mode="smoke",
        scale=0.005,
        out_dir=str(out_dir),
        start_date="2024-05-28",
        end_date="2024-06-05",  # crosses schema evolution boundary 2024-06-01
    )
    stats = gen.run()
    run_dir = gen.out_dir  # immutable runs/{dataset_id}

    # 1. Output directories exist
    ticks_dir = run_dir / "raw_market_ticks"
    assert ticks_dir.exists()
    assert (run_dir / "raw_market_ohlcv").exists()
    assert (run_dir / "dim_companies" / "dim_companies.parquet").exists()
    assert (run_dir / "raw_financial_news").exists()
    assert (run_dir / "label_ticker_daily_return" / "labels.parquet").exists()
    assert stats["dataset_id"]
    assert (out_dir / "CURRENT_DATASET_ID").exists()

    # 2. Read generated raw ticks
    df_ticks = pd.read_parquet(ticks_dir)
    assert len(df_ticks) > 0

    # Required raw tick columns
    required_cols = {
        "trade_id",
        "ticker_symbol",
        "trade_date",
        "price",
        "size",
        "event_timestamp",
        "ingestion_timestamp",
        "exchange",
    }
    assert required_cols.issubset(set(df_ticks.columns))

    # 3. Test Schema Evolution (boundary 2024-06-01)
    trade_date_str = df_ticks["trade_date"].astype(str)
    before_evol = df_ticks[trade_date_str < "2024-06-01"]
    after_evol = df_ticks[trade_date_str >= "2024-06-01"]

    assert len(before_evol) > 0
    assert len(after_evol) > 0

    if "quote_condition" in df_ticks.columns:
        assert before_evol["quote_condition"].isna().all()
        assert after_evol["quote_condition"].notna().all()
        assert after_evol["bid_price"].notna().all()
        assert after_evol["ask_price"].notna().all()
        # Bid <= Ask check
        assert (after_evol["bid_price"] <= after_evol["ask_price"]).all()

    # 4. Test Skew: 80% volume concentrated in Mega-Caps
    mega_caps = {"AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA"}
    mega_vol = df_ticks[df_ticks["ticker_symbol"].isin(mega_caps)]["size"].sum()
    total_vol = df_ticks["size"].sum()
    skew_ratio = mega_vol / total_vol
    assert 0.70 <= skew_ratio <= 0.90, f"Expected ~80% skew, got {skew_ratio:.2%}"

    # 5. Test Offline Duplicates (~2%)
    clean_ticks = len(df_ticks.drop_duplicates(subset=["trade_id"]))
    dup_rate = (len(df_ticks) - clean_ticks) / clean_ticks
    assert 0.01 <= dup_rate <= 0.035, f"Expected ~2% duplicate rate, got {dup_rate:.2%}"

    # 6. Test Cardinality Profile (Rubric M1-05)
    cardinality = stats["cardinality_profile"]["raw_market_ticks"]
    assert cardinality["trade_id"]["category"] == "HIGH"
    assert cardinality["ticker_symbol"]["category"] in {"LOW", "ULTRA_LOW", "MEDIUM"}
    assert cardinality["exchange"]["category"] in {"LOW", "ULTRA_LOW", "MEDIUM"}

    # 7. Test Resampled Daily OHLCV + VWAP Consistency
    df_daily = pd.read_parquet(run_dir / "raw_market_ohlcv")
    assert len(df_daily) > 0
    # Spot-check one ticker on one date
    sample = df_daily.iloc[0]
    sample_ticker = sample["ticker_symbol"]
    sample_date = sample["trade_date"]

    clean_sub_ticks = df_ticks[
        (df_ticks["ticker_symbol"] == sample_ticker) & (df_ticks["trade_date"] == sample_date)
    ].drop_duplicates(subset=["trade_id"])

    assert len(clean_sub_ticks) == sample["trade_count"]
    assert sample["volume"] == clean_sub_ticks["size"].sum()
    assert sample["high_price"] == round(clean_sub_ticks["price"].max(), 2)
    assert sample["low_price"] == round(clean_sub_ticks["price"].min(), 2)
    expected_vwap = round(
        (clean_sub_ticks["price"] * clean_sub_ticks["size"]).sum() / clean_sub_ticks["size"].sum(), 4
    )
    assert abs(sample["vwap"] - expected_vwap) < 1e-3
