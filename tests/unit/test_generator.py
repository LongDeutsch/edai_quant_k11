"""Unit tests for QuantDataGenerator."""

import json
from pathlib import Path
import pandas as pd
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator


def test_generator_deterministic_run(tmp_path):
    """Asserts that identical seeds produce 100% identical outputs."""
    out_dir_1 = tmp_path / "run_1"
    out_dir_2 = tmp_path / "run_2"

    gen_1 = QuantDataGenerator(seed=42, scale=0.01, out_dir=str(out_dir_1))
    stats_1 = gen_1.run()

    gen_2 = QuantDataGenerator(seed=42, scale=0.01, out_dir=str(out_dir_2))
    stats_2 = gen_2.run()

    # Compare statistics
    assert stats_1["total_market_rows"] == stats_2["total_market_rows"]
    assert stats_1["total_volume"] == stats_2["total_volume"]
    assert stats_1["mega_cap_volume"] == stats_2["mega_cap_volume"]
    assert stats_1["volume_skew_ratio"] == stats_2["volume_skew_ratio"]
    assert stats_1["total_news_articles"] == stats_2["total_news_articles"]


def test_generator_datasets_and_schemas(tmp_path):
    """Validates existence and schema columns of all generated tables."""
    out_dir = tmp_path / "lakehouse_test"
    gen = QuantDataGenerator(seed=123, scale=0.01, out_dir=str(out_dir))
    stats = gen.run()

    assert stats["total_market_rows"] > 0
    assert stats["total_news_articles"] > 0
    assert stats["total_label_rows"] > 0
    assert stats["total_dimension_rows"] > 0

    # 1. Market OHLCV
    market_files = list((out_dir / "raw_market_ohlcv").glob("**/*.parquet"))
    assert len(market_files) > 0
    df_market = pd.read_parquet(out_dir / "raw_market_ohlcv")
    expected_cols = {
        "trade_id", "ticker_symbol", "open_price", "high_price", "low_price",
        "close_price", "volume", "exchange", "ingestion_timestamp",
        "market_regime", "trade_date"
    }
    assert expected_cols.issubset(set(df_market.columns))

    # Cardinality Profile checks (Rubric M1-05)
    assert "cardinality_profile" in stats
    market_cardinality = stats["cardinality_profile"]["market_ohlcv"]
    assert "trade_id" in market_cardinality
    assert market_cardinality["trade_id"]["category"] == "HIGH"
    assert market_cardinality["ticker_symbol"]["category"] in {"LOW", "ULTRA_LOW"}

    # 2. Labels
    label_file = out_dir / "label_ticker_daily_return" / "labels.parquet"
    assert label_file.exists()
    df_labels = pd.read_parquet(label_file)
    assert {"ticker_symbol", "label_date", "daily_return_fwd", "direction_label"}.issubset(set(df_labels.columns))
    assert set(df_labels["direction_label"].unique()).issubset({0, 1})

    # Verify that direction_label aligns with daily_return_fwd
    assert (df_labels.loc[df_labels["daily_return_fwd"] > 0, "direction_label"] == 1).all()
    assert (df_labels.loc[df_labels["daily_return_fwd"] <= 0, "direction_label"] == 0).all()

    # 3. Financial News
    df_news = pd.read_parquet(out_dir / "raw_financial_news")
    assert {"article_id", "ticker_symbol", "headline", "sentiment_score"}.issubset(set(df_news.columns))

    # 4. Dimension Companies
    dim_file = out_dir / "dim_companies" / "dim_companies.parquet"
    assert dim_file.exists()
    df_dim = pd.read_parquet(dim_file)
    assert {"company_key", "ticker_symbol", "valid_from_ts", "valid_to_ts", "is_current"}.issubset(set(df_dim.columns))
    # SCD2 active flag check
    assert True in df_dim["is_current"].values
    assert False in df_dim["is_current"].values
