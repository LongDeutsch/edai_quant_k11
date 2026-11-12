"""Unit tests for QuantDataGenerator (Bronze-only; no gold features)."""

from pathlib import Path

import pandas as pd
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator


def test_generator_deterministic_run(tmp_path):
    """Asserts that identical seeds produce 100% identical outputs."""
    out_dir_1 = tmp_path / "run_1"
    out_dir_2 = tmp_path / "run_2"

    gen_1 = QuantDataGenerator(
        seed=42,
        scale=0.01,
        out_dir=str(out_dir_1),
        start_date="2024-01-02",
        end_date="2024-03-29",
    )
    stats_1 = gen_1.run()

    gen_2 = QuantDataGenerator(
        seed=42,
        scale=0.01,
        out_dir=str(out_dir_2),
        start_date="2024-01-02",
        end_date="2024-03-29",
    )
    stats_2 = gen_2.run()

    assert stats_1["total_market_rows"] == stats_2["total_market_rows"]
    assert stats_1["total_volume"] == stats_2["total_volume"]
    assert stats_1["mega_cap_volume"] == stats_2["mega_cap_volume"]
    assert stats_1["volume_skew_ratio"] == stats_2["volume_skew_ratio"]
    assert stats_1["total_news_articles"] == stats_2["total_news_articles"]


def test_generator_datasets_and_schemas(tmp_path):
    """Validates Bronze tables/schemas and that gold features are NOT written."""
    out_dir = tmp_path / "lakehouse_test"
    gen = QuantDataGenerator(
        seed=123,
        scale=0.01,
        out_dir=str(out_dir),
        start_date="2024-01-02",
        end_date="2024-03-29",
        write_gold_features=True,  # ignored — must stay Bronze-only
    )
    stats = gen.run()
    run_dir = gen.out_dir

    assert stats.get("gold_features_written") is False
    assert not (run_dir / "gold_features").exists()
    assert not (run_dir / "feat_ticker_technical_indicators.parquet").exists()

    assert stats["total_market_rows"] > 0
    assert stats["total_news_articles"] > 0
    assert stats["total_label_rows"] > 0
    assert stats["total_dimension_rows"] > 0
    assert stats["schema_evolution_boundary"] == "2024-06-01"
    assert stats["regime_drift_boundary"] == "2024-08-01"
    assert stats.get("scale_mode") == "tick_budget"
    assert stats.get("ohlcv_volume_scaled_by_scale") is False
    assert "offline_start" in stats
    assert "offline_end" in stats
    assert stats.get("dataset_id")

    market_files = list((run_dir / "raw_market_ohlcv").glob("**/*.parquet"))
    assert len(market_files) > 0
    df_market = pd.read_parquet(run_dir / "raw_market_ohlcv")
    expected_cols = {
        "trade_id",
        "ticker_symbol",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "exchange",
        "ingestion_timestamp",
        "market_regime",
        "trade_date",
    }
    assert expected_cols.issubset(set(df_market.columns))

    assert "cardinality_profile" in stats
    market_cardinality = stats["cardinality_profile"]["market_ohlcv"]
    assert "trade_id" in market_cardinality
    assert market_cardinality["trade_id"]["category"] == "HIGH"
    assert market_cardinality["ticker_symbol"]["category"] in {"LOW", "ULTRA_LOW"}

    label_file = run_dir / "label_ticker_daily_return" / "labels.parquet"
    assert label_file.exists()
    df_labels = pd.read_parquet(label_file)
    assert {"ticker_symbol", "label_date", "daily_return_fwd", "direction_label"}.issubset(
        set(df_labels.columns)
    )
    assert set(df_labels["direction_label"].unique()).issubset({0, 1})
    assert (df_labels.loc[df_labels["daily_return_fwd"] > 0, "direction_label"] == 1).all()
    assert (df_labels.loc[df_labels["daily_return_fwd"] <= 0, "direction_label"] == 0).all()

    df_news = pd.read_parquet(run_dir / "raw_financial_news")
    assert {"article_id", "ticker_symbol", "headline", "sentiment_score"}.issubset(set(df_news.columns))

    dim_file = run_dir / "dim_companies" / "dim_companies.parquet"
    assert dim_file.exists()
    df_dim = pd.read_parquet(dim_file)
    assert {"company_key", "ticker_symbol", "valid_from_ts", "valid_to_ts", "is_current"}.issubset(
        set(df_dim.columns)
    )
    assert True in df_dim["is_current"].values
    assert False in df_dim["is_current"].values
