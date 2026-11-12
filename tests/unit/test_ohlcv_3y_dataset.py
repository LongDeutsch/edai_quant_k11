"""3-year Bronze OHLCV + Bronze→Silver→Gold pipeline sync tests (rubric-faithful)."""

from __future__ import annotations

import pandas as pd
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.processing.features.feature_calculator import (
    compute_technical_indicators,
    run_feature_calculation,
)
from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
from quant_edai.quant.alpha_metrics_service import _load_feature_panel, _normalize_panel, run_alpha_metrics
from quant_edai.quant.alpha_seed import _load_feature_rows, run_seed
from quant_edai.quant.paper_db import PaperDatabase


REQUIRED_OHLCV_COLS = {
    "trade_id",
    "ticker_symbol",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
}

REQUIRED_FEATURE_COLS = {
    "ticker_symbol",
    "event_timestamp",
    "close_price",
    "sma_20",
    "sma_50",
    "sma_ratio_20_50",
    "rsi_14d",
    "macd_histogram",
    "rolling_volatility_30d",
    "intraday_return_1d",
}


@pytest.fixture(scope="module")
def lakehouse_3y(tmp_path_factory):
    """Generate 3y Bronze-only lakehouse once for the module."""
    out_dir = tmp_path_factory.mktemp("lakehouse_3y")
    gen = QuantDataGenerator(
        seed=7,
        scale=0.01,
        out_dir=str(out_dir),
        start_date="2022-11-01",
        end_date="2025-10-31",
        write_gold_features=False,
    )
    stats = gen.run()
    return gen.out_dir, stats


def test_ohlcv_spans_three_years_bronze_only(lakehouse_3y):
    out_dir, stats = lakehouse_3y
    assert stats["span_years"] >= 2.9
    assert stats["trading_days"] >= 700
    assert stats.get("gold_features_written") is False
    assert not (out_dir / "gold_features").exists()
    assert not (out_dir / "feat_ticker_technical_indicators.parquet").exists()

    df = pd.read_parquet(out_dir / "raw_market_ohlcv")
    assert REQUIRED_OHLCV_COLS.issubset(set(df.columns))
    dates = pd.to_datetime(df["trade_date"].astype(str))
    span_days = (dates.max() - dates.min()).days
    assert span_days >= 365 * 2 + 300

    assert (df["high_price"] >= df[["open_price", "close_price"]].max(axis=1) - 1e-6).all()
    assert (df["low_price"] <= df[["open_price", "close_price"]].min(axis=1) + 1e-6).all()
    assert (df["close_price"] > 0).all()
    assert (df["volume"] > 0).all()


def test_rubric_fault_ratios_on_3y_bronze(lakehouse_3y):
    """Skew / duplicate / schema evolution must match coursework rubric specs."""
    out_dir, stats = lakehouse_3y
    assert 0.78 <= float(stats["volume_skew_ratio"]) <= 0.82
    assert 0.015 <= float(stats["offline_duplicate_rate"]) <= 0.025
    assert stats["schema_evolution_boundary"] == "2024-06-01"
    assert stats["regime_drift_boundary"] == "2024-08-01"
    assert int(stats["schema_evolved_rows"]) > 0

    df = pd.read_parquet(out_dir / "raw_market_ohlcv")
    assert "vwap" in df.columns
    dates = pd.to_datetime(df["trade_date"].astype(str))
    post = df.loc[dates >= pd.Timestamp("2024-06-01")]
    assert post["vwap"].notna().sum() > 0
    assert "market_regime" in df.columns


def test_silver_to_gold_feature_pipeline_sync(lakehouse_3y, tmp_path):
    """Gold features must come from Silver (deduped), not the generator."""
    out_dir, _ = lakehouse_3y
    # Isolate pipeline outputs under a work copy path by pointing jobs at out_dir
    silver_dir = out_dir / "stg_market_ohlcv"
    job = DeduplicationAndSchemaEvolutionJob(
        source_dir=str(out_dir),
        target_dir=str(silver_dir),
    )
    dedup_metrics = job.run(engine="sim")
    assert silver_dir.exists()
    assert dedup_metrics is not None

    gold_path = out_dir / "gold_features" / "feat_ticker_technical_indicators.parquet"
    feat_metrics = run_feature_calculation(
        source_path=str(silver_dir),
        target_path=str(gold_path),
        engine="sim",
        out_file=None,
    )
    assert gold_path.exists()
    assert feat_metrics["output_rows"] > 0
    assert feat_metrics.get("source_zone") in ("explicit", "silver")

    df_silver = pd.read_parquet(silver_dir)
    df_feat = pd.read_parquet(gold_path)
    assert REQUIRED_FEATURE_COLS.issubset(set(df_feat.columns))

    # Align with deduped silver keys (ticker, date)
    silver_clean = df_silver.drop_duplicates(subset=["ticker_symbol", "trade_date"])
    assert set(df_feat["ticker_symbol"].unique()) == set(silver_clean["ticker_symbol"].unique())
    assert len(df_feat) == len(silver_clean)

    sample = "AAPL" if "AAPL" in set(df_feat["ticker_symbol"]) else sorted(df_feat["ticker_symbol"].unique())[0]
    recomputed = compute_technical_indicators(silver_clean[silver_clean["ticker_symbol"] == sample])
    stored = df_feat[df_feat["ticker_symbol"] == sample].sort_values("event_timestamp")
    recomputed = recomputed.sort_values("event_timestamp")
    assert len(stored) == len(recomputed)
    pd.testing.assert_series_equal(
        stored["rsi_14d"].reset_index(drop=True),
        recomputed["rsi_14d"].reset_index(drop=True),
        check_names=False,
        rtol=1e-5,
        atol=1e-5,
    )

    # Downstream seed/metrics consume post-DP3 gold
    df_seed, source = _load_feature_rows(str(out_dir))
    assert len(df_seed) > 100
    assert "gold_features" in source or "feat_ticker_technical_indicators" in source

    panel, _ = _load_feature_panel(str(out_dir))
    panel = _normalize_panel(panel)
    assert len(panel) > 500
    assert panel["event_timestamp"].min().year <= 2023
    assert panel["event_timestamp"].max().year >= 2025

    db_path = str(tmp_path / "sys_input.db")
    seed_result = run_seed(source_dir=str(out_dir), sqlite_path=db_path, promote=False)
    assert seed_result["status"] == "SUCCESS"
    assert seed_result["registry"]["registry_rows"] == 20

    db = PaperDatabase(sqlite_path=db_path)
    db.init_db(reset=False)
    metrics = run_alpha_metrics(db=db, source_dir=str(out_dir), booksize=100_000.0, horizon="DAILY")
    assert metrics["status"] == "SUCCESS"
    assert metrics["alphas_updated"] == 10
    assert metrics["window"]["window_used"] in ("last_2y", "full_available")


def test_labels_align_with_ohlcv_forward_return(lakehouse_3y):
    out_dir, _ = lakehouse_3y
    df_ohlcv = pd.read_parquet(out_dir / "raw_market_ohlcv")
    df_labels = pd.read_parquet(out_dir / "label_ticker_daily_return" / "labels.parquet")

    clean = (
        df_ohlcv.drop_duplicates(subset=["ticker_symbol", "trade_date"])
        .sort_values(["ticker_symbol", "trade_date"])
        .copy()
    )
    clean["next_close"] = clean.groupby("ticker_symbol")["close_price"].shift(-1)
    sample = clean.dropna(subset=["next_close"]).head(50)
    for _, row in sample.iterrows():
        match = df_labels[
            (df_labels["ticker_symbol"] == row["ticker_symbol"])
            & (df_labels["label_date"] == row["trade_date"])
        ]
        assert len(match) == 1
        expected = (row["next_close"] - row["close_price"]) / row["close_price"]
        assert abs(float(match.iloc[0]["daily_return_fwd"]) - float(expected)) < 1e-5
