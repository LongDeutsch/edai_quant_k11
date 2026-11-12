"""Unit tests for Canonical 4Y Dataset Generator & 3-Split Lifecycle (Phase 1)."""

from __future__ import annotations

import json
from pathlib import Path
import pytest
import pandas as pd

from quant_edai.generator.data_generator import QuantDataGenerator


@pytest.fixture(scope="module")
def canonical_4y_pilot(tmp_path_factory):
    """Run pilot canonical 4y generation (scale=0.01) once for this test module."""
    out_dir = tmp_path_factory.mktemp("canonical_4y_test")
    gen = QuantDataGenerator(
        profile="canonical_4y",
        scale=0.01,
        seed=42,
        out_dir=str(out_dir),
        start_date="2022-11-01",
        end_date="2026-10-31",
        dataset_id="ds_test_canonical_4y",
    )
    stats = gen.run()
    return gen.out_dir, stats


def test_canonical_4y_timeline_and_splits(canonical_4y_pilot):
    """Verifies 4-year span (1044 trading days) and 3 chronological non-overlapping splits."""
    out_dir, stats = canonical_4y_pilot

    assert stats["span_years"] >= 3.99
    assert stats["trading_days"] == 1044
    assert stats["start_date"] == "2022-11-01"
    assert stats["end_date"] == "2026-10-31"

    # Split breakdown
    splits = stats["splits"]
    assert splits["is_sessions"] == 652
    assert splits["oos_sessions"] == 261
    assert splits["tr_sessions"] == 131
    assert splits["is_sessions"] + splits["oos_sessions"] + splits["tr_sessions"] == 1044

    # Verify no date overlap across splits in ticks
    df_ticks = pd.read_parquet(out_dir / "raw_market_ticks")
    is_dates = set(df_ticks.loc[df_ticks["split_id"] == "IS", "trade_date"].unique())
    oos_dates = set(df_ticks.loc[df_ticks["split_id"] == "OOS", "trade_date"].unique())
    tr_dates = set(df_ticks.loc[df_ticks["split_id"] == "TRADING", "trade_date"].unique())

    assert len(is_dates) == 652
    assert len(oos_dates) == 261
    assert len(tr_dates) == 131
    assert not (is_dates & oos_dates), "IS and OOS must have zero date overlap"
    assert not (oos_dates & tr_dates), "OOS and TRADING must have zero date overlap"
    assert not (is_dates & tr_dates), "IS and TRADING must have zero date overlap"

    # Temporal monotonicity
    assert max(is_dates) < min(oos_dates)
    assert max(oos_dates) < min(tr_dates)
    assert max(is_dates) == "2025-04-30"
    assert min(oos_dates) == "2025-05-01"
    assert max(oos_dates) == "2026-04-30"
    assert min(tr_dates) == "2026-05-01"


def test_two_tier_tick_density(canonical_4y_pilot):
    """Verifies that IS/OOS density > TRADING replay density."""
    _, stats = canonical_4y_pilot
    splits = stats["splits"]

    is_density = splits["is_density_avg"]
    oos_density = splits["oos_density_avg"]
    tr_density = splits["tr_density_avg"]

    # Target: IS/OOS ~80 ticks/day at scale=0.01; TRADING ~50 ticks/day
    assert is_density > tr_density
    assert oos_density > tr_density
    assert 75.0 <= is_density <= 88.0
    assert 75.0 <= oos_density <= 88.0
    assert 45.0 <= tr_density <= 55.0


def test_split_boundary_zero_leakage(canonical_4y_pilot):
    """Verifies that labels on split boundary dates (2025-04-30, 2026-04-30) are masked."""
    out_dir, _ = canonical_4y_pilot
    df_labels = pd.read_parquet(out_dir / "label_ticker_daily_return" / "labels.parquet")

    # Boundary dates must not be present in valid forward labels
    assert "2025-04-30" not in df_labels["label_date"].values
    assert "2026-04-30" not in df_labels["label_date"].values

    # Check OHLCV has the boundary dates
    df_ohlcv = pd.read_parquet(out_dir / "raw_market_ohlcv")
    assert "2025-04-30" in df_ohlcv["trade_date"].values
    assert "2026-04-30" in df_ohlcv["trade_date"].values

    # Total labels must equal total_ohlcv - (3 boundary/terminal days * 16 tickers = 48)
    assert len(df_labels) == len(df_ohlcv) - 48


def test_engineering_faults_preservation(canonical_4y_pilot):
    """Verifies skew, duplicates, schema evolution, and regime drift."""
    out_dir, stats = canonical_4y_pilot

    # Volume skew ~80% mega-cap
    assert 0.78 <= float(stats["volume_skew_ratio"]) <= 0.82

    # Offline duplicate rate ~2%
    assert 0.015 <= float(stats["offline_duplicate_rate"]) <= 0.025

    # Schema evolution at 2024-06-01
    assert stats["schema_evolution_boundary"] == "2024-06-01"
    assert stats["schema_evolved_rows"] > 0
    df_ticks = pd.read_parquet(out_dir / "raw_market_ticks")
    tick_dates = df_ticks["trade_date"].astype(str)
    post_evo = df_ticks.loc[tick_dates >= "2024-06-01"]
    assert post_evo["quote_condition"].notna().sum() > 0

    # Regime drift at 2024-08-01
    assert stats["regime_drift_boundary"] == "2024-08-01"
    df_ohlcv = pd.read_parquet(out_dir / "raw_market_ohlcv")
    ohlcv_dates = df_ohlcv["trade_date"].astype(str)
    assert (df_ohlcv.loc[ohlcv_dates >= "2024-08-01", "market_regime"] == "BEAR_HIGH_VOL").all()
    assert (df_ohlcv.loc[ohlcv_dates < "2024-08-01", "market_regime"] == "BULL_LOW_VOL").all()


def test_manifest_and_split_provenance(canonical_4y_pilot):
    """Verifies that split_manifest.json and manifest.json exist with valid hashes."""
    out_dir, _ = canonical_4y_pilot

    split_manifest_path = out_dir / "split_manifest.json"
    assert split_manifest_path.exists()
    with open(split_manifest_path) as f:
        split_m = json.load(f)

    assert split_m["profile"] == "canonical_4y"
    assert "splits" in split_m
    assert set(split_m["splits"].keys()) == {"IN_SAMPLE", "OUT_OF_SAMPLE", "TRADING_REPLAY"}
    assert split_m["tables"]["label_ticker_daily_return"]["boundary_masked_dates"] == [
        "2025-04-30",
        "2026-04-30",
    ]

    manifest_path = out_dir / "manifest.json"
    assert manifest_path.exists()
    with open(manifest_path) as f:
        manifest = json.load(f)
    assert manifest["total_files"] > 0
    assert all("sha256" in item for item in manifest["files"])


def test_scaling_calibration_specs():
    """Verifies the offline scaling calibration calculations for 10 GB and 100 GB."""
    spec_10gb = QuantDataGenerator.calculate_workload_specs(
        pilot_bytes=3_774_873,  # ~3.6 MB
        pilot_ticks=76_000,
        pilot_duration_sec=6.5,
        target_gb=10.0,
        total_trading_days=1044,
    )
    assert spec_10gb["target_gb"] == 10.0
    assert spec_10gb["projected_ticks"] > 200_000_000
    assert spec_10gb["recommended_chunk_rows"] == 1_000_000
    assert spec_10gb["estimated_peak_memory_mb"] <= 512

    spec_100gb = QuantDataGenerator.calculate_workload_specs(
        pilot_bytes=3_774_873,
        pilot_ticks=76_000,
        pilot_duration_sec=6.5,
        target_gb=100.0,
        total_trading_days=1044,
    )
    assert spec_100gb["target_gb"] == 100.0
    assert spec_100gb["projected_ticks"] > 2_000_000_000
