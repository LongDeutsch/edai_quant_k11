"""Unit tests for FaultsInjector."""

import numpy as np
import pandas as pd
import pytest

from quant_edai.generator.faults_injector import FaultsInjector


@pytest.fixture
def sample_market_df():
    """Generates a synthetic market dataframe for fault testing."""
    dates = pd.date_range("2024-05-01", "2024-06-30", freq="B")
    tickers = ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM", "WMT"]
    rows = []
    for d in dates:
        for t in tickers:
            rows.append({
                "ticker_symbol": t,
                "trade_date": d.strftime("%Y-%m-%d"),
                "open_price": 100.0,
                "high_price": 105.0,
                "low_price": 95.0,
                "close_price": 102.0,
                "volume": 100000,
                "ingestion_timestamp": "2024-05-01T00:00:00Z",
            })
    return pd.DataFrame(rows)


def test_volume_skew_injection(sample_market_df):
    injector = FaultsInjector(seed=42)
    mega_caps = ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA"]
    df_skewed = injector.inject_volume_skew(sample_market_df, mega_caps=mega_caps, target_volume_ratio=0.80)

    total_vol = df_skewed["volume"].sum()
    mega_vol = df_skewed.loc[df_skewed["ticker_symbol"].isin(mega_caps), "volume"].sum()
    ratio = mega_vol / total_vol

    # Assert volume skew is approximately 80% (+- 2%)
    assert abs(ratio - 0.80) < 0.02


def test_schema_evolution_injection(sample_market_df):
    injector = FaultsInjector(seed=42)
    boundary_date = "2024-06-01"
    df_evolved = injector.inject_schema_evolution(sample_market_df, boundary_date=boundary_date)

    assert "vwap" in df_evolved.columns
    assert "trade_count" in df_evolved.columns

    pre_boundary = df_evolved[df_evolved["trade_date"] < boundary_date]
    post_boundary = df_evolved[df_evolved["trade_date"] >= boundary_date]

    # Pre-boundary rows must have NaN for newly evolved columns
    assert pre_boundary["vwap"].isna().all()
    assert pre_boundary["trade_count"].isna().all()

    # Post-boundary rows must have valid populated values
    assert post_boundary["vwap"].notna().all()
    assert post_boundary["trade_count"].notna().all()
    assert (post_boundary["trade_count"] > 0).all()


def test_offline_duplicates_injection(sample_market_df):
    injector = FaultsInjector(seed=42)
    original_len = len(sample_market_df)
    dup_ratio = 0.02
    df_dups = injector.inject_offline_duplicates(sample_market_df, duplicate_ratio=dup_ratio)

    expected_dups = int(original_len * dup_ratio)
    actual_dups = len(df_dups) - original_len

    assert actual_dups == expected_dups
    # Check that duplicated rows share the same (ticker_symbol, trade_date)
    dup_counts = df_dups.groupby(["ticker_symbol", "trade_date"]).size()
    assert (dup_counts > 1).sum() == expected_dups


def test_streaming_late_arrivals():
    injector = FaultsInjector(seed=42)
    base_ts = pd.Timestamp("2024-05-01 10:00:00", tz="UTC")
    ticks = []
    for i in range(1000):
        ticks.append({
            "trade_id": f"t_{i}",
            "ticker_symbol": "AAPL",
            "event_timestamp": (base_ts + pd.Timedelta(seconds=i)).isoformat(),
            "price": 180.0,
            "size": 100,
        })
    df_ticks = pd.DataFrame(ticks)

    late_ratio = 0.05
    df_late = injector.inject_streaming_late_arrivals(df_ticks, late_ratio=late_ratio, min_delay_seconds=300, max_delay_seconds=600)

    assert "is_late" in df_late.columns
    assert "published_timestamp" in df_late.columns

    num_late = df_late["is_late"].sum()
    assert num_late == int(1000 * late_ratio)

    late_records = df_late[df_late["is_late"]]
    for _, row in late_records.iterrows():
        event_t = pd.to_datetime(row["event_timestamp"])
        pub_t = pd.to_datetime(row["published_timestamp"])
        delay = (pub_t - event_t).total_seconds()
        assert 300 <= delay <= 600


def test_streaming_duplicates():
    injector = FaultsInjector(seed=42)
    ticks = [{"trade_id": f"t_{i}", "price": 100.0} for i in range(1000)]
    df_ticks = pd.DataFrame(ticks)

    dup_ratio = 0.015
    df_result = injector.inject_streaming_duplicates(df_ticks, duplicate_ratio=dup_ratio)

    expected_dups = int(1000 * dup_ratio)
    assert len(df_result) == 1000 + expected_dups
    assert df_result["is_duplicate"].sum() == expected_dups


def test_market_regime_drift(sample_market_df):
    injector = FaultsInjector(seed=42)
    drift_date = "2024-06-01"
    df_drift = injector.inject_market_regime_drift(sample_market_df, drift_date=drift_date, volatility_multiplier=2.5)

    assert "market_regime" in df_drift.columns
    pre_drift = df_drift[df_drift["trade_date"] < drift_date]
    post_drift = df_drift[df_drift["trade_date"] >= drift_date]

    assert (pre_drift["market_regime"] == "BULL_LOW_VOL").all()
    assert (post_drift["market_regime"] == "BEAR_HIGH_VOL").all()

    # Bear regime spreads must be larger
    pre_spread = (pre_drift["high_price"] - pre_drift["low_price"]).mean()
    post_spread = (post_drift["high_price"] - post_drift["low_price"]).mean()
    assert post_spread > pre_spread
