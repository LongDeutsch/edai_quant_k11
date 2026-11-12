"""Unit tests for candlestick market data, alignment, indicators, and PnL verification."""

from datetime import datetime, timezone
import numpy as np
import pandas as pd
import pytest

from quant_edai.serving.market_data_service import (
    DataValidator,
    ExecutionMarkerClassifier,
    IndicatorCalculator,
    MarketDataService,
    NY_TZ,
)


def test_timezone_normalization_utc_to_ny():
    """Verify timestamps are parsed to UTC and correctly converted to America/New_York."""
    raw_df = pd.DataFrame({
        "event_timestamp": [
            "2024-03-15T09:30:00Z",
            "2024-03-15T10:00:00Z",
            "2024-03-15T16:00:00Z",
        ],
        "close_price": [180.0, 180.5, 181.0],
    })

    norm_df = DataValidator.normalize_timestamps(raw_df, time_col="event_timestamp")

    assert "event_timestamp_utc" in norm_df.columns
    assert "event_timestamp_ny" in norm_df.columns

    # 09:30 UTC during EDT is 05:30 EDT (or EST)
    assert norm_df["event_timestamp_utc"].iloc[0].tzinfo is not None
    assert str(norm_df["event_timestamp_utc"].iloc[0].tzinfo) == "UTC"
    assert "2024-03-15" in norm_df["event_timestamp_ny"].iloc[0]


def test_timezone_normalization_mixed_microsecond_iso_formats():
    """Verify handling of mixed ISO formats with and without microseconds (e.g. +00:00 vs Z)."""
    raw_df = pd.DataFrame({
        "executed_at": [
            "2024-03-15T10:00:00Z",
            "2026-11-10T07:18:17.083599+00:00",
            "2024-03-15 15:55:00",
        ],
        "fill_price": [180.0, 180.5, 181.0],
    })

    norm_df = DataValidator.normalize_timestamps(
        raw_df,
        time_col="executed_at",
        output_utc_col="executed_at_utc",
        output_ny_col="executed_at_ny",
    )

    assert len(norm_df) == 3
    assert norm_df["executed_at_utc"].iloc[1].microsecond == 83599
    assert norm_df["executed_at_utc"].iloc[1].tzinfo is not None
    assert "EST" in norm_df["executed_at_ny"].iloc[1]


def test_alignment_validation_aligned_vs_misaligned():
    """Verify alignment validator correctly identifies matching sessions and catches mismatches."""
    # Aligned case: Both on 2024-03-15
    candles_aligned = pd.DataFrame({
        "event_timestamp_utc": pd.date_range("2024-03-15 09:30:00", periods=30, freq="1min", tz="UTC"),
        "close_price": np.linspace(180, 182, 30),
    })
    trades_aligned = pd.DataFrame({
        "executed_at_utc": pd.date_range("2024-03-15 09:45:00", periods=5, freq="2min", tz="UTC"),
        "fill_price": [180.5, 180.7, 181.0, 181.2, 181.5],
    })

    rep_aligned = DataValidator.validate_alignment(candles_aligned, trades_aligned)
    assert rep_aligned.is_aligned is True
    assert rep_aligned.status == "HEALTHY"
    assert rep_aligned.candle_count == 30
    assert rep_aligned.trade_count == 5

    # Misaligned case: Candles from 2026-11-08, Trades from 2024-03-15
    candles_misaligned = pd.DataFrame({
        "event_timestamp_utc": pd.date_range("2026-11-08 09:30:00", periods=30, freq="1min", tz="UTC"),
        "close_price": np.linspace(180, 182, 30),
    })

    rep_misaligned = DataValidator.validate_alignment(candles_misaligned, trades_aligned)
    assert rep_misaligned.is_aligned is False
    assert rep_misaligned.status == "MISALIGNED"
    assert any("Misalignment" in w for w in rep_misaligned.warnings)


def test_indicator_computation_sma_and_vwap():
    """Verify SMA(20), SMA(50), and VWAP computation correctness and properties."""
    np.random.seed(42)
    n = 60
    prices = 100.0 + np.cumsum(np.random.normal(0, 0.5, n))
    volumes = np.random.uniform(1000, 5000, n)

    df = pd.DataFrame({
        "close_price": prices,
        "volume": volumes,
        "high_price": prices + 0.2,
        "low_price": prices - 0.2,
    })

    res = IndicatorCalculator.compute_indicators(df)

    assert "sma_20" in res.columns
    assert "sma_50" in res.columns
    assert "vwap" in res.columns
    assert "spread_bps" in res.columns

    # Verify SMA(20) at index 19 equals rolling mean of first 20 items
    expected_sma20 = float(np.mean(prices[:20]))
    assert abs(res["sma_20"].iloc[19] - expected_sma20) < 1e-4

    # Verify VWAP manual formula
    expected_vwap = float(np.sum(prices * volumes) / np.sum(volumes))
    assert abs(res["vwap"].iloc[-1] - expected_vwap) < 1e-4


def test_realized_pnl_calculations_long_and_short():
    """Verify mathematical correctness of Realized PnL for LONG and SHORT roundtrips."""
    # Case 1: LONG Position (Buy at 100, Sell at 105, 50 shares, comm $1.00)
    entry_p = 100.0
    exit_p = 105.0
    qty = 50.0
    comm = 1.00

    gross_pnl_long = (exit_p - entry_p) * qty
    net_pnl_long = gross_pnl_long - comm
    assert gross_pnl_long == 250.0
    assert net_pnl_long == 249.0

    # Case 2: SHORT Position (Sell to open at 100, Buy to close at 95, 50 shares, comm $1.00)
    entry_p_short = 100.0
    exit_p_short = 95.0
    gross_pnl_short = (entry_p_short - exit_p_short) * qty
    net_pnl_short = gross_pnl_short - comm
    assert gross_pnl_short == 250.0
    assert net_pnl_short == 249.0

    # Case 3: SHORT Position Loss (Sell to open at 100, Buy to close at 104, 50 shares, comm $1.00)
    exit_p_short_loss = 104.0
    gross_pnl_short_loss = (entry_p_short - exit_p_short_loss) * qty
    net_pnl_short_loss = gross_pnl_short_loss - comm
    assert gross_pnl_short_loss == -200.0
    assert net_pnl_short_loss == -201.0

    # Case 4: Partial Fill (Closed 30 out of 50 shares)
    partial_qty = 30.0
    gross_pnl_partial = (exit_p - entry_p) * partial_qty
    net_pnl_partial = gross_pnl_partial - (comm * (partial_qty / qty))
    assert gross_pnl_partial == 150.0
    assert net_pnl_partial == 149.4


def test_semantic_execution_marker_classification():
    """Verify trades are properly classified into semantic actions and exit tags."""
    trades_df = pd.DataFrame([
        {
            "trade_id": "trd_1",
            "direction": "BUY",
            "order_type": "MARKET",
            "realized_pnl": 0.0,
            "fill_price": 180.0,
            "requested_price": 180.0,
            "fill_quantity": 50.0,
            "commission": 1.0,
            "slippage_bps": 5.0,
            "executed_at": "2024-03-15T09:44:00Z",
        },
        {
            "trade_id": "trd_2",
            "direction": "SELL",
            "order_type": "STOP_LOSS",
            "realized_pnl": -25.0,
            "fill_price": 179.5,
            "requested_price": 179.6,
            "fill_quantity": 50.0,
            "commission": 1.0,
            "slippage_bps": 5.0,
            "executed_at": "2024-03-15T09:47:00Z",
        },
        {
            "trade_id": "trd_3",
            "direction": "SELL",
            "order_type": "MARKET",
            "realized_pnl": 0.0,
            "fill_price": 180.0,
            "requested_price": 180.0,
            "fill_quantity": 50.0,
            "commission": 1.0,
            "slippage_bps": 5.0,
            "executed_at": "2024-03-15T09:48:00Z",
        },
        {
            "trade_id": "trd_4",
            "direction": "BUY",
            "order_type": "TAKE_PROFIT",
            "realized_pnl": 35.0,
            "fill_price": 179.3,
            "requested_price": 179.4,
            "fill_quantity": 50.0,
            "commission": 1.0,
            "slippage_bps": 5.0,
            "executed_at": "2024-03-15T09:51:00Z",
        },
    ])

    classified = ExecutionMarkerClassifier.classify_trades(trades_df)

    assert classified["semantic_action"].iloc[0] == "OPEN LONG"
    assert classified["semantic_action"].iloc[1] == "CLOSE LONG"
    assert classified["semantic_action"].iloc[2] == "OPEN SHORT"
    assert classified["semantic_action"].iloc[3] == "CLOSE SHORT"

    assert "STOP LOSS" in classified["hover_text"].iloc[1]
    assert "TAKE PROFIT" in classified["hover_text"].iloc[3]
