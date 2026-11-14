"""Live check: AAPL May 2026 fills sit on Daily TRADING candles."""

from __future__ import annotations

import pandas as pd
import pytest

from quant_edai.serving.dashboard.dashboard_service import DATASET_CATALOG, MarketDataService

DATASET_ID = "ds_canonical_4y_eval_s05"
TICKER = "AAPL"


def _trading_preset() -> str:
    return next(name for name, spec in DATASET_CATALOG.items() if spec.get("split_id") == "TRADING")


@pytest.mark.live
def test_aapl_may_2026_fills_land_on_daily_trading_candles():
    bounds = MarketDataService.get_fill_time_bounds(DATASET_ID, TICKER)
    assert bounds.error is None, bounds.error
    assert bounds.count == 6
    assert bounds.min_time >= pd.Timestamp("2026-05-01", tz="UTC")
    assert bounds.max_time <= pd.Timestamp("2026-05-06 23:59:59.999", tz="UTC")

    start, end = MarketDataService.resolve_candle_window(
        _trading_preset(),
        "Daily",
        bounds.min_time,
        bounds.max_time,
    )
    assert start <= "2026-05-01"
    assert end >= "2026-05-04"

    candles = MarketDataService.fetch_candlesticks(
        dataset_id=DATASET_ID,
        ticker=TICKER,
        timeframe="Daily",
        start_date=start,
        end_date=end,
        split_id="TRADING",
    )
    assert not candles.empty
    window_start, window_end = MarketDataService.candle_marker_bounds(candles, "Daily")
    assert window_start <= bounds.min_time
    assert window_end >= bounds.max_time

    summary = MarketDataService.summarize_trade_markers(
        dataset_id=DATASET_ID,
        ticker=TICKER,
        start_time=window_start,
        end_time=window_end,
        alpha_id="alpha_mom_z_intraday",
    )
    assert summary.status == "matched", summary.message
    assert summary.matched_count == 6
    assert set(summary.frame["alpha_id"]) == {"alpha_mom_z_intraday"}
