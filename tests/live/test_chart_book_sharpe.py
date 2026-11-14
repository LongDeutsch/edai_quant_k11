"""Live check: a flat JPM 5-minute book reports an undefined Sharpe, not a huge negative one."""

from __future__ import annotations

import pytest

from quant_edai.quant.metrics import DISPLAY_NO_TRADES, SHARPE_ZERO_VOLATILITY, format_sharpe
from quant_edai.serving.dashboard.dashboard_service import DATASET_CATALOG, MarketDataService

DATASET_ID = "ds_canonical_4y_eval_s05"
TICKER = "JPM"
ALPHA_ID = "alpha_sma_cross_intraday"


def _is_preset() -> str:
    return next(name for name, spec in DATASET_CATALOG.items() if spec.get("split_id") == "IS")


@pytest.mark.live
def test_jpm_5m_sma_cross_flat_book_sharpe_is_undefined():
    preset = _is_preset()
    spec = DATASET_CATALOG[preset]
    start, end = spec["date_range"]
    candles = MarketDataService.calculate_indicators(MarketDataService.fetch_candlesticks(
        dataset_id=DATASET_ID,
        ticker=TICKER,
        timeframe="5m",
        start_date=start,
        end_date=end,
        split_id="IS",
    ))
    assert len(candles) > 1_000

    book = MarketDataService.evaluate_candle_book(
        candles,
        alpha_id=ALPHA_ID,
        dataset_id=DATASET_ID,
        ticker=TICKER,
        timeframe="5m",
    )
    assert book["trade_count"] == 0
    assert book["pnl"] == 0.0
    assert book["trading_pnl"] == 0.0
    assert book["cash_interest"] == 0.0
    assert book["max_drawdown"] == 0.0
    assert book["final_equity"] == book["booksize"]
    assert book["sharpe"] is None
    assert book["sharpe_reason"] == SHARPE_ZERO_VOLATILITY
    assert format_sharpe(book["sharpe"], book["sharpe_reason"], trade_count=0) == DISPLAY_NO_TRADES


@pytest.mark.live
def test_aapl_daily_macd_book_keeps_a_finite_sharpe():
    preset = _is_preset()
    spec = DATASET_CATALOG[preset]
    start, end = spec["date_range"]
    candles = MarketDataService.calculate_indicators(MarketDataService.fetch_candlesticks(
        dataset_id=DATASET_ID,
        ticker="AAPL",
        timeframe="Daily",
        start_date=start,
        end_date=end,
        split_id="IS",
    ))
    assert len(candles) > 200

    book = MarketDataService.evaluate_candle_book(
        candles,
        alpha_id="alpha_macd_z_daily",
        dataset_id=DATASET_ID,
        ticker="AAPL",
        timeframe="Daily",
    )
    assert book["trade_count"] > 0
    assert book["sharpe_reason"] is None
    assert book["sharpe"] is not None
    assert abs(book["sharpe"]) < 100
    assert book["cash_interest"] == 0.0
