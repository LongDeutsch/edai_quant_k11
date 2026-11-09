"""Unit tests for Market Regime Adaptive Factor Weighting and Risk Parity (Rubric M2-55)."""

import os
import tempfile
import pandas as pd
import pytest

from quant_edai.quant.alpha_synthesis import (
    FIXED_BASELINE_WEIGHTS,
    REGIME_WEIGHT_MAP,
    MarketRegime,
    compute_composite_alpha,
    compute_risk_parity_weights,
    detect_market_regime,
    generate_attribution_report,
)


def test_detect_market_regime():
    assert detect_market_regime(0.08) == MarketRegime.LOW_VOL
    assert detect_market_regime(0.18) == MarketRegime.NORMAL_VOL
    assert detect_market_regime(0.32) == MarketRegime.HIGH_VOL


def test_compute_composite_alpha():
    df = pd.DataFrame({
        "ticker_symbol": ["AAPL", "MSFT", "NVDA"],
        "intraday_return_1d": [0.02, -0.01, 0.05],
        "sma_ratio_20_50": [1.02, 0.98, 1.05],
        "rsi_14d": [65.0, 30.0, 75.0],
        "macd_histogram": [0.5, -0.2, 0.8],
        "p_ml": [0.65, 0.40, 0.80],
    })

    # Fixed weights
    z_fixed = compute_composite_alpha(df, weights=FIXED_BASELINE_WEIGHTS)
    assert len(z_fixed) == 3
    assert not z_fixed.isna().any()

    # High vol regime weights
    z_high_vol = compute_composite_alpha(df, regime=MarketRegime.HIGH_VOL)
    assert len(z_high_vol) == 3
    # Different weights yield different scores
    assert not z_fixed.equals(z_high_vol)


def test_compute_risk_parity_weights():
    # Asset A has vol 0.10, Asset B has vol 0.20
    # Inverse vol: 1/0.10 = 10, 1/0.20 = 5. Total = 15.
    # Weights: A = 10/15 = 2/3 (66.67%), B = 5/15 = 1/3 (33.33%)
    vols = {"AAPL": 0.10, "TSLA": 0.20}
    weights = compute_risk_parity_weights(vols)

    assert pytest.approx(weights["AAPL"], 0.001) == 2.0 / 3.0
    assert pytest.approx(weights["TSLA"], 0.001) == 1.0 / 3.0
    assert pytest.approx(sum(weights.values()), 0.001) == 1.0

    # Test empty or floor vol clamping
    assert compute_risk_parity_weights({}) == {}
    clamped = compute_risk_parity_weights({"X": 0.01, "Y": 0.01}, floor_vol=0.05)
    assert pytest.approx(clamped["X"], 0.001) == 0.5


def test_generate_attribution_report():
    with tempfile.TemporaryDirectory() as tmpdir:
        report_path = os.path.join(tmpdir, "novel_idea_report.md")

        dates = pd.date_range("2024-01-01", periods=50)
        base_eq = pd.Series([1.0 + i * 0.002 for i in range(50)], index=dates)
        regime_eq = pd.Series([1.0 + i * 0.003 for i in range(50)], index=dates)
        bench_eq = pd.Series([1.0 + i * 0.001 for i in range(50)], index=dates)

        content = generate_attribution_report(
            fixed_equity=base_eq,
            regime_equity=regime_eq,
            equal_weight_equity=base_eq,
            risk_parity_equity=regime_eq,
            benchmark_equity=bench_eq,
            output_path=report_path,
        )

        assert os.path.exists(report_path)
        assert "Novel Idea 2" in content
        assert "Risk Parity" in content
        assert "Market Regime" in content
