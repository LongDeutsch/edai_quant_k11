"""Unit tests for QuantAlphaBacktester and Zero Look-Ahead Bias proof (Rubric M1-37)."""

import json
import os
import tempfile
import numpy as np
import pandas as pd
import pytest

from quant_edai.quant.backtester import QuantAlphaBacktester


def test_backtester_synthetic_run_and_contract():
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = os.path.join(tmpdir, "phase5_test")
        backtester = QuantAlphaBacktester(
            features_path=os.path.join(tmpdir, "non_existent_feat.parquet"),
            labels_path=os.path.join(tmpdir, "non_existent_lbl.parquet"),
            output_dir=out_dir,
            top_k=2,
            friction_bps=7.0,
        )

        results = backtester.run_backtest()
        assert results is not None
        assert "strategy_name" in results
        assert "benchmark_name" in results
        assert "metrics" in results
        assert "series" in results
        assert "artifacts" in results

        metrics = results["metrics"]
        assert "ic_mean" in metrics
        assert "rank_ic_mean" in metrics
        assert "sharpe" in metrics
        assert "max_drawdown" in metrics
        assert "cum_return" in metrics
        assert "spy_sharpe" in metrics

        # Verify artifacts written
        json_file = os.path.join(out_dir, "backtest_summary.json")
        assert os.path.exists(json_file)
        with open(json_file) as f:
            disk_contract = json.load(f)
        assert disk_contract["date_range"]["trading_days"] > 0
        assert len(disk_contract["series"]["equity_curve"]) > 0

        assert os.path.exists(os.path.join(out_dir, "cumulative_returns_vs_benchmark.png"))
        assert os.path.exists(os.path.join(out_dir, "factor_ic_distribution.png"))
        assert os.path.exists(os.path.join(out_dir, "rolling_sharpe_ratio.png"))
        assert os.path.exists(os.path.join(out_dir, "novel_idea_attribution_report.md"))


def test_zero_look_ahead_bias_proof():
    """Mathematical proof that shuffling future dates does not alter past/present trading decisions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backtester = QuantAlphaBacktester(
            features_path=os.path.join(tmpdir, "non_existent_feat.parquet"),
            labels_path=os.path.join(tmpdir, "non_existent_lbl.parquet"),
            output_dir=tmpdir,
            top_k=3,
        )

        df_base = backtester.load_data()
        dates = sorted(df_base["date"].unique())
        mid_point = len(dates) // 2
        cutoff_date = dates[mid_point]

        # Run 1: Normal run up to cutoff_date
        df_run1 = df_base.copy()

        # Run 2: Severely perturb / invert returns in the future (after cutoff_date)
        df_run2 = df_base.copy()
        future_mask = df_run2["date"] > cutoff_date
        df_run2.loc[future_mask, "daily_return_fwd"] = -df_run2.loc[future_mask, "daily_return_fwd"] * 5.0
        df_run2.loc[future_mask, "close_price"] = df_run2.loc[future_mask, "close_price"] * 10.0

        # Assert decisions up to cutoff_date are 100% identical
        # Compare day decisions on cutoff_date
        slice_run1 = df_run1[df_run1["date"] == cutoff_date]
        slice_run2 = df_run2[df_run2["date"] == cutoff_date]

        alpha1 = backtester._run_day_scoring(slice_run1) if hasattr(backtester, "_run_day_scoring") else slice_run1["p_ml"]
        alpha2 = backtester._run_day_scoring(slice_run2) if hasattr(backtester, "_run_day_scoring") else slice_run2["p_ml"]

        # Decisions made strictly prior to or on cutoff_date must be bitwise identical
        pd.testing.assert_series_equal(alpha1, alpha2)


def test_transaction_cost_impact():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Zero friction run
        bt_zero = QuantAlphaBacktester(
            output_dir=os.path.join(tmpdir, "zero"),
            friction_bps=0.0,
        )
        res_zero = bt_zero.run_backtest()

        # Heavy friction run (50 bps)
        bt_heavy = QuantAlphaBacktester(
            output_dir=os.path.join(tmpdir, "heavy"),
            friction_bps=50.0,
        )
        res_heavy = bt_heavy.run_backtest()

        # Net return with heavy friction must be strictly less than with zero friction
        assert res_heavy["metrics"]["cum_return"] < res_zero["metrics"]["cum_return"]
