"""Rules for independent IS/OOS evaluation. These tests do not read market data."""

from __future__ import annotations

import numpy as np
import pandas as pd

import sqlite3

from quant_edai.quant.alpha_split_evaluation import (
    approval_block_reason,
    assess_replay_ensemble,
    combine_name_books,
    eval_run_id,
    is_admin_approver,
    persist_ensemble_approval,
    rank_ensemble_candidates,
    resolve_signal_rule,
    sharpe_degradation,
    weight_block_reason,
)
from quant_edai.serving.dashboard.dashboard_service import MarketDataService


def test_degradation_requires_a_positive_in_sample_sharpe():
    missing = sharpe_degradation(1.6, None)
    assert missing["comparable"] is False
    assert missing["degradation_pct"] is None
    assert missing["sharpe_change"] is None

    near_zero = sharpe_degradation(0.05, -0.2)
    assert near_zero["comparable"] is False
    assert near_zero["degradation_pct"] is None
    assert near_zero["sharpe_change"] == -0.25

    negative = sharpe_degradation(-1.0, -0.4)
    assert negative["degradation_pct"] is None
    assert negative["sharpe_change"] == 0.6

    comparable = sharpe_degradation(1.6, 0.8)
    assert comparable["comparable"] is True
    assert comparable["degradation_pct"] == 50.0
    assert comparable["sharpe_change"] == -0.8


def test_missing_signal_rule_is_not_a_completed_flat_book():
    strategy, status, reason = resolve_signal_rule("DAILY", {})
    assert strategy is None
    assert status == "INELIGIBLE"
    assert "No signal rule" in reason

    strategy, status, reason = resolve_signal_rule("INTRADAY", {"strategy": "ml_prob"})
    assert strategy is None
    assert status == "INELIGIBLE"
    assert "intraday" in reason.lower()

    strategy, status, reason = resolve_signal_rule("DAILY", {"strategy": "sma_ratio", "entry_z": 1.0})
    assert strategy == "sma_ratio"
    assert status is None


def test_admin_gate_rejects_before_a_database_lookup():
    assert is_admin_approver("ADMIN_PM") is True
    assert is_admin_approver("admin") is True
    assert is_admin_approver("trader") is False
    assert "admin" in approval_block_reason(["a", "b", "c", "d", "e"], "DAILY", "trader").lower()
    assert "five" in approval_block_reason(["only_one"], "DAILY", "ADMIN").lower()


def test_run_id_is_stable_and_fits_the_column():
    first = eval_run_id("IS", "Daily", "v1.0.0", "alpha_sma_cross_daily")
    assert first == eval_run_id("IS", "Daily", "v1.0.0", "alpha_sma_cross_daily")
    assert len(first) <= 64
    long_id = eval_run_id("OOS", "5m", "v1.0.0", "alpha_" + ("x" * 80))
    assert len(long_id) <= 64


def test_flat_books_keep_a_completed_result_with_undefined_sharpe():
    index = pd.date_range("2024-01-01", periods=80, freq="D", tz="UTC")
    book = {
        "ticker": "AAPL",
        "bars": 80,
        "signal_count": 0,
        "bar_returns": [0.0] * 80,
        "bar_time": list(index),
        "leg_pnls": [],
        "pnl": 0.0,
    }
    combined = combine_name_books([book, {**book, "ticker": "MSFT"}], booksize=100_000, timeframe="Daily")
    assert combined["status"] == "COMPLETED"
    assert combined["trade_count"] == 0
    assert combined["sharpe"] is None
    assert combined["sharpe_reason"] == "zero_volatility"
    assert combined["net_pnl"] == 0.0
    assert combined["max_drawdown"] == 0.0
    assert combined["win_rate"] is None


def test_vectorized_scores_match_the_strict_bar_loop():
    from quant_edai.quant.alpha_catalog import FeatureContext, score_strategy_strict

    rng = np.random.default_rng(7)
    count = 40
    close = 100 + np.cumsum(rng.normal(0, 0.4, count))
    frame = pd.DataFrame({
        "bar_time": pd.date_range("2024-01-02", periods=count, freq="5min", tz="UTC"),
        "open": close,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": rng.integers(100, 1000, count).astype(float),
        "vwap": close * (1 + rng.normal(0, 0.001, count)),
    })
    frame = MarketDataService.calculate_indicators(frame)
    frame["bar_vol_ann"] = frame["close"].pct_change().rolling(10, min_periods=5).std() * np.sqrt(252)
    config = {"w_mom": 0.5, "w_tech": 0.3, "w_ml": 0.2, "entry_z": 1.0}
    probabilities = rng.uniform(0.2, 0.8, count).tolist()
    for strategy in ("momentum_z", "vwap_dev_z", "rsi_mr", "macd_hist_z", "sma_ratio", "vol_breakout", "volume_z", "ml_prob", "composite_eq", "regime_adapt"):
        scored = MarketDataService.score_candle_frame(
            frame, strategy, config, p_ml=probabilities, allow_asof=False,
        )
        closes = frame["close"].tolist()
        volumes = frame["volume"].tolist()
        vwap_devs = []
        for i, row in frame.iterrows():
            features = {}
            if i >= 4 and pd.notna(row["rsi_14"]):
                features["rsi_14d"] = float(row["rsi_14"])
            if i >= 4 and pd.notna(row["macd_histogram"]):
                features["macd_histogram"] = float(row["macd_histogram"])
            if i >= 4 and pd.notna(row["sma_20"]) and pd.notna(row["sma_50"]) and float(row["sma_50"]) != 0:
                features["sma_ratio_20_50"] = float(row["sma_20"]) / float(row["sma_50"])
            if i > 0 and closes[i - 1] != 0:
                features["intraday_return_1d"] = (closes[i] - closes[i - 1]) / closes[i - 1]
            if pd.notna(row["vwap"]) and float(row["vwap"]) != 0:
                vwap_devs.append((closes[i] - float(row["vwap"])) / float(row["vwap"]))
            vol = float(row["bar_vol_ann"]) if pd.notna(row["bar_vol_ann"]) else None
            if strategy == "regime_adapt" and vol is None:
                expected = 0.0
            else:
                ctx = FeatureContext(
                    ticker="AAPL",
                    price=closes[i],
                    p_ml=float(probabilities[i]),
                    volume=volumes[i],
                    features=features,
                    close_buf=closes[: i + 1],
                    volume_buf=volumes[: i + 1],
                    vwap_buf=list(vwap_devs),
                    realized_vol_ann=0.0 if vol is None else vol,
                )
                expected, _ = score_strategy_strict(strategy, ctx, config)
            assert abs(float(scored.loc[i, "z_score"]) - expected) < 1e-8, (strategy, i, scored.loc[i, "z_score"], expected)
    frame = pd.DataFrame({
        "bar_time": pd.date_range("2024-01-02", periods=8, freq="5min", tz="UTC"),
        "open": [100.0] * 8,
        "high": [101.0] * 8,
        "low": [99.0] * 8,
        "close": [100.0 + i for i in range(8)],
        "volume": [1_000.0] * 8,
        "asof_sma_ratio_20_50": [2.0] * 8,
    })
    isolated = MarketDataService.score_candle_frame(frame, "sma_ratio", allow_asof=False)
    assert (isolated["z_score"].abs() < 1e-9).all()
    leaked = MarketDataService.score_candle_frame(frame, "sma_ratio", allow_asof=True)
    assert (leaked["z_score"].abs() > 1.0).any()


def _evaluated(alpha_id: str, version: str = "v1.0.0"):
    registry = {"alpha_id": alpha_id, "version": version, "alpha_type": "INTRADAY", "status": "PAPER_TRIAL"}
    evaluations = [
        {
            "alpha_id": alpha_id,
            "version": version,
            "split_id": split_id,
            "evaluation_status": "COMPLETED",
            "sharpe_ratio": -1.5 if split_id == "IS" else -1.2,
            "dataset_id": "ds_canonical_4y_eval_s05",
            "run_id": f"ev_{split_id}_5m_v100_{alpha_id}",
        }
        for split_id in ("IS", "OOS")
    ]
    return registry, evaluations


def test_replay_rejects_test_ids_that_are_not_in_the_registry():
    config = {
        "ensemble_id": "default",
        "approval_status": "APPROVED_FROZEN",
        "approved_by": "test",
        "updated_by": "test",
        "selected_alpha_ids": [f"alpha_ens_{i}_intraday" for i in range(5)],
        "weights": [0.2] * 5,
    }
    verdict = assess_replay_ensemble(config, [], [])
    assert verdict["ok"] is False
    assert "REJECTED_ALPHA_NOT_IN_REGISTRY" in verdict["reason"]
    assert "test writer" in verdict["reason"]


def test_replay_accepts_only_one_evaluated_version_per_member():
    ids = [f"alpha_real_{i}_intraday" for i in range(5)]
    registry = []
    evaluations = []
    for alpha_id in ids:
        row, runs = _evaluated(alpha_id)
        registry.append(row)
        evaluations.extend(runs)
    config = {
        "ensemble_id": "default",
        "approval_status": "APPROVED_FROZEN",
        "approved_by": "ADMIN_QUANT",
        "updated_by": "ADMIN_QUANT",
        "selected_alpha_ids": ids,
        "weights": [0.2] * 5,
    }
    verdict = assess_replay_ensemble(config, registry, evaluations)
    assert verdict["ok"] is True
    assert [row["alpha_id"] for row in verdict["alphas"]] == ids
    assert all(row["version"] == "v1.0.0" and row["weight"] == 0.2 for row in verdict["alphas"])

    registry.append({"alpha_id": ids[0], "version": "v1.1.0", "alpha_type": "INTRADAY"})
    evaluations.extend(_evaluated(ids[0], "v1.1.0")[1])
    ambiguous = assess_replay_ensemble(config, registry, evaluations)
    assert ambiguous["ok"] is False
    assert "versions" in ambiguous["reason"]


def _candidate(alpha_id, horizon, oos_sharpe, oos_mdd, oos_trades, is_sharpe=-1.0, status="COMPLETED"):
    return {
        "alpha_id": alpha_id,
        "version": "v1.0.0",
        "horizon": horizon,
        "is_status": status,
        "oos_status": status,
        "is_sharpe": None if status != "COMPLETED" else is_sharpe,
        "oos_sharpe": None if status != "COMPLETED" else oos_sharpe,
        "is_mdd": -0.1,
        "oos_mdd": oos_mdd,
        "is_trades": 10,
        "oos_trades": oos_trades,
        "is_run_id": f"ev_IS_5m_v100_{alpha_id}",
        "oos_run_id": f"ev_OOS_5m_v100_{alpha_id}",
        "is_sharpe_reason": None,
        "oos_sharpe_reason": None,
        "note": None if status == "COMPLETED" else "No signal rule is configured for this alpha.",
    }


def test_intraday_rank_uses_oos_sharpe_and_leaves_daily_alphas_out():
    frame = pd.DataFrame([
        _candidate("alpha_worse_intraday", "INTRADAY", -20.0, -0.01, 100),
        _candidate("alpha_better_intraday", "INTRADAY", -5.0, -0.40, 10),
        _candidate("alpha_tie_low_drawdown", "INTRADAY", -5.0, -0.02, 10),
        _candidate("alpha_sma_cross_daily", "DAILY", 3.0, -0.01, 40),
        _candidate("alpha_ml_prob_intraday", "INTRADAY", None, None, None, status="INELIGIBLE"),
    ])
    ranked = rank_ensemble_candidates(frame, "INTRADAY")
    intraday_ids = set(ranked["alpha_id"])
    assert "alpha_sma_cross_daily" not in intraday_ids
    eligible = ranked[ranked["eligible"]].sort_values("rank")
    assert list(eligible["alpha_id"]) == [
        "alpha_tie_low_drawdown",
        "alpha_better_intraday",
        "alpha_worse_intraday",
    ]
    excluded = ranked[ranked["alpha_id"] == "alpha_ml_prob_intraday"].iloc[0]
    assert excluded["eligible"] == False
    assert "INELIGIBLE" in excluded["exclusion_reason"]
    assert weight_block_reason([0.2, 0.2, 0.2, 0.2, 0.2]) is None
    assert weight_block_reason([0.5, 0.5, 0.5, 0.5, 0.5]) is not None


def test_approval_insert_keeps_the_previous_version():
    conn = sqlite3.connect(":memory:")
    payload = {
        "horizon": "INTRADAY",
        "dataset_id": "ds_canonical_4y_eval_s05",
        "alpha_ids": [f"alpha_{i}" for i in range(5)],
        "versions": ["v1.0.0"] * 5,
        "weights": [0.2] * 5,
        "approved_by": "ADMIN_PM",
        "member_evidence": [{"alpha_id": "alpha_0", "weight": 0.2}],
        "rank_snapshot": [{"rank": 1, "alpha_id": "alpha_0"}],
    }
    first = persist_ensemble_approval(conn, payload, is_postgres=False)
    payload["weights"] = [0.4, 0.2, 0.2, 0.1, 0.1]
    payload["member_evidence"] = [{"alpha_id": "alpha_0", "weight": 0.4}]
    second = persist_ensemble_approval(conn, payload, is_postgres=False)
    rows = conn.execute(
        "SELECT ensemble_version, weights_json FROM alpha_ensemble_approval ORDER BY ensemble_version"
    ).fetchall()
    audits = conn.execute(
        "SELECT action_type, alpha_id FROM pm_governance_audit ORDER BY audit_id"
    ).fetchall()
    conn.close()
    assert first["ok"] is True and first["ensemble_version"] == 1
    assert second["ok"] is True and second["ensemble_version"] == 2
    assert rows[0][1] == "[0.2, 0.2, 0.2, 0.2, 0.2]"
    assert rows[1][1] == "[0.4, 0.2, 0.2, 0.1, 0.1]"
    assert [row[0] for row in audits] == ["ENSEMBLE_APPROVE_FREEZE", "ENSEMBLE_APPROVE_FREEZE"]


def test_pytest_cannot_write_a_production_approval():
    refused = persist_ensemble_approval(
        None,
        {"approved_by": "ADMIN", "weights": [0.2] * 5},
        is_postgres=True,
    )
    assert refused["ok"] is False
    assert "production" in refused["reason"]
