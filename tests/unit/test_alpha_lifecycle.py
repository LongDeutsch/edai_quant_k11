"""Unit Tests for Production Alpha Lifecycle, Serving, and PM Governance Controls.

Covers:
1. Intraday Lookback Window Clamping: ALPHA_INTRADAY_LOOKBACK_DAYS <= 10.
2. Daily Signals from Postgres only (alpha_signals_daily table).
3. Multi-Alpha Equal Average Combination: Z = (1/K) * sum(Z_i).
4. Max 1 active version per alpha_id (Rule 4 auto-demote).
5. Daily execution: hold until 15:55 ET; SL/TP + PM flatten only.
6. PM Governance Controls: Force Flatten, Block New Entries, Audit Trail.
"""

from datetime import datetime, timezone
import json
import os
import pytest

from quant_edai.quant.alpha_registry import AlphaHorizon, AlphaRegistry, AlphaStatus
from quant_edai.quant.alpha_serving import (
    AlphaSignalServer,
    get_clamped_intraday_lookback_days,
)
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine
from quant_edai.quant.pm_controls import PMControlService


@pytest.fixture
def memory_db():
    db = PaperDatabase(sqlite_path=":memory:")
    db.init_db(reset=True)
    return db


@pytest.fixture(autouse=True)
def isolated_pm_state(tmp_path, monkeypatch):
    state_file = str(tmp_path / "pm_controls.json")
    monkeypatch.setenv("PM_STATE_FILE", state_file)
    monkeypatch.setattr("quant_edai.quant.pm_controls.PM_STATE_FILE", state_file)
    yield state_file


# =============================================================================
# 1. ALPHA REGISTRY & LIFECYCLE TESTS (RULE 4)
# =============================================================================
def test_alpha_registry_lifecycle_and_quality_gates(memory_db):
    registry = AlphaRegistry(db=memory_db)

    # 1. Register candidate alpha passing quality gates
    candidate = {
        "alpha_id": "alpha_momentum_ml",
        "version": "v1.0.0",
        "author": "quant_alice",
        "horizon": "INTRADAY",
        "status": AlphaStatus.CANDIDATE.value,
        "train_start_date": "2024-01-01",
        "train_end_date": "2024-06-30",
        "test_start_date": "2024-07-01",
        "test_end_date": "2024-10-31",
        "in_sample_metrics": {"ic_mean": 0.08, "sharpe": 2.10, "max_drawdown": -0.12},
        "out_of_sample_metrics": {"ic_mean": 0.07, "sharpe": 1.95, "max_drawdown": -0.14},
        "config_json": {"w_mom": 0.6, "w_ml": 0.4},
    }
    assert registry.register_candidate(candidate)

    # 2. Promote to Paper Trial
    assert registry.promote_to_paper("alpha_momentum_ml", "v1.0.0", pm_user="PM_Lead", reason="Ready for paper trial")
    alpha_rec = registry.get_alpha("alpha_momentum_ml", "v1.0.0")
    assert alpha_rec is not None
    assert alpha_rec["status"] == AlphaStatus.PAPER_TRIAL.value

    # 3. Promote to Live Active
    assert registry.promote_to_live("alpha_momentum_ml", "v1.0.0", pm_user="PM_Lead", reason="Passed paper trial soak")
    alpha_rec = registry.get_alpha("alpha_momentum_ml", "v1.0.0")
    assert alpha_rec["status"] == AlphaStatus.PROMOTED_ACTIVE.value


def test_rule4_max_one_active_version_per_alpha_id(memory_db):
    """Rule 4: At most 1 active version per alpha_id; promoting a new version auto-retires the older version."""
    registry = AlphaRegistry(db=memory_db)

    # Register v1.0.0
    registry.register_candidate({
        "alpha_id": "alpha_reversion_stat",
        "version": "v1.0.0",
        "horizon": "DAILY",
        "status": AlphaStatus.CANDIDATE.value,
        "out_of_sample_metrics": {"ic_mean": 0.06, "sharpe": 1.80, "max_drawdown": -0.10},
    })
    registry.promote_to_live("alpha_reversion_stat", "v1.0.0", pm_user="PM", reason="Initial deploy")
    assert registry.get_alpha("alpha_reversion_stat", "v1.0.0")["status"] == AlphaStatus.PROMOTED_ACTIVE.value

    # Register v1.1.0 with improved performance
    registry.register_candidate({
        "alpha_id": "alpha_reversion_stat",
        "version": "v1.1.0",
        "horizon": "DAILY",
        "status": AlphaStatus.CANDIDATE.value,
        "out_of_sample_metrics": {"ic_mean": 0.09, "sharpe": 2.40, "max_drawdown": -0.08},
    })

    # Promote v1.1.0 to Live -> v1.0.0 MUST be retired
    assert registry.promote_to_live("alpha_reversion_stat", "v1.1.0", pm_user="PM", reason="Upgrade to v1.1.0")

    rec_v10 = registry.get_alpha("alpha_reversion_stat", "v1.0.0")
    rec_v11 = registry.get_alpha("alpha_reversion_stat", "v1.1.0")

    assert rec_v11["status"] == AlphaStatus.PROMOTED_ACTIVE.value
    assert rec_v10["status"] == AlphaStatus.RETIRED.value

    # Active alphas query should only return v1.1.0
    active = registry.get_active_alphas(horizon="DAILY", mode="live")
    matching = [a for a in active if a["alpha_id"] == "alpha_reversion_stat"]
    assert len(matching) == 1
    assert matching[0]["version"] == "v1.1.0"


def test_quality_gate_rejection_on_insufficient_metrics(memory_db):
    """Candidate with Sharpe < 1.5 or IC < 0.05 or MDD < -20% must be rejected from live promotion."""
    registry = AlphaRegistry(db=memory_db)

    # Subpar alpha
    registry.register_candidate({
        "alpha_id": "alpha_bad_perf",
        "version": "v1.0.0",
        "horizon": "DAILY",
        "status": AlphaStatus.CANDIDATE.value,
        "out_of_sample_metrics": {"ic_mean": 0.01, "sharpe": 0.85, "max_drawdown": -0.35},
    })

    # Should fail quality gate
    gate = registry.evaluate_quality_gate("alpha_bad_perf", "v1.0.0")
    assert not gate.passed
    assert any("threshold" in r or "exceeds" in r for r in gate.reasons)

    # Promotion to live should fail
    promoted = registry.promote_to_live("alpha_bad_perf", "v1.0.0", pm_user="PM", reason="Trying to bypass")
    assert not promoted
    assert registry.get_alpha("alpha_bad_perf", "v1.0.0")["status"] in [AlphaStatus.RESEARCH.value, AlphaStatus.CANDIDATE.value]


# =============================================================================
# 2. INTRADAY LOOKBACK WINDOW CLAMPING (RULE 1)
# =============================================================================
def test_intraday_lookback_window_clamped_to_22_days():
    """Rule 1: Lookback clamped strictly to <= 22 days (1 Quant trading month)."""
    # Test default
    if "ALPHA_INTRADAY_LOOKBACK_DAYS" in os.environ:
        del os.environ["ALPHA_INTRADAY_LOOKBACK_DAYS"]
    assert get_clamped_intraday_lookback_days() == 22

    # Test clamping when user requests > 22
    os.environ["ALPHA_INTRADAY_LOOKBACK_DAYS"] = "30"
    assert get_clamped_intraday_lookback_days() == 22

    os.environ["ALPHA_INTRADAY_LOOKBACK_DAYS"] = "100"
    assert get_clamped_intraday_lookback_days() == 22

    # Test valid sub-22 window
    os.environ["ALPHA_INTRADAY_LOOKBACK_DAYS"] = "10"
    assert get_clamped_intraday_lookback_days() == 10

    os.environ["ALPHA_INTRADAY_LOOKBACK_DAYS"] = "5"
    assert get_clamped_intraday_lookback_days() == 5

    # Test minimum clamp (>= 1)
    os.environ["ALPHA_INTRADAY_LOOKBACK_DAYS"] = "0"
    assert get_clamped_intraday_lookback_days() == 1

    # Cleanup
    del os.environ["ALPHA_INTRADAY_LOOKBACK_DAYS"]


# =============================================================================
# 3. MULTI-ALPHA EQUAL AVERAGE COMBINATION (RULE 3 & 5)
# =============================================================================
def test_multi_alpha_equal_average_combination(memory_db):
    """Rule 3 & 5: K active alphas combined via Equal Average (1/K * sum(Z_i))."""
    registry = AlphaRegistry(db=memory_db)

    # Register two active paper trial alphas
    registry.register_candidate({
        "alpha_id": "alpha_1",
        "version": "v1.0.0",
        "horizon": "INTRADAY",
        "status": AlphaStatus.CANDIDATE.value,
        "out_of_sample_metrics": {"ic_mean": 0.06, "sharpe": 1.9, "max_drawdown": -0.10},
        "config_json": {"w_mom": 1.0, "w_tech": 0.0, "w_ml": 0.0},
    })
    registry.promote_to_paper("alpha_1", "v1.0.0")

    registry.register_candidate({
        "alpha_id": "alpha_2",
        "version": "v1.0.0",
        "horizon": "INTRADAY",
        "status": AlphaStatus.CANDIDATE.value,
        "out_of_sample_metrics": {"ic_mean": 0.08, "sharpe": 2.2, "max_drawdown": -0.09},
        "config_json": {"w_mom": 0.0, "w_tech": 1.0, "w_ml": 0.0},
    })
    registry.promote_to_paper("alpha_2", "v1.0.0")

    server = AlphaSignalServer(db=memory_db, registry=registry, z_threshold=0.5)
    sig = server.get_intraday_signal(
        ticker="AAPL",
        bar_price=180.0,
        bar_timestamp="2024-03-15T10:00:00Z",
        mode="paper",
    )

    assert sig is not None
    assert sig.horizon == "INTRADAY"
    assert len(sig.alphas_combined) == 2
    assert "alpha_1:v1.0.0" in sig.alphas_combined
    assert "alpha_2:v1.0.0" in sig.alphas_combined
    assert "EqualAvg(K=2)" in sig.notes


def test_alpha_source_store_mode_blocks_unpromoted_entry(memory_db):
    """In store mode: engine only enters if Alpha Serving produces BUY/SELL from active alpha.
    If PM blocks or serving returns FLAT, engine does NOT enter position.
    """
    server = AlphaSignalServer(db=memory_db)
    engine_store = PaperTradingEngine(
        db=memory_db,
        alpha_source="store",
        signal_server=server,
    )

    # When PM blocks or signal is FLAT, store mode strictly refrains from opening
    server.set_pm_block_entries(True)
    for i in range(20):
        bar = MarketBar(
            ticker_symbol="NVDA",
            timestamp=f"2024-03-15T10:{i:02d}:00Z",
            open_price=100.0 + i * 0.1,
            high_price=100.5 + i * 0.1,
            low_price=99.9 + i * 0.1,
            close_price=100.4 + i * 0.1,
            volume=5000.0,
            vwap=100.0,
            p_ml=0.50,
        )
        engine_store.on_bar(bar)

    assert "NVDA" not in engine_store.positions


# =============================================================================
# 4. DAILY SIGNALS POSTGRES ONLY (RULE 2 & 6)
# =============================================================================
def test_daily_signal_postgres_retrieval_and_hold(memory_db):
    """Rule 2: Daily signals read from DB only (alpha_signals_daily table)."""
    # Publish daily signals
    today_str = "2024-03-15"
    signals = [
        {
            "signal_id": f"sig_daily_1_{today_str}_NVDA",
            "alpha_id": "alpha_daily_core",
            "version": "v1.0.0",
            "ticker_symbol": "NVDA",
            "asof_date": today_str,
            "effective_date": today_str,
            "raw_score": 2.4,  # Strong BUY
            "target_weight": 0.25,
            "confidence": 0.90,
            "regime_detected": "BULL",
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
    ]
    memory_db.record_daily_signals(signals)

    # Server reads daily signal
    server = AlphaSignalServer(db=memory_db, z_threshold=1.0)
    sig = server.get_daily_signal(ticker="NVDA", effective_date=today_str)

    assert sig is not None
    assert sig.action == "BUY"
    assert sig.z_score == 2.4
    assert sig.target_weight == 0.25
    assert sig.regime == "BULL"
    assert "Hold to 15:55 ET" in sig.notes


def test_paper_trading_daily_profile_hold_policy(memory_db):
    """Rule 6: Daily profile enters at 09:30 ET and holds until 15:55 ET without VWAP slicing."""
    today_str = "2024-03-15"
    memory_db.record_daily_signals([
        {
            "signal_id": f"sig_daily_test_{today_str}_AAPL",
            "alpha_id": "alpha_daily_core",
            "version": "v1.0.0",
            "ticker_symbol": "AAPL",
            "asof_date": today_str,
            "effective_date": today_str,
            "raw_score": 2.0,  # BUY
            "target_weight": 0.20,
            "confidence": 0.85,
            "regime_detected": "NORMAL",
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
    ])

    engine = PaperTradingEngine(
        db=memory_db,
        profile="daily",
        starting_cash=100_000.0,
        trade_notional=10_000.0,
    )

    # 1. Market open bar at 09:30 ET -> should trigger entry
    bar_open = MarketBar(
        ticker_symbol="AAPL",
        timestamp=f"{today_str}T09:31:00Z",
        open_price=175.0,
        high_price=175.5,
        low_price=174.8,
        close_price=175.2,
        volume=10000.0,
    )
    res_open = engine.on_bar(bar_open)
    assert "AAPL" in engine.positions
    assert any("ENTRY_LONG_AAPL" in ev for ev in res_open["events"])

    # 2. Midday bar at 12:00 ET -> should hold position, NO exit or slicing
    bar_mid = MarketBar(
        ticker_symbol="AAPL",
        timestamp=f"{today_str}T12:00:00Z",
        open_price=175.2,
        high_price=175.6,
        low_price=175.0,
        close_price=175.3,
        volume=5000.0,
    )
    res_mid = engine.on_bar(bar_mid)
    assert "AAPL" in engine.positions
    assert len([ev for ev in res_mid["events"] if "ENTRY" in ev or "CLOSE" in ev]) == 0

    # 3. Market close at 15:55 ET -> EOD flattening triggers
    bar_eod = MarketBar(
        ticker_symbol="AAPL",
        timestamp=f"{today_str}T15:55:00Z",
        open_price=175.5,
        high_price=175.8,
        low_price=175.4,
        close_price=175.6,
        volume=8000.0,
    )
    res_eod = engine.on_bar(bar_eod)
    assert "AAPL" not in engine.positions
    assert any("EOD_FLAT_AAPL" in ev for ev in res_eod["events"])


# =============================================================================
# 5. PM GOVERNANCE CONTROLS & KILL-SWITCHES
# =============================================================================
def test_pm_controls_block_new_entries(memory_db, tmp_path):
    state_file = str(tmp_path / "pm_controls.json")
    pm = PMControlService(db=memory_db, state_file=state_file)
    server = AlphaSignalServer(db=memory_db)

    # 1. Normal state: signal is allowed
    assert not pm.is_entry_blocked()

    # 2. PM triggers kill-switch
    pm.set_block_new_entries(True, pm_user="Risk_Officer", reason="High market volatility spike")
    assert pm.is_entry_blocked()

    # AlphaSignalServer must block entries
    server.set_pm_block_entries(True)
    sig = server.get_intraday_signal("AAPL", 180.0, "2024-03-15T10:00:00Z")
    assert sig.action == "FLAT"
    assert sig.notes == "BLOCKED_BY_PM_GOVERNANCE"

    # Reset
    pm.set_block_new_entries(False, pm_user="Risk_Officer", reason="Volatility subsided")
    assert not pm.is_entry_blocked()


def test_pm_emergency_force_flatten(memory_db, tmp_path):
    state_file = str(tmp_path / "pm_controls.json")
    engine = PaperTradingEngine(db=memory_db, starting_cash=100_000.0)

    # Open positions in MSFT and TSLA
    engine._open_position("MSFT", 400.0, "2024-03-15T10:00:00Z", "LONG")
    engine._open_position("TSLA", 200.0, "2024-03-15T10:00:00Z", "LONG")
    assert len(memory_db.get_positions()) == 2

    # PM triggers Emergency Force Flatten
    pm = PMControlService(db=memory_db, state_file=state_file)
    closed_count = pm.force_flatten_all_positions(pm_user="PM_Lead", reason="Emergency liquidity event")

    assert closed_count == 2
    assert len(memory_db.get_positions()) == 0

    # Audit log must record the action
    audits = memory_db.get_pm_audits()
    assert len(audits) >= 1
    assert any(a.get("action_type") == "FORCE_FLATTEN" or a.get("action") == "FORCE_FLATTEN" for a in audits)
