"""Unit tests for alpha metrics service, K=5 ensemble filter, and top-5 entry gate."""

from __future__ import annotations

import json
import pytest

from quant_edai.quant.alpha_catalog import ALPHA_VERSION
from quant_edai.quant.alpha_metrics_service import METRICS_KEYS, run_alpha_metrics
from quant_edai.quant.alpha_registry import AlphaRegistry, AlphaStatus
from quant_edai.quant.alpha_serving import AlphaSignalServer
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine


@pytest.fixture
def memory_db(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "metrics_test.db"))
    db.init_db(reset=True)
    return db


def _seed_alpha(db: PaperDatabase, alpha_id: str, horizon: str = "INTRADAY", sharpe: float = 1.5):
    db.upsert_alpha(
        {
            "alpha_id": alpha_id,
            "version": ALPHA_VERSION,
            "alpha_type": horizon,
            "status": AlphaStatus.PAPER_TRIAL.value,
            "description": f"test {alpha_id}",
            "config_json": {
                "strategy": "momentum_z",
                "entry_z": 0.5,
                "exit_z": 0.1,
                "sl_pct": 0.05,
                "horizon": horizon,
            },
            "metrics_snapshot": {"ic_mean": 0.06, "sharpe": sharpe, "seeded": True},
            "created_at": "2024-01-01T00:00:00Z",
            "promoted_at": "2024-01-01T00:00:00Z",
            "promoted_by": "test",
        }
    )


def test_alpha_metrics_keys_and_window_used(memory_db, tmp_path):
    for i in range(3):
        _seed_alpha(memory_db, f"alpha_test_{i}_intraday", "INTRADAY", sharpe=1.0 + i * 0.1)

    result = run_alpha_metrics(
        db=memory_db,
        source_dir=str(tmp_path / "missing_lakehouse"),
        booksize=100_000.0,
        horizon="INTRADAY",
    )
    # Missing lakehouse -> synthetic panel -> labelled SIMULATED (never bare SUCCESS)
    assert result["status"] == "PASSED_SIMULATED"
    assert result["execution_mode"] == "SIMULATED"
    assert result["alphas_updated"] == 3
    assert "window_used" in result["window"]
    assert result["window"]["window_used"] in ("full_available", "last_2y", "empty")

    rows = memory_db.list_alphas(alpha_type="INTRADAY")
    assert len(rows) == 3
    for row in rows:
        metrics = row["metrics_snapshot"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        for key in METRICS_KEYS:
            assert key in metrics, f"missing metrics key {key}"
        assert metrics["booksize"] == 100_000.0
        assert metrics["window_used"]


def test_ensemble_filter_combines_only_selected_k5(memory_db):
    """With 5 selected ids, server combines only those K=5 alphas."""
    ids = [f"alpha_ens_{i}_intraday" for i in range(8)]
    for aid in ids:
        _seed_alpha(memory_db, aid, "INTRADAY")

    selected = ids[:5]
    memory_db.upsert_ensemble_config(
        {
            "ensemble_id": "default",
            "horizon": "INTRADAY",
            "selected_alpha_ids": selected,
            "booksize": 100_000.0,
            "top_k_tickers": 5,
            "updated_by": "test",
        }
    )

    registry = AlphaRegistry(db=memory_db)
    server = AlphaSignalServer(db=memory_db, registry=registry, z_threshold=0.5)
    active = server.get_active_alphas_for_serving(horizon="INTRADAY", mode="paper")
    assert len(active) == 5
    assert {a["alpha_id"] for a in active} == set(selected)

    sig = server.get_intraday_signal(
        ticker="AAPL",
        bar_price=180.0,
        bar_timestamp="2024-03-15T10:00:00Z",
        mode="paper",
    )
    assert "EqualAvg(K=5)" in sig.notes
    assert len(sig.alphas_combined) == 5
    for name in sig.alphas_combined:
        aid = name.split(":")[0]
        assert aid in selected

    breakdown = server.get_combined_signal_breakdown(
        ticker="AAPL",
        bar_price=180.0,
        bar_timestamp="2024-03-15T10:00:00Z",
        horizon="INTRADAY",
        mode="paper",
    )
    assert breakdown["k"] == 5
    assert set(breakdown["selected_alpha_ids"]) == set(selected)
    assert "combined_z" in breakdown


def test_top5_entry_gate_blocks_outside_topk(memory_db):
    """Ticker outside top-5 by combined z does not open when capacity policy on."""
    engine = PaperTradingEngine(
        db=memory_db,
        starting_cash=100_000.0,
        trade_notional=10_000.0,
        max_positions=5,
        alpha_source="inline",
        z_entry_threshold=0.5,
        sl_pct=None,
        sl_bps=50.0,
        tp_bps=100.0,
    )
    engine.enforce_top_k_gate = True
    engine.top_k_tickers = 5

    # Seed cross-section: 5 strong names + 1 weak outsider
    leaders = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]
    for i, t in enumerate(leaders):
        engine.cross_section_scores[t] = 3.0 - 0.1 * i
    engine.cross_section_scores["LOSER"] = -2.0  # far outside top-5 for LONG

    # Warm buffers then try to force a LONG entry signal on LOSER via inline thresholds
    for i in range(20):
        bar = MarketBar(
            ticker_symbol="LOSER",
            timestamp=f"2024-03-15T10:{i:02d}:00Z",
            open_price=100.0,
            high_price=101.0,
            low_price=99.5,
            close_price=100.0 + 0.5,  # positive mom each bar
            volume=10_000.0,
            vwap=100.0,
            p_ml=0.80,
        )
        # Keep outsider score low so gate blocks even if inline z fires
        res = engine.on_bar(bar)
        engine.cross_section_scores["LOSER"] = -2.0
        for t in leaders:
            engine.cross_section_scores[t] = 3.0

    assert "LOSER" not in engine.positions
    # Confirm gate helper itself rejects LONG for outsider
    assert engine._is_in_top_k("LOSER", "LONG") is False
    assert engine._is_in_top_k("AAPL", "LONG") is True
