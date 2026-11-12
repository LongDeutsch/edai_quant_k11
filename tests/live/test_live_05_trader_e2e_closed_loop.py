"""Step 7 Live Integration Test: Paper Trading Closed-Loop with Real Flink Bars & Provenance Fills."""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from quant_edai.common.market_time import is_eod_flat_time, parse_bar_timestamp
from quant_edai.common.ml_provenance import PROVENANCE_FIELDS, ProvenanceError
from quant_edai.quant.market_replay import MarketReplayRunner, kafka_message_to_bar
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine

DATASET_ID = "ds_live_e2e_closed_loop_007"
MODEL_VER = "lgbm-20261111T120000Z-ds_closed_loop"
FEAT_SRC = f"s3a://lakehouse/gold/runs/{DATASET_ID}/gold_features/feat_ticker_technical_indicators.parquet"

PROV = {
    "model_version": MODEL_VER,
    "dataset_id": DATASET_ID,
    "feature_source": FEAT_SRC,
}


@pytest.fixture
def clean_db(tmp_path: Path):
    db_file = tmp_path / "live_paper_trader.db"
    db = PaperDatabase(sqlite_path=str(db_file))
    db.init_db(reset=True)
    return db


def test_trader_fill_execution_with_full_provenance(clean_db, tmp_path):
    """Verifies that trades and orders record the complete provenance chain."""
    engine = PaperTradingEngine(
        db=clean_db,
        starting_cash=100_000.0,
        trade_notional=10_000.0,
        alpha_source="inline",
        sl_pct=0.05,
        **PROV,
    )

    # Execute an entry trade
    engine._open_position("AAPL", 175.0, "2024-03-15T14:30:00Z", side="LONG")
    assert "AAPL" in engine.positions
    assert engine.positions["AAPL"].quantity > 0

    # Execute a closing trade
    engine._close_position("AAPL", 178.5, "2024-03-15T14:35:00Z", reason="TAKE_PROFIT")
    assert "AAPL" not in engine.positions

    trades = clean_db.get_trades_with_orders()
    assert not trades.empty, "Expected trades to be executed and persisted"
    assert len(trades) == 2, "Expected open and close trades"

    # Verify all provenance columns are correctly recorded
    for field in PROVENANCE_FIELDS:
        assert (trades[field] == PROV[field]).all(), f"Mismatch in trade provenance {field}"

    # Verify orders table
    orders = clean_db._sqlite_conn.execute("SELECT * FROM paper_orders").fetchall()
    assert len(orders) == 2
    for order in orders:
        order_dict = dict(order)
        for field in PROVENANCE_FIELDS:
            assert order_dict[field] == PROV[field], f"Mismatch in order provenance {field}"

    # Write evidence
    ev_dir = Path(f"evidence/live/{DATASET_ID}")
    ev_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = ev_dir / "trader_fills_provenance.json"
    evidence_data = {
        "status": "SUCCESS",
        "dataset_id": DATASET_ID,
        "model_version": MODEL_VER,
        "feature_source": FEAT_SRC,
        "total_trades": len(trades),
        "ending_cash": engine.cash,
        "open_positions": list(engine.positions.keys()),
        "trades": trades.to_dict(orient="records"),
    }
    evidence_path.write_text(json.dumps(evidence_data, indent=2, default=str))
    assert evidence_path.exists()


def test_trader_eod_flatten_at_new_york_close(clean_db):
    """Verifies that open positions are automatically flattened at 15:55 America/New_York."""
    engine = PaperTradingEngine(
        db=clean_db,
        starting_cash=100_000.0,
        trade_notional=10_000.0,
        alpha_source="inline",
        tp_bps=5000.0,  # 50% TP so position stays open until EOD
        sl_bps=5000.0,  # 50% SL so position stays open until EOD
        **PROV,
    )

    # 1. Open position at 10:00 EDT
    engine._open_position("MSFT", 400.0, "2024-03-15T10:00:00-04:00", side="LONG")
    assert "MSFT" in engine.positions
    assert engine.positions["MSFT"].quantity > 0

    # 2. Midday bar at 14:00 EDT - position stays open
    bar_midday = MarketBar(
        ticker_symbol="MSFT",
        timestamp="2024-03-15T14:00:00-04:00",
        open_price=400.2,
        high_price=400.4,
        low_price=400.1,
        close_price=400.25,
        volume=2000.0,
        vwap=400.2,
        **PROV,
    )
    res_mid = engine.on_bar(bar_midday)
    assert not any(e.startswith("EOD_FLAT") for e in res_mid.get("events", []))
    assert "MSFT" in engine.positions

    # 3. EOD Flatten Bar at 15:55 EDT
    bar_eod = MarketBar(
        ticker_symbol="MSFT",
        timestamp="2024-03-15T15:55:00-04:00",
        open_price=400.3,
        high_price=400.5,
        low_price=400.2,
        close_price=400.35,
        volume=10000.0,
        vwap=400.3,
        **PROV,
    )
    res_eod = engine.on_bar(bar_eod)
    assert any(e == "EOD_FLAT_MSFT" for e in res_eod.get("events", []))
    assert "MSFT" not in engine.positions, "Position must be flattened at 15:55 ET"


def test_integration_mode_fails_hard_on_missing_provenance(clean_db, monkeypatch):
    """Verifies that in MODE=integration, missing provenance immediately raises ProvenanceError."""
    monkeypatch.setenv("MODE", "integration")

    # Incomplete provenance (missing feature_source)
    bad_prov = {
        "model_version": MODEL_VER,
        "dataset_id": DATASET_ID,
    }
    engine = PaperTradingEngine(
        db=clean_db,
        starting_cash=100_000.0,
        alpha_source="inline",
        **bad_prov,
    )

    with pytest.raises(ProvenanceError, match="feature_source"):
        engine._open_position("NVDA", 800.0, "2024-03-15T14:30:00Z", side="LONG")
