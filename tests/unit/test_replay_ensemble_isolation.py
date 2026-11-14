"""Replay ensemble tests stay on SQLite and never treat a missing alpha as HOLD."""

from __future__ import annotations

import psycopg2

from quant_edai.quant.alpha_serving import AlphaSignalServer
from quant_edai.quant.paper_db import PaperDatabase


def test_explicit_sqlite_path_does_not_change_production_ensemble(tmp_path):
    conn = psycopg2.connect(
        "postgresql://quant:quant_password@localhost:5432/quant_db",
        connect_timeout=3,
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT horizon, selected_alpha_ids, approval_status, approved_by, updated_at
        FROM alpha_ensemble_config
        WHERE ensemble_id = 'default'
        ORDER BY horizon
        """
    )
    before = cur.fetchall()
    cur.close()
    conn.close()

    db = PaperDatabase(sqlite_path=str(tmp_path / "replay_isolation.db"))
    assert db.is_postgres is False
    db.init_db(reset=True)
    db.upsert_ensemble_config({
        "ensemble_id": "default",
        "horizon": "INTRADAY",
        "selected_alpha_ids": [f"alpha_ens_{i}_intraday" for i in range(5)],
        "updated_by": "test",
        "approved_by": "test",
    })

    conn = psycopg2.connect(
        "postgresql://quant:quant_password@localhost:5432/quant_db",
        connect_timeout=3,
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT horizon, selected_alpha_ids, approval_status, approved_by, updated_at
        FROM alpha_ensemble_config
        WHERE ensemble_id = 'default'
        ORDER BY horizon
        """
    )
    after = cur.fetchall()
    cur.close()
    conn.close()
    assert after == before


def test_missing_registry_alpha_is_rejected_instead_of_flat(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "replay_signal.db"))
    db.init_db(reset=True)
    server = AlphaSignalServer(db=db)
    signal = server.get_intraday_signal(
        ticker="AAPL",
        bar_price=180.0,
        bar_timestamp="2026-05-01T14:00:00Z",
        alpha_ids=["alpha_ens_0_intraday"],
        allow_default_composite=False,
        strict=True,
    )
    assert signal.action == "REJECTED"
    assert signal.notes.startswith("REJECTED_ALPHA_NOT_IN_REGISTRY")
    assert signal.alpha_scores[0]["action"] == "REJECTED_ALPHA_NOT_IN_REGISTRY"
    assert signal.alpha_scores[0]["z_score"] is None
