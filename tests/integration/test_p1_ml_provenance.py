"""P1.3: Features/Labels + ML provenance (model_version, dataset_id, feature_source)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from quant_edai.common.dataset_paths import write_current_pointer
from quant_edai.common.ml_provenance import (
    PROVENANCE_FIELDS,
    ProvenanceError,
    dataset_id_from_path,
    require_provenance,
    write_model_card,
    read_model_card,
)
from quant_edai.quant.paper_db import PaperDatabase

GOOD_PROV = {
    "model_version": "lgbm-20250101T000000Z-ds_p1",
    "dataset_id": "ds_p1",
    "feature_source": "s3a://lakehouse/gold/runs/ds_p1/gold_features/feat_ticker_technical_indicators.parquet",
}


def _signal(**extra):
    return {
        "signal_id": "sig_a_2025-01-03_AAPL",
        "alpha_id": "alpha_a_daily",
        "version": "v1.0.0",
        "ticker_symbol": "AAPL",
        "asof_date": "2025-01-02",
        "effective_date": "2025-01-03",
        "target_side": "BUY",
        "target_weight": 0.3,
        "confidence": 0.8,
        "raw_score": 1.2,
        **extra,
    }


def _alpha(**extra):
    return {
        "alpha_id": "alpha_a_daily",
        "version": "v1.0.0",
        "alpha_type": "DAILY",
        "status": "PAPER_TRIAL",
        "description": "t",
        "config_json": {},
        "metrics_snapshot": {},
        **extra,
    }


@pytest.fixture
def db(tmp_path):
    d = PaperDatabase(sqlite_path=str(tmp_path / "p1.db"))
    d.init_db(reset=True)
    return d


@pytest.mark.p1
def test_ml_provenance_required_on_signals(integration_env, db) -> None:
    """integration|full: signals / registry rows without provenance are rejected; with it they persist."""
    for missing in PROVENANCE_FIELDS:
        prov = {k: v for k, v in GOOD_PROV.items() if k != missing}
        with pytest.raises(ProvenanceError, match=missing):
            db.record_daily_signals([_signal(**prov)])
        with pytest.raises(ProvenanceError, match=missing):
            db.upsert_alpha(_alpha(**prov))

    # Synthetic / simulated lineage is not real lineage in integration
    with pytest.raises(ProvenanceError):
        db.record_daily_signals([_signal(**{**GOOD_PROV, "feature_source": "synthetic_panel"})])
    with pytest.raises(ProvenanceError):
        db.record_daily_signals([_signal(**{**GOOD_PROV, "model_version": "SIMULATED"})])
    assert db.get_daily_signals("2025-01-03").empty

    assert db.record_daily_signals([_signal(**GOOD_PROV)]) == 1
    db.upsert_alpha(_alpha(**GOOD_PROV))

    sig = db.get_daily_signals("2025-01-03").iloc[0]
    reg = db.get_alpha("alpha_a_daily", "v1.0.0")
    for f in PROVENANCE_FIELDS:
        assert sig[f] == GOOD_PROV[f]
        assert reg[f] == GOOD_PROV[f]


@pytest.mark.p1
def test_smoke_signals_are_stamped_simulated(smoke_env, db) -> None:
    assert db.record_daily_signals([_signal()]) == 1
    sig = db.get_daily_signals("2025-01-03").iloc[0]
    assert {sig[f] for f in PROVENANCE_FIELDS} == {"SIMULATED"}
    out = require_provenance({}, mode="smoke")
    assert out["provenance_status"] == "SIMULATED"


@pytest.mark.p1
def test_legacy_sqlite_db_is_migrated(smoke_env, tmp_path) -> None:
    import sqlite3

    path = tmp_path / "legacy.db"
    con = sqlite3.connect(path)
    con.executescript(
        """CREATE TABLE alpha_signals_daily (signal_id TEXT PRIMARY KEY, alpha_id TEXT NOT NULL, version TEXT NOT NULL,
        ticker_symbol TEXT NOT NULL, asof_date TEXT NOT NULL, effective_date TEXT NOT NULL, target_side TEXT NOT NULL,
        target_weight REAL NOT NULL, confidence REAL NOT NULL, raw_score REAL NOT NULL, regime_detected TEXT,
        created_at TEXT NOT NULL, CONSTRAINT uq_daily_signal UNIQUE (alpha_id, version, ticker_symbol, effective_date));"""
    )
    con.commit()
    con.close()
    legacy = PaperDatabase(sqlite_path=str(path))
    cols = {r[1] for r in legacy._sqlite_conn.execute("PRAGMA table_info(alpha_signals_daily)").fetchall()}
    assert set(PROVENANCE_FIELDS) <= cols


@pytest.mark.p1
def test_model_card_roundtrip(tmp_path) -> None:
    model = tmp_path / "models" / "champion_model.joblib"
    write_model_card(model, **GOOD_PROV)
    assert read_model_card(model)["model_version"] == GOOD_PROV["model_version"]
    assert dataset_id_from_path("/x/lakehouse/runs/ds_9/gold_features/f.parquet") == "ds_9"
    assert dataset_id_from_path("/tmp/lakehouse_source/gold_features/f.parquet") is None


def _write_gold_features(out_dir: Path, dataset_id: str) -> Path:
    root = out_dir / "runs" / dataset_id
    feat_dir = root / "gold_features"
    feat_dir.mkdir(parents=True)
    rng = np.random.default_rng(1)
    rows = []
    for t in ("AAPL", "MSFT", "NVDA"):
        for d in pd.bdate_range("2024-12-02", "2025-01-02"):
            rows.append(
                {
                    "ticker_symbol": t,
                    "event_timestamp": f"{d:%Y-%m-%d} 16:00:00",
                    "close_price": 100 + float(rng.normal()),
                    "rsi_14d": float(rng.uniform(20, 80)),
                    "macd_histogram": float(rng.normal()),
                    "sma_ratio_20_50": 1.0 + float(rng.normal(0, 0.02)),
                    "intraday_return_1d": float(rng.normal(0, 0.01)),
                    "rolling_volatility_30d": 0.2,
                    "p_ml": float(rng.uniform(0.3, 0.7)),
                }
            )
    path = feat_dir / "feat_ticker_technical_indicators.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    write_current_pointer(out_dir, dataset_id)
    return path


@pytest.mark.p1
def test_integration_seed_fails_without_gold(integration_env, tmp_path) -> None:
    from quant_edai.quant.alpha_seed import run_seed

    with pytest.raises(RuntimeError, match="Gold features"):
        run_seed(source_dir=str(tmp_path / "empty"), sqlite_path=str(tmp_path / "s.db"))


@pytest.mark.p1
def test_smoke_seed_synthetic_is_simulated(smoke_env, tmp_path) -> None:
    from quant_edai.quant.alpha_seed import run_seed

    res = run_seed(source_dir=str(tmp_path / "empty"), sqlite_path=str(tmp_path / "s.db"))
    assert res["status"] == "PASSED_SIMULATED"
    assert res["execution_mode"] == "SIMULATED"
    assert res["daily_signals"]["synthetic_features"] is True


@pytest.mark.p1
def test_integration_seed_carries_provenance_from_gold(integration_env, tmp_path) -> None:
    from quant_edai.quant.alpha_seed import run_seed

    out_dir = tmp_path / "lakehouse"
    feat = _write_gold_features(out_dir, "ds_p1_seed")
    integration_env.setenv("MODEL_VERSION", "alpha_catalog-test")
    dbp = tmp_path / "seed.db"
    res = run_seed(source_dir=str(out_dir), sqlite_path=str(dbp))
    assert res["status"] == "SUCCESS" and res["execution_mode"] == "LIVE"
    assert res["dataset_id"] == "ds_p1_seed"
    assert res["feature_source"] == str(feat)

    db = PaperDatabase(sqlite_path=str(dbp))
    alphas = db.list_alphas()
    assert alphas and all(
        a["model_version"] == "alpha_catalog-test" and a["dataset_id"] == "ds_p1_seed" and a["feature_source"] == str(feat)
        for a in alphas
    )
    eff = res["daily_signals"]["effective_date"]
    sigs = db.get_daily_signals(eff)
    assert not sigs.empty
    assert (sigs["dataset_id"] == "ds_p1_seed").all() and (sigs["feature_source"] == str(feat)).all()
    assert (sigs["model_version"] == "alpha_catalog-test").all()


@pytest.mark.p1
def test_integration_alpha_metrics_and_backtester_fail_without_gold(integration_env, tmp_path) -> None:
    from quant_edai.quant.alpha_metrics_service import run_alpha_metrics
    from quant_edai.quant.backtester import QuantAlphaBacktester

    db = PaperDatabase(sqlite_path=str(tmp_path / "m.db"))
    with pytest.raises(RuntimeError, match="Gold features"):
        run_alpha_metrics(db=db, source_dir=str(tmp_path / "missing"))

    bt = QuantAlphaBacktester(
        features_path=str(tmp_path / "nope_features.parquet"),
        labels_path=str(tmp_path / "nope_labels.parquet"),
        model_path=str(tmp_path / "nope_model.joblib"),
        output_dir=str(tmp_path / "bt"),
    )
    with pytest.raises(RuntimeError, match="smoke-only"):
        bt.load_data()


@pytest.mark.p1
def test_smoke_backtester_synthetic_is_simulated(smoke_env, tmp_path) -> None:
    from quant_edai.quant.backtester import QuantAlphaBacktester

    bt = QuantAlphaBacktester(
        features_path=str(tmp_path / "nope_features.parquet"),
        labels_path=str(tmp_path / "nope_labels.parquet"),
        model_path=str(tmp_path / "nope_model.joblib"),
        output_dir=str(tmp_path / "bt"),
    )
    df = bt.load_data()
    assert not df.empty and bt.synthetic_data is True


@pytest.mark.p1
def test_integration_feature_calc_rejects_bronze(integration_env, tmp_path) -> None:
    from quant_edai.processing.features.feature_calculator import resolve_ohlcv_source

    (tmp_path / "raw_market_ohlcv").mkdir()
    with pytest.raises(RuntimeError, match="Bronze"):
        resolve_ohlcv_source(str(tmp_path))
