"""P1.4: trader pipeline — provenance on fills, ET EOD flatten, Kafka env, integration gates."""

from __future__ import annotations

import pytest

from quant_edai.common.kafka_env import resolve_kafka_bootstrap
from quant_edai.common.market_time import is_eod_flat_time, parse_bar_timestamp
from quant_edai.common.ml_provenance import PROVENANCE_FIELDS, ProvenanceError
from quant_edai.quant.market_replay import MarketReplayRunner, kafka_message_to_bar
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine

PROV = {
    "model_version": "lgbm-20250101T000000Z-ds_trader",
    "dataset_id": "ds_trader",
    "feature_source": "s3a://lakehouse/gold/runs/ds_trader/gold_features/feat.parquet",
}


@pytest.fixture
def db():
    d = PaperDatabase(sqlite_path=":memory:")
    d.init_db(reset=True)
    return d


def _engine(db, **kw):
    return PaperTradingEngine(db=db, alpha_source="inline", sl_pct=None, **kw)


@pytest.mark.p1
def test_fill_missing_provenance_fails_in_integration(integration_env, db) -> None:
    engine = _engine(db)
    cash_before = engine.cash
    with pytest.raises(ProvenanceError, match="fill|order"):
        engine._open_position("AAPL", 180.0, "2024-03-15T10:00:00-04:00", side="LONG")
    # Nothing persisted, no position, no cash movement
    assert engine.cash == cash_before and not engine.positions
    assert db.get_recent_trades() == []


@pytest.mark.p1
def test_fill_with_provenance_persisted_on_order_and_fill(integration_env, db) -> None:
    engine = _engine(db, **PROV)
    engine._open_position("AAPL", 180.0, "2024-03-15T10:00:00-04:00", side="LONG")
    trades = db.get_trades_with_orders()
    assert len(trades) == 1
    for f in PROVENANCE_FIELDS:
        assert trades.iloc[0][f] == PROV[f]
    order = db._sqlite_conn.execute("SELECT model_version, dataset_id, feature_source FROM paper_orders").fetchone()
    assert dict(order) == PROV


@pytest.mark.p1
def test_signal_provenance_overrides_engine_defaults(integration_env, db) -> None:
    engine = _engine(db, **PROV)
    engine._signal_provenance["AAPL"] = {**PROV, "model_version": "sig-model-v9"}
    engine._open_position("AAPL", 180.0, "2024-03-15T10:00:00-04:00", side="LONG")
    assert db.get_trades_with_orders().iloc[0]["model_version"] == "sig-model-v9"


@pytest.mark.p1
def test_smoke_fill_is_stamped_simulated(smoke_env, db) -> None:
    engine = _engine(db)
    engine._open_position("AAPL", 180.0, "2024-03-15T10:00:00-04:00", side="LONG")
    row = db.get_trades_with_orders().iloc[0]
    assert {row[f] for f in PROVENANCE_FIELDS} == {"SIMULATED"}


@pytest.mark.p1
def test_pm_force_flatten_inherits_fill_provenance(integration_env, db) -> None:
    engine = _engine(db, **PROV)
    engine._open_position("AAPL", 180.0, "2024-03-15T10:00:00-04:00", side="LONG")
    engine.pm_controls.force_flatten_all_positions(pm_user="test", reason="p1")
    trades = db.get_trades_with_orders()
    assert len(trades) == 2 and (trades["dataset_id"] == PROV["dataset_id"]).all()


@pytest.mark.p1
def test_eod_timezone_is_america_new_york(smoke_env) -> None:
    # Integration policy = UTC for naive/Z; explicit offsets always honoured
    utc = lambda s: parse_bar_timestamp(s, policy="utc")  # noqa: E731
    assert is_eod_flat_time(utc("2024-03-15T19:55:00Z"))  # 15:55 EDT
    assert not is_eod_flat_time(utc("2024-03-15T15:55:00Z"))  # 11:55 EDT
    assert is_eod_flat_time(utc("2024-03-15T15:55:00-04:00"))
    assert utc("2024-01-16T20:55:00Z").hour == 15  # EST (UTC-5) in winter
    # Legacy smoke policy: "Z" is exchange wall-clock
    assert is_eod_flat_time(parse_bar_timestamp("2024-03-15T15:55:00Z", policy="et"))
    assert parse_bar_timestamp("not-a-time") is None


@pytest.mark.p1
def test_engine_eod_flat_uses_et_in_integration(integration_env, db) -> None:
    engine = _engine(db, **PROV)
    engine._open_position("GOOGL", 150.0, "2024-03-15T10:00:00-04:00", side="LONG")
    assert "GOOGL" in engine.positions
    # 15:55Z is 11:55 ET -> must NOT flatten under the integration (UTC) policy
    res = engine.on_bar(MarketBar("GOOGL", "2024-03-15T15:55:00Z", 150.0, 150.1, 149.9, 150.0, 1000.0, 150.0))
    assert not any(e.startswith("EOD_FLAT") for e in res["events"])
    # 19:55Z == 15:55 EDT -> flatten
    res = engine.on_bar(MarketBar("GOOGL", "2024-03-15T19:55:00Z", 150.0, 150.1, 149.9, 150.0, 1000.0, 150.0))
    assert any(e == "EOD_FLAT_GOOGL" for e in res["events"])


@pytest.mark.p1
def test_kafka_bootstrap_reads_both_env_vars(smoke_env) -> None:
    assert resolve_kafka_bootstrap() == "localhost:9092"
    smoke_env.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    assert resolve_kafka_bootstrap() == "kafka:29092"
    smoke_env.setenv("KAFKA_BOOTSTRAP", "broker:9092")
    assert resolve_kafka_bootstrap() == "broker:9092"
    assert resolve_kafka_bootstrap("explicit:1") == "explicit:1"


@pytest.mark.p1
def test_integration_runner_rejects_synthetic_gates(integration_env, db) -> None:
    with pytest.raises(RuntimeError, match="--mode live"):
        MarketReplayRunner(mode="replay", db=db, alpha_source="store")
    with pytest.raises(RuntimeError, match="alpha-source store"):
        MarketReplayRunner(mode="live", db=db, alpha_source="inline")
    runner = MarketReplayRunner(mode="live", db=db, alpha_source="store", **PROV)
    assert runner.engine.provenance["dataset_id"] == PROV["dataset_id"]


@pytest.mark.p1
def test_integration_feeder_and_bronze_publisher_refuse(integration_env) -> None:
    from quant_edai.quant.bronze_kafka_publisher import run_publish
    from quant_edai.quant.continuous_bar_feeder import ContinuousBarFeeder

    with pytest.raises(RuntimeError, match="smoke only"):
        ContinuousBarFeeder()
    with pytest.raises(RuntimeError, match="smoke-only"):
        run_publish()


@pytest.mark.p1
def test_integration_serving_has_no_default_composite_gate(integration_env, db) -> None:
    from quant_edai.quant.alpha_serving import AlphaSignalServer

    sig = AlphaSignalServer(db=db).get_intraday_signal("AAPL", 180.0, "2024-03-15T10:00:00-04:00")
    assert sig.action == "FLAT" and "REJECTED" in sig.notes


@pytest.mark.p1
def test_kafka_message_maps_flink_and_provenance_fields() -> None:
    bar = kafka_message_to_bar(
        {
            "ticker_symbol": "AAPL",
            "window_start": "2024-03-15 14:30:00.000",  # PyFlink sink shape
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10, "vwap": 1.4,
            "dataset_id": "ds_x", "source_layer": "bronze",
        }
    )
    assert bar.timestamp.startswith("2024-03-15 14:30") and bar.close_price == 1.5
    assert bar.dataset_id == "ds_x" and bar.source_layer == "bronze"
