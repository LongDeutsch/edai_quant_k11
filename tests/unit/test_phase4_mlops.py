"""Unit tests for Phase 4: Feast, MLflow, Model Training, KServe v2 Mock, and Drift Detection."""

from datetime import datetime, timezone
import json
from pathlib import Path
import time
import numpy as np
import pandas as pd
import pytest

from quant_edai.ml.feast.entities import ticker_symbol
from quant_edai.ml.feast.feature_views import fv_ticker_technical_indicators
from quant_edai.ml.feast.feast_manager import FeastFeatureStoreManager
from quant_edai.ml.trainer import QuantModelTrainer
from quant_edai.ml.kfp.pipeline import run_kfp_pipeline
from quant_edai.serving.api_feature_fetch import app as feature_fetch_app
from quant_edai.serving.mock_kserve_v2 import create_kserve_app, ModelRegistry
from quant_edai.ml.drift_api import calculate_psi, calculate_ks, FeatureDriftMonitor


def test_feast_entity_and_feature_view_definitions():
    """Verify Feast entity and feature view definitions (M2-09)."""
    assert ticker_symbol.name == "ticker_symbol"
    assert ticker_symbol.join_key == "ticker_symbol"

    assert fv_ticker_technical_indicators.name == "fv_ticker_technical_indicators"
    assert len(fv_ticker_technical_indicators.entities) == 1
    assert fv_ticker_technical_indicators.entities[0] == "ticker_symbol"

    field_names = [f.name for f in fv_ticker_technical_indicators.schema]
    required_features = [
        "close_price",
        "sma_20",
        "sma_50",
        "sma_ratio_20_50",
        "rsi_14d",
        "macd_12_26",
        "macd_signal_9",
        "macd_histogram",
        "rolling_volatility_30d",
        "intraday_return_1d",
    ]
    for rf in required_features:
        assert rf in field_names, f"Missing field in FeatureView: {rf}"

    # Verify event_timestamp and created audit fields on source (M1-34)
    assert fv_ticker_technical_indicators.source.timestamp_field == "event_timestamp"
    assert fv_ticker_technical_indicators.source.created_timestamp_column == "created"


def test_feast_manager_materialize_and_latency(tmp_path: Path):
    """Test Feast materialization and sub-10ms retrieval latency (M2-09, M2-10)."""
    # Create synthetic parquet
    df = pd.DataFrame({
        "ticker_symbol": ["AAPL", "GOOGL"],
        "event_timestamp": [pd.Timestamp("2026-03-01 10:00:00"), pd.Timestamp("2026-03-01 10:00:00")],
        "close_price": [180.5, 140.2],
        "sma_20": [178.0, 138.0],
        "sma_50": [175.0, 135.0],
        "sma_ratio_20_50": [1.017, 1.022],
        "rsi_14d": [55.4, 60.1],
        "macd_12_26": [1.2, 0.8],
        "macd_signal_9": [1.0, 0.7],
        "macd_histogram": [0.2, 0.1],
        "rolling_volatility_30d": [0.18, 0.22],
        "intraday_return_1d": [0.005, -0.002],
        "created": [pd.Timestamp.now(timezone.utc), pd.Timestamp.now(timezone.utc)],
    })
    p_file = tmp_path / "features.parquet"
    df.to_parquet(p_file)

    manager = FeastFeatureStoreManager(source_parquet=str(p_file))
    out_evidence = tmp_path / "metrics.json"
    stats = manager.materialize(out_file=str(out_evidence))

    assert stats["status"] == "SUCCESS"
    assert stats["materialized_tickers_count"] == 2
    assert out_evidence.exists()

    # Query online features
    t0 = time.perf_counter()
    res = manager.get_online_features("AAPL")
    latency_ms = (time.perf_counter() - t0) * 1000

    assert res["entity_count"] == 1
    features = res["records"][0]
    assert features["ticker_symbol"] == "AAPL"
    assert features["close_price"] == 180.5
    assert latency_ms < 10.0, f"Feast online retrieval latency {latency_ms:.2f}ms exceeds 10ms SLA!"


def test_walk_forward_split_no_lookahead_bias():
    """Verify chronological walk-forward split (80/10/10) eliminates look-ahead bias (M2-11)."""
    dates = pd.date_range("2026-01-01", periods=100, freq="D")
    records = []
    for d in dates:
        for ticker in ["AAPL", "MSFT"]:
            records.append({
                "ticker_symbol": ticker,
                "join_date": d.strftime("%Y-%m-%d"),
                "event_timestamp": d,
                "close_price": 100.0,
                "sma_20": 100.0,
                "sma_50": 100.0,
                "sma_ratio_20_50": 1.0,
                "rsi_14d": 50.0,
                "macd_12_26": 0.0,
                "macd_signal_9": 0.0,
                "macd_histogram": 0.0,
                "rolling_volatility_30d": 0.2,
                "intraday_return_1d": 0.01,
                "return_sign": 1,
            })
    df = pd.DataFrame(records)

    trainer = QuantModelTrainer()
    df_train, df_val, df_test = trainer.temporal_walk_forward_split(df)

    # 100 dates -> 80 train, 10 val, 10 test
    assert len(df_train) == 80 * 2
    assert len(df_val) == 10 * 2
    assert len(df_test) == 10 * 2

    # Check strict temporal non-overlap
    max_train_date = df_train["join_date"].max()
    min_val_date = df_val["join_date"].min()
    max_val_date = df_val["join_date"].max()
    min_test_date = df_test["join_date"].min()

    assert max_train_date < min_val_date, "Look-ahead bias detected: train date >= val date!"
    assert max_val_date < min_test_date, "Look-ahead bias detected: val date >= test date!"


def test_quant_trainer_and_mlflow_run(tmp_path: Path):
    """Verify baseline LR and LightGBM champion training, evaluation and artifact generation (M2-11, M2-12)."""
    dates = pd.date_range("2026-01-01", periods=50, freq="D")
    np.random.seed(42)
    n = len(dates)
    df = pd.DataFrame({
        "join_date": [d.strftime("%Y-%m-%d") for d in dates],
        "close_price": np.random.uniform(100, 200, n),
        "sma_20": np.random.uniform(100, 200, n),
        "sma_50": np.random.uniform(100, 200, n),
        "sma_ratio_20_50": np.random.uniform(0.9, 1.1, n),
        "rsi_14d": np.random.uniform(30, 70, n),
        "macd_12_26": np.random.uniform(-2, 2, n),
        "macd_signal_9": np.random.uniform(-2, 2, n),
        "macd_histogram": np.random.uniform(-1, 1, n),
        "rolling_volatility_30d": np.random.uniform(0.1, 0.4, n),
        "intraday_return_1d": np.random.uniform(-0.03, 0.03, n),
        "direction_label": np.random.choice([0, 1], size=n),
    })

    trainer = QuantModelTrainer(models_dir=str(tmp_path / "models"))
    df_train = df.iloc[:35]
    df_val = df.iloc[35:42]
    df_test = df.iloc[42:]

    results = trainer.train_models(df_train, df_val, df_test)

    assert "baseline" in results
    assert "champion" in results
    assert "auc_roc" in results["baseline"]
    assert "auc_roc" in results["champion"]
    assert "accuracy" in results["champion"]
    assert "mlflow" in results

    assert Path(results["artifacts"]["baseline_path"]).exists()
    assert Path(results["artifacts"]["champion_path"]).exists()


def test_kserve_v2_canary_routing():
    """Verify KServe v2 mock server 80/20 canary routing and endpoints (M2-13)."""
    from fastapi.testclient import TestClient

    app = create_kserve_app(champion_weight=0.8)
    client = TestClient(app)

    # Health
    assert client.get("/v2/health/live").status_code == 200
    assert client.get("/v2/health/ready").status_code == 200

    # Model metadata
    meta = client.get("/v2/models/quant-test-model").json()
    assert meta["canary_split"]["v1-champion"] == 0.8
    assert meta["canary_split"]["v2-challenger"] == 0.2

    # Inference with canary routing
    versions = set()
    sample_feat = [100.0, 99.0, 98.0, 1.01, 55.0, 0.5, 0.4, 0.1, 0.2, 0.01]
    for i in range(50):
        resp = client.post(
            "/v2/models/quant-test-model/infer",
            json={
                "id": f"test-{i}",
                "inputs": [{"name": "features", "shape": [1, 10], "datatype": "FP64", "data": [sample_feat]}],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["outputs"]) == 2
        versions.add(data["model_version"])

    # Over 50 requests, both canary branches should receive traffic
    assert "v1-champion" in versions
    assert "v2-challenger" in versions


def test_feature_fetch_api_endpoints():
    """Verify FastAPI Feature Fetch API endpoints and latency (M2-14)."""
    from fastapi.testclient import TestClient

    client = TestClient(feature_fetch_app)

    # Health
    assert client.get("/health").status_code == 200

    # Single feature fetch
    resp = client.get("/v1/features/AAPL")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ticker_symbol"] == "AAPL"
    assert data["latency_ms"] < 10.0
    assert "close_price" in data["features"]

    # Batch feature fetch
    batch_resp = client.post("/v1/features/batch", json={"tickers": ["AAPL", "MSFT", "NVDA"]})
    assert batch_resp.status_code == 200
    batch_data = batch_resp.json()
    assert batch_data["total_requested"] == 3
    assert batch_data["found_count"] == 3
    assert len(batch_data["records"]) == 3


def test_statistical_drift_detection_ks_and_psi():
    """Verify KS-test and PSI calculations and alert triggering on distribution shift (M2-14, M2-15)."""
    np.random.seed(42)
    ref = np.random.normal(loc=100.0, scale=10.0, size=1000)

    # 1. Identical distribution: PSI should be ~0, KS p-value > 0.05
    same = np.random.normal(loc=100.0, scale=10.0, size=500)
    psi_stable = calculate_psi(ref, same)
    _, p_val_stable = calculate_ks(ref, same)
    assert psi_stable < 0.10, f"Expected stable PSI < 0.1, got {psi_stable}"
    assert p_val_stable > 0.05, f"Expected KS p-value > 0.05 for identical dist, got {p_val_stable}"

    # 2. Shifted distribution: PSI should exceed 0.25, KS p-value < 0.01
    shifted = np.random.normal(loc=125.0, scale=15.0, size=500)
    psi_drift = calculate_psi(ref, shifted)
    _, p_val_drift = calculate_ks(ref, shifted)
    assert psi_drift >= 0.25, f"Expected drift PSI >= 0.25, got {psi_drift}"
    assert p_val_drift < 0.01, f"Expected KS p-value < 0.01 for shifted dist, got {p_val_drift}"

    # 3. Drift monitor evaluation
    df_ref = pd.DataFrame({
        "close_price": ref,
        "rsi_14d": np.random.uniform(40, 60, size=1000),
    })
    df_curr = pd.DataFrame({
        "close_price": shifted,
        "rsi_14d": np.random.uniform(80, 100, size=500),  # Heavily drifted
    })

    monitor = FeatureDriftMonitor()
    monitor.df_ref = df_ref
    report = monitor.evaluate_drift(df_curr, features=["close_price", "rsi_14d"])

    assert report["overall_status"] == "ALERT_TRIGGERED"
    assert report["retrain_recommended"] is True
    assert report["features"]["close_price"]["status"] == "SIGNIFICANT_DRIFT"
