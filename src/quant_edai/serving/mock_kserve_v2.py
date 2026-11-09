"""Mock KServe v2 Inference Service with 80/20 Canary Routing.

Adheres to KServe v2 Data Plane Protocol:
- GET  /v2/health/live
- GET  /v2/health/ready
- GET  /v2/models/{model_name}
- POST /v2/models/{model_name}/infer

Features 80/20 Canary Split:
- 80% traffic -> v1-champion (LightGBM)
- 20% traffic -> v2-challenger (Logistic Regression Baseline)
Logs every prediction request to evidence/phase4/kserve_canary_routing.log.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import random
import time
from typing import Any, Dict, List, Optional
import uuid

import joblib
import numpy as np
from pydantic import BaseModel, Field

try:
    from fastapi import FastAPI, HTTPException, Request, Response
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KServeV2Mock")

LOG_FILE_PATH = Path("evidence/phase4/kserve_canary_routing.log")


class KServeInput(BaseModel):
    name: str = "features"
    shape: List[int] = Field(default_factory=lambda: [1, 10])
    datatype: str = "FP64"
    data: List[Any]


class KServeInferenceRequest(BaseModel):
    id: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    inputs: List[KServeInput]


class KServeOutput(BaseModel):
    name: str
    datatype: str
    shape: List[int]
    data: List[Any]


class KServeInferenceResponse(BaseModel):
    id: str
    model_name: str
    model_version: str
    parameters: Optional[Dict[str, Any]] = None
    outputs: List[KServeOutput]


class ModelRegistry:
    """Loads and holds Champion and Challenger models in memory."""

    def __init__(
        self,
        champion_path: str = "models/champion_model.joblib",
        baseline_path: str = "models/baseline_model.joblib",
        scaler_path: str = "models/scaler.joblib",
    ) -> None:
        self.champion_model: Optional[Any] = None
        self.challenger_model: Optional[Any] = None
        self.scaler: Optional[Any] = None
        self.feature_names: List[str] = [
            "close_price", "sma_20", "sma_50", "sma_ratio_20_50", "rsi_14d",
            "macd_12_26", "macd_signal_9", "macd_histogram", "rolling_volatility_30d", "intraday_return_1d"
        ]
        self._load_models(champion_path, baseline_path, scaler_path)

    def _load_models(self, champion_path: str, baseline_path: str, scaler_path: str) -> None:
        if os.path.exists(champion_path):
            try:
                loaded = joblib.load(champion_path)
                self.champion_model = loaded.get("model", loaded) if isinstance(loaded, dict) else loaded
                if isinstance(loaded, dict) and "features" in loaded:
                    self.feature_names = loaded["features"]
                logger.info(f"Loaded Champion model from {champion_path}")
            except Exception as e:
                logger.warning(f"Could not load champion model: {e}")

        if os.path.exists(baseline_path):
            try:
                loaded = joblib.load(baseline_path)
                self.challenger_model = loaded.get("model", loaded) if isinstance(loaded, dict) else loaded
                logger.info(f"Loaded Challenger baseline model from {baseline_path}")
            except Exception as e:
                logger.warning(f"Could not load baseline model: {e}")

        if os.path.exists(scaler_path):
            try:
                self.scaler = joblib.load(scaler_path)
                logger.info(f"Loaded feature scaler from {scaler_path}")
            except Exception as e:
                logger.warning(f"Could not load scaler: {e}")

    def predict(self, model_version: str, features: np.ndarray) -> Tuple[List[int], List[float]]:
        """Run prediction on selected model version."""
        import pandas as pd
        df_feat = pd.DataFrame(features, columns=self.feature_names[:features.shape[1]])

        if model_version == "v1-champion" and self.champion_model is not None:
            model = self.champion_model
            # LightGBM handles DataFrame with column names
            if hasattr(model, "predict_proba"):
                probs = model.predict_proba(df_feat)[:, 1]
            else:
                probs = np.full(len(features), 0.5)
            preds = (probs >= 0.5).astype(int).tolist()
            return preds, [round(float(p), 4) for p in probs]

        elif model_version == "v2-challenger" and self.challenger_model is not None:
            model = self.challenger_model
            x = df_feat
            if self.scaler is not None:
                try:
                    x = self.scaler.transform(df_feat)
                except Exception:
                    pass
            if hasattr(model, "predict_proba"):
                probs = model.predict_proba(x)[:, 1]
            else:
                probs = np.full(len(features), 0.5)
            preds = (probs >= 0.5).astype(int).tolist()
            return preds, [round(float(p), 4) for p in probs]

        # Fallback simulation if model files not present
        probs = [round(random.uniform(0.4, 0.65), 4) for _ in range(len(features))]
        preds = [1 if p >= 0.5 else 0 for p in probs]
        return preds, probs


def create_kserve_app(
    champion_weight: float = 0.8,
    registry: Optional[ModelRegistry] = None,
) -> FastAPI:
    """Create FastAPI application implementing KServe v2 protocol."""
    app = FastAPI(
        title="Quant EDAI KServe v2 Mock Server",
        description="KServe v2 Data Plane inference server with 80/20 Canary traffic routing",
        version="2.0.0",
    )

    reg = registry or ModelRegistry()
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    @app.get("/v2/health/live")
    def health_live():
        return {"status": "alive"}

    @app.get("/v2/health/ready")
    def health_ready():
        return {"status": "ready"}

    @app.get("/v2/models/{model_name}")
    def get_model_metadata(model_name: str):
        return {
            "name": model_name,
            "versions": ["v1-champion", "v2-challenger"],
            "platform": "kserve_v2_mock",
            "inputs": [
                {"name": "features", "datatype": "FP64", "shape": [-1, 10]}
            ],
            "outputs": [
                {"name": "prediction", "datatype": "INT64", "shape": [-1]},
                {"name": "probability", "datatype": "FP64", "shape": [-1]},
            ],
            "canary_split": {
                "v1-champion": champion_weight,
                "v2-challenger": round(1.0 - champion_weight, 2),
            },
            "ready": True,
        }

    @app.post("/v2/models/{model_name}/infer", response_model=KServeInferenceResponse)
    def infer(model_name: str, request: KServeInferenceRequest):
        t0 = time.perf_counter()
        req_id = request.id or str(uuid.uuid4())

        if not request.inputs:
            raise HTTPException(status_code=400, detail="Missing 'inputs' in request payload")

        input_data = request.inputs[0].data
        # Reshape data into 2D numpy array
        raw_arr = np.array(input_data, dtype=np.float64)
        if raw_arr.ndim == 1:
            raw_arr = raw_arr.reshape(1, -1)

        # Canary Routing: 80% Champion, 20% Challenger
        u = random.random()
        selected_version = "v1-champion" if u < champion_weight else "v2-challenger"

        preds, probs = reg.predict(selected_version, raw_arr)
        latency_ms = round((time.perf_counter() - t0) * 1000, 3)

        # Log canary routing decision
        log_line = (
            f"{datetime.now(timezone.utc).isoformat()} [CANARY_ROUTER] "
            f"req_id={req_id} model={model_name} version={selected_version} "
            f"batch_size={len(preds)} latency_ms={latency_ms} pred={preds} prob={probs}\n"
        )
        with open(LOG_FILE_PATH, "a") as f:
            f.write(log_line)

        return KServeInferenceResponse(
            id=req_id,
            model_name=model_name,
            model_version=selected_version,
            outputs=[
                KServeOutput(
                    name="prediction",
                    datatype="INT64",
                    shape=[len(preds)],
                    data=preds,
                ),
                KServeOutput(
                    name="probability",
                    datatype="FP64",
                    shape=[len(probs)],
                    data=probs,
                ),
            ],
        )

    return app


def run_smoke_test(num_requests: int = 100) -> Dict[str, Any]:
    """Execute smoke test over KServe v2 mock using TestClient."""
    from fastapi.testclient import TestClient

    logger.info(f"Starting KServe v2 Canary Smoke Test with {num_requests} requests...")
    # Clear or initialize log file
    if LOG_FILE_PATH.exists():
        LOG_FILE_PATH.unlink()

    app = create_kserve_app(champion_weight=0.8)
    client = TestClient(app)

    # 1. Check live & ready
    live_resp = client.get("/v2/health/live")
    assert live_resp.status_code == 200, f"Live check failed: {live_resp.text}"

    ready_resp = client.get("/v2/health/ready")
    assert ready_resp.status_code == 200, f"Ready check failed: {ready_resp.text}"

    # 2. Check metadata
    meta_resp = client.get("/v2/models/quant-direction-predictor")
    assert meta_resp.status_code == 200, f"Meta check failed: {meta_resp.text}"
    meta = meta_resp.json()
    assert meta["canary_split"]["v1-champion"] == 0.8
    assert meta["canary_split"]["v2-challenger"] == 0.2

    # 3. Simulate requests
    version_counts = {"v1-champion": 0, "v2-challenger": 0}
    sample_features = [
        185.5,   # close_price
        182.1,   # sma_20
        179.3,   # sma_50
        1.015,   # sma_ratio_20_50
        58.4,    # rsi_14d
        1.25,    # macd_12_26
        1.10,    # macd_signal_9
        0.15,    # macd_histogram
        0.185,   # rolling_volatility_30d
        0.008,   # intraday_return_1d
    ]

    for i in range(num_requests):
        payload = {
            "id": f"req-{i+1:04d}",
            "inputs": [
                {
                    "name": "features",
                    "shape": [1, 10],
                    "datatype": "FP64",
                    "data": [sample_features],
                }
            ],
        }
        res = client.post("/v2/models/quant-direction-predictor/infer", json=payload)
        assert res.status_code == 200, f"Inference failed on req {i}: {res.text}"
        data = res.json()
        version = data["model_version"]
        version_counts[version] = version_counts.get(version, 0) + 1

    champ_ratio = version_counts["v1-champion"] / num_requests
    chall_ratio = version_counts["v2-challenger"] / num_requests

    logger.info(
        f"Smoke test complete: {num_requests} requests. "
        f"Champion={version_counts['v1-champion']} ({champ_ratio:.1%}), "
        f"Challenger={version_counts['v2-challenger']} ({chall_ratio:.1%})"
    )

    # Sanity check that both models received traffic and champion received majority
    assert version_counts["v1-champion"] > version_counts["v2-challenger"], "Champion must receive majority of canary traffic"
    assert version_counts["v2-challenger"] > 0, "Challenger must receive canary traffic"
    assert LOG_FILE_PATH.exists(), f"Log file {LOG_FILE_PATH} was not written"

    return {
        "total_requests": num_requests,
        "champion_count": version_counts["v1-champion"],
        "challenger_count": version_counts["v2-challenger"],
        "champion_ratio": round(champ_ratio, 3),
        "challenger_ratio": round(chall_ratio, 3),
        "log_path": str(LOG_FILE_PATH),
        "status": "PASSED",
    }


def main():
    parser = argparse.ArgumentParser(description="KServe v2 Mock Inference Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--smoke-test", action="store_true", help="Run 100-request canary smoke test")
    parser.add_argument("--requests", type=int, default=100)
    args = parser.parse_args()

    if args.smoke_test:
        result = run_smoke_test(num_requests=args.requests)
        print(json.dumps(result, indent=2))
        return

    import uvicorn
    app = create_kserve_app()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
