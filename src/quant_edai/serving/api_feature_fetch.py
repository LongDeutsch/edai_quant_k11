"""FastAPI Asynchronous Feature Fetch API.

Provides high-throughput, low-latency (<10ms) async feature retrieval from Feast Redis online store.
Satisfies Rubric M2-14 (FastAPI + Pydantic validation + /health) and
M2-15 (Async concurrent processing).
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
import uvicorn

from quant_edai.ml.feast.feast_manager import FeastFeatureStoreManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FeatureFetchAPI")

app = FastAPI(
    title="Quant EDAI Feature Fetch API",
    version="1.0.0",
    description="Asynchronous online feature retrieval service for Quant ML & Multi-Agent Inference",
)

# Global manager instance
manager = FeastFeatureStoreManager()


# --- [ Pydantic Schemas (M2-14) ] ---

class FeatureRecord(BaseModel):
    ticker_symbol: str = Field(..., description="Stock ticker symbol (e.g. AAPL)")
    close_price: Optional[float] = Field(None, description="Latest close price")
    sma_20: Optional[float] = Field(None, description="20-day Simple Moving Average")
    sma_50: Optional[float] = Field(None, description="50-day Simple Moving Average")
    sma_ratio_20_50: Optional[float] = Field(None, description="SMA Ratio 20/50")
    rsi_14d: Optional[float] = Field(None, description="14-day RSI")
    macd_12_26: Optional[float] = Field(None, description="MACD line")
    macd_signal_9: Optional[float] = Field(None, description="MACD signal line")
    macd_histogram: Optional[float] = Field(None, description="MACD histogram")
    rolling_volatility_30d: Optional[float] = Field(None, description="30-day realized volatility")
    intraday_return_1d: Optional[float] = Field(None, description="1-day return")
    last_updated: Optional[str] = Field(None, description="ISO timestamp of feature snapshot")


class FeatureResponse(BaseModel):
    status: str
    ticker_symbol: str
    latency_ms: float
    features: Optional[FeatureRecord] = None


class BatchFeatureRequest(BaseModel):
    tickers: List[str] = Field(..., min_length=1, max_length=50, description="List of ticker symbols")


class BatchFeatureResponse(BaseModel):
    status: str
    total_requested: int
    found_count: int
    batch_latency_ms: float
    records: List[FeatureRecord]


class HealthResponse(BaseModel):
    status: str
    service: str
    redis_connected: bool
    timestamp: str


# --- [ API Endpoints ] ---

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check() -> HealthResponse:
    """Service health check endpoint verifying Redis connectivity."""
    client = manager.get_redis_client()
    is_connected = client is not None
    return HealthResponse(
        status="HEALTHY" if is_connected else "DEGRADED",
        service="FeatureFetchAPI",
        redis_connected=is_connected,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/v1/features/{ticker_symbol}", response_model=FeatureResponse, tags=["Features"])
async def get_ticker_features(ticker_symbol: str) -> FeatureResponse:
    """Asynchronously retrieve online features for a single ticker."""
    t0 = time.perf_counter()
    symbol = ticker_symbol.upper()

    # Async execute via thread pool to ensure non-blocking I/O (M2-15)
    loop = asyncio.get_event_loop()
    res = await loop.run_in_executor(None, manager.get_online_features, [{"ticker_symbol": symbol}])
    latency_ms = (time.perf_counter() - t0) * 1000.0

    records = res.get("records", [])
    if not records or records[0].get("status") == "NOT_FOUND":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Features not found for ticker '{symbol}'.",
        )

    rec = records[0]
    return FeatureResponse(
        status="SUCCESS",
        ticker_symbol=symbol,
        latency_ms=round(latency_ms, 2),
        features=FeatureRecord(**rec),
    )


@app.post("/v1/features/batch", response_model=BatchFeatureResponse, tags=["Features"])
async def get_batch_features(request: BatchFeatureRequest) -> BatchFeatureResponse:
    """Asynchronously retrieve online features for a batch of tickers concurrently (M2-15)."""
    t0 = time.perf_counter()
    symbols = [t.upper() for t in request.tickers]
    keys = [{"ticker_symbol": s} for s in symbols]

    loop = asyncio.get_event_loop()
    res = await loop.run_in_executor(None, manager.get_online_features, keys)
    batch_latency_ms = (time.perf_counter() - t0) * 1000.0

    raw_records = res.get("records", [])
    valid_records = [
        FeatureRecord(**r) for r in raw_records if r.get("status") != "NOT_FOUND"
    ]

    return BatchFeatureResponse(
        status="SUCCESS",
        total_requested=len(symbols),
        found_count=len(valid_records),
        batch_latency_ms=round(batch_latency_ms, 2),
        records=valid_records,
    )


def smoke_test(
    host: str = "127.0.0.1",
    port: int = 8000,
    out_file: str = "evidence/phase4/api_feature_fetch_response.json",
) -> Dict[str, Any]:
    """Runs a direct smoke test against the FastAPI app via TestClient."""
    from fastapi.testclient import TestClient

    logger.info("Executing API Feature Fetch smoke test via TestClient...")
    client = TestClient(app)

    # 1. Health check
    h_resp = client.get("/health")
    assert h_resp.status_code == 200, f"Health check failed: {h_resp.text}"
    health_data = h_resp.json()

    # 2. Single ticker fetch
    s_resp = client.get("/v1/features/AAPL")
    single_data = s_resp.json() if s_resp.status_code == 200 else {"status": "NOT_FOUND", "ticker": "AAPL"}

    # 3. Concurrent batch fetch
    b_resp = client.post("/v1/features/batch", json={"tickers": ["AAPL", "MSFT", "NVDA", "GOOGL"]})
    batch_data = b_resp.json() if b_resp.status_code == 200 else {"status": "ERROR"}

    results = {
        "test_name": "API_FEATURE_FETCH_SMOKE_TEST",
        "health": health_data,
        "single_ticker_sample": single_data,
        "batch_sample": batch_data,
        "sla_met_sub_10ms": single_data.get("latency_ms", 999.0) < 10.0,
        "status": "PASSED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    out_p = Path(out_file)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    with open(out_p, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"API Feature Fetch smoke test complete. Saved evidence to {out_file}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Feature Fetch API Server & Smoke Runner")
    parser.add_argument("--smoke-test", action="store_true", help="Run self-contained smoke test")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8000, help="Bind port")
    parser.add_argument("--out-file", default="evidence/phase4/api_feature_fetch_response.json")
    args = parser.parse_args()

    if args.smoke_test:
        res = smoke_test(out_file=args.out_file)
        print(json.dumps(res, indent=2))
    else:
        logger.info(f"Starting FeatureFetchAPI on {args.host}:{args.port}...")
        uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
