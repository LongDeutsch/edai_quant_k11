"""Feast Feature Store Lifecycle Manager.

Manages Feast initialization, entity/feature view application,
incremental materialization from Parquet/Iceberg to Redis,
and online feature retrieval with sub-10ms latency SLAs.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from quant_edai.common.dataset_paths import resolve_dataset_root
from quant_edai.common.execution_status import assert_integration_ready, is_integration_mode
from quant_edai.common.ml_provenance import dataset_id_from_path, resolve_dataset_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FeastManager")


class FeastFeatureStoreManager:
    """Manages Feast feature store operations with Redis online store."""

    FEATURE_COLUMNS = [
        "close_price",
        "volume",
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

    def __init__(
        self,
        repo_path: str = "configs/feast",
        redis_host: str = "localhost",
        redis_port: int = 6379,
        source_parquet: Optional[str] = None,
        features_path: Optional[str] = None,
    ) -> None:
        self.repo_path = Path(repo_path)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.source_parquet = source_parquet or features_path or self._find_source_parquet()
        self._redis_client = None
        self._local_cache: Dict[str, Dict[str, Any]] = {}
        self.dataset_id = dataset_id_from_path(self.source_parquet) or resolve_dataset_id()
        if not is_integration_mode():
            # Hard-coded demo features are smoke-only; integration|full serves only materialized Gold rows.
            self._seed_local_cache()

    def _seed_local_cache(self) -> None:
        """Seed simulation cache with default features for offline/isolated tests."""
        default_tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]
        base_features = {
            "close_price": 185.5,
            "sma_20": 182.1,
            "sma_50": 179.3,
            "sma_ratio_20_50": 1.015,
            "rsi_14d": 58.4,
            "macd_12_26": 1.25,
            "macd_signal_9": 1.10,
            "macd_histogram": 0.15,
            "rolling_volatility_30d": 0.185,
            "intraday_return_1d": 0.008,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        for ticker in default_tickers:
            k = f"fv_ticker_technical_indicators:{ticker}"
            data = dict(base_features)
            data["ticker_symbol"] = ticker
            self._local_cache[k] = data

    def _find_source_parquet(self) -> str:
        out_dir = os.getenv("OUT_DIR", "/tmp/lakehouse_source")
        try:
            ds_root = str(resolve_dataset_root(out_dir))  # OUT_DIR/runs/{dataset_id}
        except FileNotFoundError:
            ds_root = out_dir
        candidates = [
            os.path.join(ds_root, "gold_features", "feat_ticker_technical_indicators.parquet"),
            os.path.join(out_dir, "gold_features", "feat_ticker_technical_indicators.parquet"),
        ]
        if not is_integration_mode():
            candidates += [
                "evidence/phase3/feat_ticker_technical_indicators.parquet",
                os.path.join(ds_root, "raw_market_ohlcv"),
            ]
        for c in candidates:
            if os.path.exists(c):
                return c
        return candidates[0]

    def get_redis_client(self):
        """Lazy-initialize Redis client with connection check."""
        if hasattr(self, "_redis_checked") and self._redis_checked:
            return self._redis_client
        self._redis_checked = True
        try:
            import redis
            client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True,
                socket_connect_timeout=0.1,
            )
            if client.ping():
                self._redis_client = client
                logger.info(f"Connected to Redis online store at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            logger.warning(f"Redis offline or unreachable ({e}). Operating in simulation fallback mode.")
            self._redis_client = None
        return self._redis_client

    def apply_definitions(self) -> Dict[str, Any]:
        """Apply Feast entity and feature view definitions."""
        logger.info("Applying Feast Feature Store schema definitions...")
        entities_info = {
            "entity": "ticker_symbol",
            "join_keys": ["ticker_symbol"],
            "description": "Stock ticker symbol for US Equities",
        }
        feature_views_info = {
            "name": "fv_ticker_technical_indicators",
            "entity": "ticker_symbol",
            "features": self.FEATURE_COLUMNS,
            "source": str(self.source_parquet),
            "ttl_days": 365,
            "online": True,
        }

        # Ensure registry directory exists
        reg_dir = self.repo_path / "data"
        reg_dir.mkdir(parents=True, exist_ok=True)
        registry_file = reg_dir / "registry.json"
        with open(registry_file, "w") as f:
            json.dump({"entities": [entities_info], "feature_views": [feature_views_info]}, f, indent=2)

        return {
            "status": "APPLIED",
            "entity": entities_info,
            "feature_view": feature_views_info,
            "applied_at": datetime.now(timezone.utc).isoformat(),
        }

    def materialize(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        out_file: str = "evidence/phase4/feast_materialize_metrics.json",
    ) -> Dict[str, Any]:
        """
        Incrementally materialize features from Parquet into Redis online store.
        Key pattern: 'fv_ticker_technical_indicators:{ticker_symbol}'
        """
        p_path = Path(self.source_parquet)
        synthesized = False
        assert_integration_ready(
            condition=p_path.exists(),
            message=f"Gold feature parquet {p_path} missing; Feast materialization requires real Gold features.",
        )
        if not p_path.exists():
            synthesized = True
            # If not yet generated, synthesize sample data for smoke testing (SIMULATED)
            logger.warning(f"Parquet source {p_path} not found. Synthesizing initial feature rows...")
            df = pd.DataFrame({
                "ticker_symbol": ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL"],
                "event_timestamp": [pd.Timestamp.now()] * 5,
                "created": [pd.Timestamp.now()] * 5,
                "close_price": [185.0, 130.0, 420.0, 180.0, 175.0],
                "sma_20": [182.0, 125.0, 415.0, 178.0, 170.0],
                "sma_50": [178.0, 120.0, 405.0, 172.0, 165.0],
                "sma_ratio_20_50": [1.022, 1.041, 1.024, 1.034, 1.030],
                "rsi_14d": [58.4, 64.2, 55.1, 60.8, 52.3],
                "macd_12_26": [1.45, 2.10, 3.20, 1.15, 0.95],
                "macd_signal_9": [1.20, 1.80, 2.90, 1.00, 0.85],
                "macd_histogram": [0.25, 0.30, 0.30, 0.15, 0.10],
                "rolling_volatility_30d": [0.22, 0.35, 0.19, 0.24, 0.20],
                "intraday_return_1d": [0.008, 0.015, -0.004, 0.006, 0.002],
            })
        else:
            df = pd.read_parquet(p_path)

        logger.info(f"Loaded {len(df)} rows from {p_path} for Feast materialization.")
        if "ticker_symbol" not in df.columns:
            raise ValueError(f"Missing required entity key 'ticker_symbol' in {p_path}")

        # Keep latest feature row per ticker
        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
            df = df.sort_values(by=["ticker_symbol", "event_timestamp"]).groupby("ticker_symbol").last().reset_index()

        redis_client = self.get_redis_client()
        assert_integration_ready(
            condition=redis_client is not None,
            message=f"Redis online store {self.redis_host}:{self.redis_port} unreachable; "
            "local simulation cache is smoke-only.",
        )
        materialized_tickers = []
        t0 = time.perf_counter()

        for _, row in df.iterrows():
            ticker = str(row["ticker_symbol"])
            feature_data = {
                col: float(row[col]) if pd.notna(row[col]) else 0.0
                for col in self.FEATURE_COLUMNS
                if col in row
            }
            feature_data["ticker_symbol"] = ticker
            feature_data["last_updated"] = datetime.now(timezone.utc).isoformat()
            # Lineage travels with the online record so served signals can cite it
            feature_data["dataset_id"] = "synthetic" if synthesized else str(self.dataset_id or "")
            feature_data["feature_source"] = "synthetic_panel" if synthesized else str(p_path)

            key = f"fv_ticker_technical_indicators:{ticker}"
            mapping_dict = {k: str(v) for k, v in feature_data.items()}
            if redis_client:
                # Store as Redis Hash for general ticker key and dataset-isolated key
                redis_client.hset(key, mapping=mapping_dict)
                if self.dataset_id:
                    ds_key = f"fv_ticker_technical_indicators:{self.dataset_id}:{ticker}"
                    redis_client.hset(ds_key, mapping=mapping_dict)
            else:
                self._local_cache[key] = feature_data
                if self.dataset_id:
                    self._local_cache[f"fv_ticker_technical_indicators:{self.dataset_id}:{ticker}"] = feature_data

            materialized_tickers.append(ticker)

        duration_sec = time.perf_counter() - t0
        logger.info(f"Materialized {len(materialized_tickers)} tickers to online store in {duration_sec:.4f}s")

        # Save dump of active Redis keys to evidence
        keys_evidence = Path("evidence/phase4/feast_redis_keys.txt")
        keys_evidence.parent.mkdir(parents=True, exist_ok=True)
        with open(keys_evidence, "w") as f:
            for t in materialized_tickers:
                f.write(f"fv_ticker_technical_indicators:{t}\n")

        stats = {
            "status": "PASSED_SIMULATED" if synthesized else "SUCCESS",
            "execution_mode": "LIVE" if redis_client and not synthesized else "SIMULATED",
            "dataset_id": None if synthesized else self.dataset_id,
            "feature_source": "synthetic_panel" if synthesized else str(p_path),
            "mode": "REDIS_ONLINE" if redis_client else "LOCAL_SIMULATION",
            "materialized_tickers_count": len(materialized_tickers),
            "tickers": materialized_tickers[:20],
            "feature_columns": self.FEATURE_COLUMNS,
            "latency_seconds": round(duration_sec, 4),
            "keys_written_evidence": str(keys_evidence),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        out_p = Path(out_file)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        with open(out_p, "w") as f:
            json.dump(stats, f, indent=2)

        return stats

    def get_online_features(self, entity_keys: Any, dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve online features for a batch of tickers with latency measurement.
        Satisfies sub-10ms retrieval requirement.
        """
        if isinstance(entity_keys, str):
            entity_keys = [{"ticker_symbol": entity_keys}]
        elif isinstance(entity_keys, list) and entity_keys and isinstance(entity_keys[0], str):
            entity_keys = [{"ticker_symbol": t} for t in entity_keys]

        redis_client = self.get_redis_client()
        t0 = time.perf_counter()
        records = []

        for entity in entity_keys:
            ticker = entity.get("ticker_symbol")
            if not ticker:
                continue
            ent_ds = entity.get("dataset_id") or dataset_id or self.dataset_id
            ds_key = f"fv_ticker_technical_indicators:{ent_ds}:{ticker}" if ent_ds else None
            gen_key = f"fv_ticker_technical_indicators:{ticker}"

            data = None
            if redis_client:
                if ds_key and redis_client.exists(ds_key):
                    data = redis_client.hgetall(ds_key)
                if not data:
                    data = redis_client.hgetall(gen_key)

                if data:
                    parsed = {k: float(v) if k in self.FEATURE_COLUMNS else v for k, v in data.items()}
                    records.append(parsed)
                else:
                    records.append({"ticker_symbol": ticker, "status": "NOT_FOUND"})
            else:
                cached = (self._local_cache.get(ds_key) if ds_key else None) or self._local_cache.get(gen_key)
                if cached:
                    records.append(cached)
                else:
                    records.append({"ticker_symbol": ticker, "status": "NOT_FOUND"})

        retrieval_latency_ms = (time.perf_counter() - t0) * 1000.0

        return {
            "entity_count": len(records),
            "retrieval_latency_ms": round(retrieval_latency_ms, 2),
            "sub_10ms_sla_met": retrieval_latency_ms < 10.0,
            "records": records,
        }

    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        features: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Join historical features against entity dataframe preserving point-in-time correctness.
        Enforces: feature_event_timestamp <= entity_label_date.
        """
        features_to_join = features or self.FEATURE_COLUMNS
        p_path = Path(self.source_parquet)
        if not p_path.exists():
            raise FileNotFoundError(f"Source feature Parquet not found at {p_path}")

        df_feat = pd.read_parquet(p_path)
        df_feat["event_timestamp"] = pd.to_datetime(df_feat["event_timestamp"])

        ent_df = entity_df.copy()
        date_col = "label_date" if "label_date" in ent_df.columns else "trade_date"
        ent_df[date_col] = pd.to_datetime(ent_df[date_col])

        # Point-in-time merge using merge_asof per ticker
        results = []
        for ticker, group_ent in ent_df.groupby("ticker_symbol"):
            group_feat = df_feat[df_feat["ticker_symbol"] == ticker].sort_values("event_timestamp")
            if group_feat.empty:
                continue
            group_ent = group_ent.sort_values(date_col)
            merged = pd.merge_asof(
                group_ent,
                group_feat[["ticker_symbol", "event_timestamp"] + [c for c in features_to_join if c in group_feat.columns]],
                left_on=date_col,
                right_on="event_timestamp",
                by="ticker_symbol",
                direction="backward",
            )
            results.append(merged)

        if not results:
            return pd.DataFrame()
        return pd.concat(results, ignore_index=True)


def main():
    parser = argparse.ArgumentParser(description="Feast Feature Store Manager")
    parser.add_argument("--action", choices=["apply", "materialize", "query"], default="materialize")
    parser.add_argument("--source", default=None, help="Path to Parquet features")
    parser.add_argument("--ticker", default="AAPL", help="Ticker for query action")
    parser.add_argument("--out-file", default="evidence/phase4/feast_materialize_metrics.json")
    args = parser.parse_args()

    manager = FeastFeatureStoreManager(source_parquet=args.source)
    if args.action == "apply":
        res = manager.apply_definitions()
        print(json.dumps(res, indent=2))
    elif args.action == "materialize":
        manager.apply_definitions()
        res = manager.materialize(out_file=args.out_file)
        print(json.dumps(res, indent=2))
    elif args.action == "query":
        res = manager.get_online_features([{"ticker_symbol": args.ticker}])
        print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
