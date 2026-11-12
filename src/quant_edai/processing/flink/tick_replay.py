"""
Tick Replay Utility for Gate 3 Real Flink Streaming Integration.

Replays raw ticks from Bronze Parquet directory to Kafka topic `market.ticks`
with strictly deterministic ordering (event_timestamp ASC, trade_id ASC).
Provides watermark flush markers to finalize bounded replay windows.
"""

from __future__ import annotations

import glob
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TickReplay")


def replay_ticks_from_bronze(
    dataset_id: str,
    bronze_ticks_dir: str | Path,
    kafka_bootstrap: str = "localhost:9092",
    topic: str = "market.ticks",
    inject_malformed: bool = False,
    add_watermark_flush: bool = True,
    flush_delay_minutes: int = 2,
) -> Dict[str, Any]:
    """
    Replays raw Bronze market ticks to Kafka for a specific dataset_id.
    Ensures strict deterministic ordering by (event_timestamp ASC, trade_id ASC).
    """
    p = Path(bronze_ticks_dir)
    if not p.exists():
        raise FileNotFoundError(f"Bronze ticks directory not found: {bronze_ticks_dir}")

    files = sorted(glob.glob(str(p / "**" / "*.parquet"), recursive=True))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {bronze_ticks_dir}")

    frames = [pd.read_parquet(f) for f in files]
    df_raw = pd.concat(frames, ignore_index=True)

    if "dataset_id" in df_raw.columns:
        df_target = df_raw[df_raw["dataset_id"] == dataset_id].copy()
    else:
        df_target = df_raw.copy()
        df_target["dataset_id"] = dataset_id

    if df_target.empty:
        raise ValueError(f"No ticks found for dataset_id='{dataset_id}' in {bronze_ticks_dir}")

    # Deterministic sort
    df_target = df_target.sort_values(by=["event_timestamp", "trade_id"], ascending=[True, True])

    logger.info(
        f"[TickReplay] Preparing to replay {len(df_target)} raw ticks for dataset_id={dataset_id} "
        f"to Kafka ({kafka_bootstrap}, topic={topic})"
    )

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    ticks_sent = 0
    tickers_seen = set()

    # 1. Send all raw ticks in deterministic order
    for _, row in df_target.iterrows():
        ticker = str(row["ticker_symbol"])
        tickers_seen.add(ticker)

        payload = {
            "dataset_id": str(row["dataset_id"]),
            "trade_id": str(row["trade_id"]),
            "ticker_symbol": ticker,
            "price": float(row["price"]),
            "size": int(row["size"]),
            "exchange": str(row.get("exchange", "NASDAQ")),
            "event_timestamp": str(row["event_timestamp"]),
            "is_late": bool(row.get("is_late", False)),
            "is_duplicate": bool(row.get("is_duplicate", False)),
        }
        key = f"{dataset_id}_{ticker}"
        producer.send(topic, key=key, value=payload)
        ticks_sent += 1

    # 2. Optionally inject malformed ticks to verify DLQ routing
    malformed_sent = 0
    if inject_malformed:
        now_iso = datetime.now(timezone.utc).isoformat()
        bad_ticks = [
            {
                "dataset_id": dataset_id,
                "trade_id": f"dlq_bad_price_{dataset_id}",
                "ticker_symbol": list(tickers_seen)[0] if tickers_seen else "AAPL",
                "price": -50.0,  # Negative price
                "size": 100,
                "exchange": "NASDAQ",
                "event_timestamp": now_iso,
                "is_late": False,
                "is_duplicate": False,
            },
            {
                "dataset_id": dataset_id,
                "trade_id": f"dlq_missing_ticker_{dataset_id}",
                "ticker_symbol": "",  # Missing ticker
                "price": 150.0,
                "size": 50,
                "exchange": "NASDAQ",
                "event_timestamp": now_iso,
                "is_late": False,
                "is_duplicate": False,
            },
        ]
        for bad in bad_ticks:
            producer.send(topic, key=f"{dataset_id}_bad", value=bad)
            malformed_sent += 1

    # 3. Add trailing watermark flush record per partition to advance Flink watermark on all partitions
    flush_sent = 0
    if add_watermark_flush:
        try:
            ts_series = pd.to_datetime(df_target["event_timestamp"], utc=True)
            max_ts = ts_series.max()
        except Exception:
            max_ts = datetime.now(timezone.utc)

        flush_ts = max_ts + timedelta(minutes=flush_delay_minutes)
        flush_iso = flush_ts.isoformat()

        # Send to all partitions of the topic (or at least partitions 0..3)
        try:
            partitions = producer.partitions_for(topic) or {0, 1, 2, 3}
        except Exception:
            partitions = {0, 1, 2, 3}

        for p_id in partitions:
            flush_payload = {
                "dataset_id": dataset_id,
                "trade_id": f"watermark_flush_p{p_id}_{dataset_id}",
                "ticker_symbol": "WATERMARK_FLUSH",
                "price": -1.0,  # Negative price: filtered from deduped_ticks, excluded from DLQ, advances row_time watermark
                "size": 1,
                "exchange": "NASDAQ",
                "event_timestamp": flush_iso,
                "is_late": False,
                "is_duplicate": False,
            }
            producer.send(topic, partition=p_id, value=flush_payload)
            flush_sent += 1

    producer.flush()
    producer.close()

    logger.info(
        f"[TickReplay] Completed: {ticks_sent} valid ticks, {malformed_sent} malformed, "
        f"{flush_sent} watermark flush events sent to topic '{topic}'."
    )

    return {
        "dataset_id": dataset_id,
        "total_ticks_sent": ticks_sent,
        "malformed_sent": malformed_sent,
        "watermark_flush_sent": flush_sent,
        "tickers": sorted(list(tickers_seen)),
        "min_timestamp": str(df_target["event_timestamp"].min()),
        "max_timestamp": str(df_target["event_timestamp"].max()),
    }
