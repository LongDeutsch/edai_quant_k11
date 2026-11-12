"""Publish Bronze lakehouse market data onto Kafka for the paper trader.

Flow (honest, no trader-side parquet reads):
  Bronze Parquet (raw_market_ohlcv)
    → this publisher
    → Kafka topics ``market.ticks`` and ``market.bars.1m``
    → ``make trader-run`` (Kafka consumer only)

Daily Bronze OHLCV rows are expanded into a short sequence of 1-minute bars
around each session so the intraday engine receives bar-shaped events without
requiring Flink for the smoke demo. Production path can still use
``make stream-kafka`` + Flink → ``market.bars.1m``.
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from quant_edai.common.dataset_paths import resolve_dataset_root
from quant_edai.common.execution_status import is_integration_mode, normalize_mode
from quant_edai.common.kafka_env import resolve_kafka_bootstrap
from quant_edai.common.market_time import MARKET_TZ

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BronzeKafkaPublisher")

DEFAULT_BOOTSTRAP = resolve_kafka_bootstrap()  # KAFKA_BOOTSTRAP or KAFKA_BOOTSTRAP_SERVERS
BARS_TOPIC = "market.bars.1m"
TICKS_TOPIC = "market.ticks"


def _dataset_root() -> Optional[Path]:
    """OUT_DIR/runs/{dataset_id} (CURRENT_DATASET pointer) when a generator run exists."""
    try:
        return resolve_dataset_root(os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    except FileNotFoundError:
        return None


def resolve_bronze_path(explicit: Optional[str] = None) -> Path:
    ds_root = _dataset_root()
    candidates = [
        explicit,
        os.path.join(str(ds_root), "raw_market_ohlcv") if ds_root else None,
        os.path.join(os.getenv("OUT_DIR", "/tmp/lakehouse_source"), "raw_market_ohlcv"),
        os.path.join(os.getenv("OUT_DIR", "/tmp/lakehouse_source"), "bronze", "raw_market_ohlcv"),
        "evidence/phase3/feat_ticker_technical_indicators.parquet",  # last-resort feature close as proxy
    ]
    for c in candidates:
        if not c:
            continue
        p = Path(c)
        if p.exists():
            return p
    raise FileNotFoundError(
        "Bronze source not found. Run data generation first "
        f"(expected under {os.getenv('OUT_DIR', '/tmp/lakehouse_source')}/raw_market_ohlcv)."
    )


def load_bronze_frame(source: Path, max_rows: int = 500) -> pd.DataFrame:
    if source.is_file() and source.suffix == ".parquet":
        df = pd.read_parquet(source)
    else:
        files = sorted(glob.glob(str(source / "**" / "*.parquet"), recursive=True))
        if not files:
            raise FileNotFoundError(f"No parquet files under {source}")
        frames = [pd.read_parquet(f) for f in files[:40]]
        df = pd.concat(frames, ignore_index=True)

    # Normalize columns from bronze / feature tables
    rename = {
        "ticker": "ticker_symbol",
        "date": "trade_date",
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
    }
    for old, new in rename.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    if "ticker_symbol" not in df.columns:
        raise ValueError("Bronze frame missing ticker_symbol")
    if "close_price" not in df.columns and "close" in df.columns:
        df["close_price"] = df["close"]

    ts_col = next(
        (c for c in ("event_timestamp", "ingestion_timestamp", "trade_date", "timestamp") if c in df.columns),
        None,
    )
    if ts_col is None:
        raise ValueError("Bronze frame missing timestamp/trade_date column")
    df["event_timestamp"] = pd.to_datetime(df[ts_col], utc=True)

    for col in ("open_price", "high_price", "low_price"):
        if col not in df.columns:
            df[col] = df["close_price"]
    if "volume" not in df.columns:
        df["volume"] = 1000.0

    df = df.sort_values(["event_timestamp", "ticker_symbol"]).head(max_rows)
    return df.reset_index(drop=True)


def bronze_rows_to_bars(
    df: pd.DataFrame, bars_per_row: int = 5, dataset_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Expand each Bronze OHLCV row into a few 1m bar events for Kafka."""
    messages: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        # Anchor each daily row at the 09:30 America/New_York open (explicit offset, no Z ambiguity)
        session_date = pd.Timestamp(row["event_timestamp"]).date()
        base_ts = datetime(session_date.year, session_date.month, session_date.day, 9, 30, tzinfo=MARKET_TZ)
        o = float(row["open_price"])
        h = float(row["high_price"])
        l = float(row["low_price"])
        c = float(row["close_price"])
        vol = float(row.get("volume", 1000.0))
        ticker = str(row["ticker_symbol"])
        path = [o, h, l, c] if bars_per_row >= 4 else [o, c]
        # pad / sample to bars_per_row
        while len(path) < bars_per_row:
            path.append(c)
        path = path[:bars_per_row]
        for i, px in enumerate(path):
            ts = base_ts + timedelta(minutes=i)
            hi = max(px * 1.0005, px)
            lo = min(px * 0.9995, px)
            messages.append(
                {
                    "ticker_symbol": ticker,
                    "event_timestamp": ts.isoformat(),
                    "open_price": round(px * 0.9999, 4),
                    "high_price": round(hi, 4),
                    "low_price": round(lo, 4),
                    "close_price": round(px, 4),
                    "volume": round(vol / bars_per_row, 2),
                    "vwap": round(px, 4),
                    "source_layer": "bronze",
                    "source_table": "raw_market_ohlcv",
                    **({"dataset_id": dataset_id} if dataset_id else {}),
                }
            )
    return messages


def bars_to_ticks(bars: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ticks: List[Dict[str, Any]] = []
    for i, bar in enumerate(bars):
        ticks.append(
            {
                "trade_id": f"bronze_{bar['ticker_symbol']}_{i}",
                "ticker_symbol": bar["ticker_symbol"],
                "event_timestamp": bar["event_timestamp"],
                "price": bar["close_price"],
                "size": int(max(1, bar.get("volume", 1))),
                "exchange": "NASDAQ",
                "source_layer": "bronze",
            }
        )
    return ticks


def ensure_topics(bootstrap: str, topics: List[str]) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
    try:
        existing = set(admin.list_topics())
        to_create = [NewTopic(name=t, num_partitions=4, replication_factor=1) for t in topics if t not in existing]
        if to_create:
            admin.create_topics(to_create)
            logger.info("Created Kafka topics: %s", [t.name for t in to_create])
    finally:
        admin.close()


def publish_messages(bootstrap: str, topic: str, messages: List[Dict[str, Any]]) -> int:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )
    sent = 0
    try:
        for msg in messages:
            key = str(msg.get("ticker_symbol", "NA"))
            producer.send(topic, key=key, value=msg)
            sent += 1
        producer.flush(timeout=10)
    finally:
        producer.close()
    return sent


def run_publish(
    bronze_path: Optional[str] = None,
    bootstrap: Optional[str] = None,
    max_rows: int = 400,
    bars_per_row: int = 5,
) -> Dict[str, Any]:
    if is_integration_mode():
        raise RuntimeError(
            f"[MODE={normalize_mode()}] Bronze→Kafka bar expansion is a smoke-only demo path; "
            "integration|full must consume Flink-aggregated market.bars.1m (make stream-flink)."
        )
    bootstrap = resolve_kafka_bootstrap(bootstrap)
    source = resolve_bronze_path(bronze_path)
    ds_root = _dataset_root()
    dataset_id = ds_root.name if ds_root is not None and ds_root.parent.name == "runs" else os.environ.get("DATASET_ID")
    df = load_bronze_frame(source, max_rows=max_rows)
    bars = bronze_rows_to_bars(df, bars_per_row=bars_per_row, dataset_id=dataset_id)
    ticks = bars_to_ticks(bars)

    ensure_topics(bootstrap, [BARS_TOPIC, TICKS_TOPIC])
    n_bars = publish_messages(bootstrap, BARS_TOPIC, bars)
    n_ticks = publish_messages(bootstrap, TICKS_TOPIC, ticks)

    summary = {
        "status": "PUBLISHED_SIMULATED_FEED",
        "execution_mode": "SIMULATED",
        "dataset_id": dataset_id,
        "bronze_source": str(source),
        "kafka_bootstrap": bootstrap,
        "rows_loaded": int(len(df)),
        "bars_published": n_bars,
        "ticks_published": n_ticks,
        "bars_topic": BARS_TOPIC,
        "ticks_topic": TICKS_TOPIC,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    out = Path("evidence/phase6/bronze_kafka_publish.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info(
        "Published %s bars → %s and %s ticks → %s from bronze %s",
        n_bars,
        BARS_TOPIC,
        n_ticks,
        TICKS_TOPIC,
        source,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish Bronze market data to Kafka for paper trading")
    parser.add_argument("--bronze-path", default=None, help="Path to bronze parquet file or directory")
    parser.add_argument("--kafka-bootstrap", default=None, help="Defaults to KAFKA_BOOTSTRAP / KAFKA_BOOTSTRAP_SERVERS")
    parser.add_argument("--max-rows", type=int, default=400)
    parser.add_argument("--bars-per-row", type=int, default=5)
    args = parser.parse_args()
    summary = run_publish(
        bronze_path=args.bronze_path,
        bootstrap=args.kafka_bootstrap,
        max_rows=args.max_rows,
        bars_per_row=args.bars_per_row,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
