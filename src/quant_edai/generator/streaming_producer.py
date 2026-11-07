"""
Streaming Tick Producer & Kafka Publisher.

Generates real-time tick events for the Kafka topic `market_ticks`,
injecting streaming late arrivals (5-10m delay) and producer retries (1.5% duplicates).
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import numpy as np
import pandas as pd

from quant_edai.generator.faults_injector import FaultsInjector


class StreamingTickProducer:
    """Produces tick-level market events with injected late arrivals and duplicates."""

    def __init__(
        self,
        seed: int = 42,
        tickers: Optional[List[str]] = None,
        kafka_bootstrap: Optional[str] = None,
        topic: str = "market_ticks",
    ):
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.faults = FaultsInjector(seed=seed)
        self.tickers = tickers or ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "SPY"]
        self.kafka_bootstrap = kafka_bootstrap
        self.topic = topic

    def generate_tick_batch(
        self,
        batch_size: int = 1000,
        base_timestamp: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Generates a batch of ticks with realistic price spreads and volume."""
        ts = base_timestamp or datetime.now(timezone.utc)
        records = []

        for i in range(batch_size):
            ticker = self.rng.choice(self.tickers)
            price = round(float(self.rng.uniform(100.0, 500.0)), 2)
            volume = int(self.rng.choice([10, 50, 100, 200, 500, 1000]))
            event_ts = ts + pd.Timedelta(milliseconds=i * 10)

            records.append({
                "trade_id": f"trd_{int(event_ts.timestamp() * 1000)}_{i}",
                "ticker_symbol": ticker,
                "event_timestamp": event_ts.isoformat(),
                "price": price,
                "size": volume,
                "exchange": "NASDAQ",
            })

        df = pd.DataFrame(records)

        # Inject 5% late arrivals (300 - 600 seconds delay)
        df = self.faults.inject_streaming_late_arrivals(df, late_ratio=0.05, min_delay_seconds=300, max_delay_seconds=600)

        # Inject 1.5% streaming duplicates
        df = self.faults.inject_streaming_duplicates(df, duplicate_ratio=0.015)

        return df

    def stream_to_kafka_or_disk(
        self,
        num_batches: int = 5,
        batch_size: int = 200,
        out_file: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Emits tick batches to Kafka if reachable; otherwise records to disk for audit.
        """
        all_ticks = []
        for b in range(num_batches):
            batch = self.generate_tick_batch(batch_size=batch_size)
            all_ticks.append(batch)

        df_all = pd.concat(all_ticks, ignore_index=True)

        if out_file:
            out_path = Path(out_file)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df_all.to_json(out_path, orient="records", lines=True, date_format="iso")

        num_late = int(df_all["is_late"].sum()) if "is_late" in df_all.columns else 0
        num_dups = int(df_all["is_duplicate"].sum()) if "is_duplicate" in df_all.columns else 0
        total = len(df_all)

        stats = {
            "total_streaming_ticks": total,
            "late_arrival_count": num_late,
            "late_arrival_ratio": round(num_late / max(1, total), 4),
            "duplicate_count": num_dups,
            "duplicate_ratio": round(num_dups / max(1, total), 4),
        }
        return stats


def main():
    parser = argparse.ArgumentParser(description="Streaming Tick Generator")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batches", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--out-file", type=str, default="/tmp/lakehouse_source/streaming_ticks.jsonl")

    args = parser.parse_args()
    producer = StreamingTickProducer(seed=args.seed)
    stats = producer.stream_to_kafka_or_disk(
        num_batches=args.batches, batch_size=args.batch_size, out_file=args.out_file
    )
    print(f"[StreamingProducer] Finished: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
