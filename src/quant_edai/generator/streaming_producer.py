"""
Streaming Tick Producer & Kafka Publisher.

Generates real-time tick events for the Kafka topic `market.ticks`,
injecting streaming late arrivals (5-10m delay), producer retries (1.5% duplicates),
and optional malformed payloads for Dead Letter Queue (DLQ) validation.
Supports both JSON and Confluent Avro Wire Format (Magic Byte 0x00 + 4-byte Schema ID).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import numpy as np
import pandas as pd

from quant_edai.generator.faults_injector import FaultsInjector
from quant_edai.governance.schema_registry_client import SchemaRegistryClient


class StreamingTickProducer:
    """Produces tick-level market events with injected late arrivals and duplicates."""

    def __init__(
        self,
        seed: int = 42,
        tickers: Optional[List[str]] = None,
        kafka_bootstrap: Optional[str] = None,
        topic: str = "market.ticks",
        dlq_topic: str = "market.ticks.dlq",
        registry_url: Optional[str] = None,
        message_format: str = "avro",
    ):
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.faults = FaultsInjector(seed=seed)
        self.tickers = tickers or ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "SPY"]
        self.kafka_bootstrap = kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.message_format = message_format.lower()
        self.registry_client = SchemaRegistryClient(registry_url=registry_url)

        # Load Avro schema
        v1_path = Path("schemas/avro/market_tick_v1.avsc")
        if v1_path.exists():
            with open(v1_path, "r") as f:
                self.avro_schema = json.load(f)
        else:
            self.avro_schema = None

    def generate_tick_batch(
        self,
        batch_size: int = 1000,
        base_timestamp: Optional[datetime] = None,
        inject_malformed: bool = False,
    ) -> pd.DataFrame:
        """Generates a batch of ticks with realistic price spreads, volume, and injected faults."""
        ts = base_timestamp or datetime.now(timezone.utc)
        records = []

        for i in range(batch_size):
            ticker = self.rng.choice(self.tickers)
            price = round(float(self.rng.uniform(100.0, 500.0)), 2)
            volume = int(self.rng.choice([10, 50, 100, 200, 500, 1000]))
            event_ts = ts + pd.to_timedelta(float(i * 10), unit="ms")

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

        # Inject 2 malformed records for DLQ routing test (negative price, missing ticker)
        if inject_malformed and len(df) > 10:
            malformed_1 = {
                "trade_id": f"bad_trd_neg_price_{int(ts.timestamp())}",
                "ticker_symbol": "AAPL",
                "event_timestamp": ts.isoformat(),
                "price": -99.99,
                "size": 100,
                "exchange": "NASDAQ",
                "is_late": False,
                "is_duplicate": False,
            }
            malformed_2 = {
                "trade_id": f"bad_trd_missing_ticker_{int(ts.timestamp())}",
                "ticker_symbol": "",
                "event_timestamp": ts.isoformat(),
                "price": 150.0,
                "size": -10,
                "exchange": "NASDAQ",
                "is_late": False,
                "is_duplicate": False,
            }
            df = pd.concat([df, pd.DataFrame([malformed_1, malformed_2])], ignore_index=True)

        return df

    def publish_to_kafka(self, records: List[Dict[str, Any]]) -> int:
        """Publishes records to Kafka topic using Avro Wire Format or JSON."""
        if not self.kafka_bootstrap:
            return 0

        try:
            from kafka import KafkaProducer
            from kafka.admin import KafkaAdminClient, NewTopic
        except ImportError:
            print("[StreamingProducer] kafka-python not installed; skipping Kafka publish.")
            return 0

        try:
            # Ensure business topics exist (do NOT create internal _schemas)
            try:
                admin = KafkaAdminClient(bootstrap_servers=self.kafka_bootstrap, request_timeout_ms=3000)
                existing = admin.list_topics()
                new_topics = []
                for t in [self.topic, self.dlq_topic, "market.bars.1m", "market.features.5m"]:
                    if t not in existing:
                        new_topics.append(NewTopic(name=t, num_partitions=4, replication_factor=1))
                if new_topics:
                    admin.create_topics(new_topics=new_topics)
                admin.close()
            except Exception:
                pass

            # Resolve Schema ID if Avro format is requested
            schema_id = 1
            if self.message_format == "avro" and self.avro_schema and self.registry_client.is_reachable():
                try:
                    schema_id = self.registry_client.register_schema(f"{self.topic}-value", self.avro_schema)
                except Exception as e:
                    print(f"[StreamingProducer] Using fallback schema ID 1 ({e})")

            # Value serializer based on message format
            if self.message_format == "avro" and self.avro_schema:
                value_serializer = lambda v: self.registry_client.encode_wire_format(v, self.avro_schema, schema_id=schema_id)
            else:
                value_serializer = lambda v: json.dumps(v).encode("utf-8")

            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=value_serializer,
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                request_timeout_ms=5000,
            )

            published = 0
            for r in records:
                key = r.get("ticker_symbol") or "UNKNOWN"
                # Strip out internal fault tracking flags if Avro schema doesn't have them
                payload = {k: v for k, v in r.items() if self.avro_schema and k in {f["name"] for f in self.avro_schema.get("fields", [])}}
                if not payload:
                    payload = r
                producer.send(self.topic, key=key, value=payload)
                published += 1

            producer.flush(timeout=5)
            producer.close()
            print(f"[StreamingProducer] Published {published} ticks (format: {self.message_format.upper()}) to Kafka topic '{self.topic}' at {self.kafka_bootstrap}")
            return published
        except Exception as e:
            print(f"[StreamingProducer] Kafka publish skipped/failed ({e}); retaining local output.")
            return 0

    def stream_to_kafka_or_disk(
        self,
        num_batches: int = 5,
        batch_size: int = 200,
        out_file: Optional[str] = None,
        publish_kafka: bool = True,
        inject_malformed: bool = False,
        from_corpus: Optional[str] = None,
        max_ticks: Optional[int] = None,
        synthetic_batch: bool = False,
    ) -> Dict[str, Any]:
        """Replay online tick corpus (default) or emit synthetic batches for injector unit tests."""
        mode = "synthetic_batch"
        if from_corpus and not synthetic_batch:
            from quant_edai.generator.tick_corpus import load_tick_corpus

            corpus_path = Path(from_corpus)
            if not corpus_path.exists():
                raise FileNotFoundError(
                    f"Online tick corpus missing at {corpus_path}. "
                    "Run `make generate-online-ticks` (or `make generate-data`) first."
                )
            df_all = load_tick_corpus(str(corpus_path), max_ticks=max_ticks)
            mode = "corpus_replay"
            if inject_malformed and len(df_all) > 10:
                # Append malformed probes without rewriting the persisted corpus
                ts = datetime.now(timezone.utc).isoformat()
                extra = pd.DataFrame(
                    [
                        {
                            "trade_id": f"bad_trd_neg_price_{int(time.time())}",
                            "ticker_symbol": "AAPL",
                            "event_timestamp": ts,
                            "price": -99.99,
                            "size": 100,
                            "exchange": "NASDAQ",
                            "is_late": False,
                            "is_duplicate": False,
                        },
                        {
                            "trade_id": f"bad_trd_missing_ticker_{int(time.time())}",
                            "ticker_symbol": "",
                            "event_timestamp": ts,
                            "price": 150.0,
                            "size": -10,
                            "exchange": "NASDAQ",
                            "is_late": False,
                            "is_duplicate": False,
                        },
                    ]
                )
                df_all = pd.concat([df_all, extra], ignore_index=True)
        else:
            all_ticks = []
            for _ in range(num_batches):
                batch = self.generate_tick_batch(
                    batch_size=batch_size, inject_malformed=inject_malformed
                )
                all_ticks.append(batch)
            df_all = pd.concat(all_ticks, ignore_index=True)

        records = df_all.to_dict(orient="records")

        published_kafka_count = 0
        if publish_kafka and self.kafka_bootstrap:
            published_kafka_count = self.publish_to_kafka(records)

        if out_file:
            out_path = Path(out_file)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df_all.to_json(out_path, orient="records", lines=True, date_format="iso")

        num_late = int(df_all["is_late"].sum()) if "is_late" in df_all.columns else 0
        num_dups = int(df_all["is_duplicate"].sum()) if "is_duplicate" in df_all.columns else 0
        total = len(df_all)

        stats = {
            "total_streaming_ticks": total,
            "mode": mode,
            "from_corpus": from_corpus if mode == "corpus_replay" else None,
            "message_format": self.message_format,
            "late_arrival_count": num_late,
            "late_arrival_ratio": round(num_late / max(1, total), 4),
            "duplicate_count": num_dups,
            "duplicate_ratio": round(num_dups / max(1, total), 4),
            "published_to_kafka_count": published_kafka_count,
            "kafka_bootstrap": self.kafka_bootstrap,
            "kafka_topic": self.topic,
            "out_file": out_file,
            "status": "SUCCESS",
        }
        return stats


def main():
    parser = argparse.ArgumentParser(description="Streaming Market Tick Producer / Corpus Replay")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batches", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--out-file", type=str, default="/tmp/lakehouse_source/streaming_ticks.jsonl")
    parser.add_argument("--kafka-bootstrap", type=str, default=None, help="Kafka bootstrap server, e.g., localhost:9092")
    parser.add_argument("--topic", type=str, default="market.ticks")
    parser.add_argument("--format", type=str, default="avro", choices=["avro", "json"])
    parser.add_argument("--registry-url", type=str, default=None)
    parser.add_argument("--inject-malformed", action="store_true", default=False, help="Inject malformed records for DLQ verification")
    parser.add_argument(
        "--from-corpus",
        default=os.path.join(os.getenv("OUT_DIR", "/tmp/lakehouse_source"), "raw_market_ticks_online"),
        help="Replay pre-materialized online tick corpus (default)",
    )
    parser.add_argument("--max-ticks", type=int, default=None, help="Cap replay size for CI/smoke")
    parser.add_argument(
        "--synthetic-batch",
        action="store_true",
        help="Legacy random batches (injector unit tests only); ignores corpus",
    )

    args = parser.parse_args()
    producer = StreamingTickProducer(
        seed=args.seed,
        kafka_bootstrap=args.kafka_bootstrap,
        topic=args.topic,
        message_format=args.format,
        registry_url=args.registry_url,
    )
    stats = producer.stream_to_kafka_or_disk(
        num_batches=args.batches,
        batch_size=args.batch_size,
        out_file=args.out_file,
        inject_malformed=args.inject_malformed,
        from_corpus=None if args.synthetic_batch else args.from_corpus,
        max_ticks=args.max_ticks,
        synthetic_batch=args.synthetic_batch,
    )
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
