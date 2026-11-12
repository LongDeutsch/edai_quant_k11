"""Unit tests for StreamingTickProducer."""

import json
from pathlib import Path
import pandas as pd
import pytest

from quant_edai.generator.streaming_producer import StreamingTickProducer


def test_streaming_tick_batch():
    producer = StreamingTickProducer(seed=42, tickers=["AAPL", "NVDA", "MSFT"])
    df = producer.generate_tick_batch(batch_size=500)

    assert len(df) > 500  # due to injected streaming duplicates
    assert {"trade_id", "ticker_symbol", "event_timestamp", "published_timestamp", "price", "size"}.issubset(
        set(df.columns)
    )
    assert set(df["ticker_symbol"].unique()).issubset({"AAPL", "NVDA", "MSFT"})
    assert (df["price"] > 0).all()
    assert (df["size"] > 0).all()


def test_streaming_to_disk(tmp_path):
    out_file = tmp_path / "stream.jsonl"
    producer = StreamingTickProducer(seed=99)
    stats = producer.stream_to_kafka_or_disk(
        num_batches=3,
        batch_size=100,
        out_file=str(out_file),
        synthetic_batch=True,
    )

    assert out_file.exists()
    assert stats["mode"] == "synthetic_batch"
    assert stats["total_streaming_ticks"] > 300
    assert stats["late_arrival_count"] > 0
    assert stats["duplicate_count"] > 0

    # Read back lines
    with open(out_file, "r") as f:
        lines = f.readlines()
    assert len(lines) == stats["total_streaming_ticks"]
    first_record = json.loads(lines[0])
    assert "ticker_symbol" in first_record
