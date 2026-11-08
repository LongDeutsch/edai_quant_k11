"""Unit tests for Flink Event-Time Windowing & RocksDB Deduplication."""

import pandas as pd
import pytest

from quant_edai.generator.streaming_producer import StreamingTickProducer
from quant_edai.processing.flink.streaming_dedup import RocksDBStateDeduplicator
from quant_edai.processing.flink.streaming_job import FlinkStreamingJob


@pytest.fixture
def streaming_ticks():
    producer = StreamingTickProducer(seed=42)
    return producer.generate_tick_batch(batch_size=800)


def test_rocksdb_deduplication(streaming_ticks):
    deduplicator = RocksDBStateDeduplicator(ttl_seconds=3600)
    df_clean, summary = deduplicator.process_batch(streaming_ticks)

    # Injected streaming duplicates must be eliminated
    assert summary["duplicate_ticks_eliminated"] > 0
    assert summary["unique_ticks_retained"] < len(streaming_ticks)
    assert len(df_clean) == summary["unique_ticks_retained"]


def test_flink_baseline_drops_late_data(streaming_ticks):
    job = FlinkStreamingJob(watermark_delay_seconds=600)
    baseline_stats = job.run_baseline_processing_time(streaming_ticks)

    assert baseline_stats["watermark_enabled"] is False
    assert baseline_stats["late_ticks_dropped"] > 0


def test_flink_event_time_tumbling_window(streaming_ticks):
    job = FlinkStreamingJob(watermark_delay_seconds=600, enable_dedup=True)
    df_bars, summary = job.run_event_time_tumbling_window_1m(streaming_ticks)

    assert not df_bars.empty
    assert {"ticker_symbol", "window_start", "open_price", "high_price", "low_price", "close_price", "volume", "vwap_1m"}.issubset(
        set(df_bars.columns)
    )
    # Price integrity
    assert (df_bars["high_price"] >= df_bars["low_price"]).all()
    assert (df_bars["high_price"] >= df_bars["open_price"]).all()
    assert (df_bars["high_price"] >= df_bars["close_price"]).all()
    assert (df_bars["low_price"] <= df_bars["open_price"]).all()
    assert (df_bars["low_price"] <= df_bars["close_price"]).all()
    assert (df_bars["vwap_1m"] > 0).all()


def test_flink_sliding_window_5m(streaming_ticks):
    job = FlinkStreamingJob(watermark_delay_seconds=600, enable_dedup=True)
    df_bars, _ = job.run_event_time_tumbling_window_1m(streaming_ticks)
    df_sliding = job.run_sliding_window_5m(df_bars)

    assert not df_sliding.empty
    assert {"ticker_symbol", "window_start", "rolling_vwap_5m", "rolling_volatility_5m", "rolling_volume_5m"}.issubset(
        set(df_sliding.columns)
    )
    assert (df_sliding["rolling_volume_5m"] > 0).all()
    assert (df_sliding["rolling_vwap_5m"] > 0).all()
