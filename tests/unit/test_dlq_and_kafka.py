"""
Unit tests for Flink Dead Letter Queue (DLQ) routing and schema validation.
"""

import pandas as pd
import pytest

from quant_edai.processing.flink.streaming_job import FlinkStreamingJob


def test_dlq_schema_validation():
    job = FlinkStreamingJob(watermark_delay_seconds=600, allowed_lateness_seconds=600)

    # Construct test dataframe with valid and malformed records
    records = [
        {"trade_id": "t1", "ticker_symbol": "AAPL", "event_timestamp": "2024-06-01T10:00:00Z", "price": 150.0, "size": 100},
        {"trade_id": "t2", "ticker_symbol": "MSFT", "event_timestamp": "2024-06-01T10:00:01Z", "price": -50.0, "size": 100},  # Negative price -> DLQ
        {"trade_id": "t3", "ticker_symbol": "", "event_timestamp": "2024-06-01T10:00:02Z", "price": 200.0, "size": 100},      # Empty ticker -> DLQ
        {"trade_id": "t4", "ticker_symbol": "NVDA", "event_timestamp": "2024-06-01T10:00:03Z", "price": 120.0, "size": -10},    # Negative size -> DLQ
        {"trade_id": "t5", "ticker_symbol": "GOOGL", "event_timestamp": None, "price": 180.0, "size": 50},                      # Missing timestamp -> DLQ
    ]
    df = pd.DataFrame(records)

    df_valid, df_dlq = job.validate_and_filter_dlq(df)

    assert len(df_valid) == 1
    assert df_valid.iloc[0]["ticker_symbol"] == "AAPL"

    assert len(df_dlq) == 4
    dlq_reasons = set(df_dlq["dlq_reason"].tolist())
    assert any("Non-positive price" in r for r in dlq_reasons)
    assert any("Missing or empty ticker_symbol" in r for r in dlq_reasons)
    assert any("Non-positive size" in r for r in dlq_reasons)
    assert any("Missing event_timestamp" in r for r in dlq_reasons)


def test_dlq_expired_late_data_routing():
    job = FlinkStreamingJob(watermark_delay_seconds=60, allowed_lateness_seconds=60)

    # Records: one current, one slightly late (within allowed lateness), one very late (> watermark + lateness)
    records = [
        {"trade_id": "t1", "ticker_symbol": "AAPL", "event_timestamp": "2024-06-01T10:10:00Z", "price": 150.0, "size": 100},
        {"trade_id": "t2", "ticker_symbol": "AAPL", "event_timestamp": "2024-06-01T10:09:30Z", "price": 151.0, "size": 100},  # 30s late -> admitted
        {"trade_id": "t3", "ticker_symbol": "AAPL", "event_timestamp": "2024-06-01T10:00:00Z", "price": 149.0, "size": 100},  # 10m late -> DLQ side output
    ]
    df = pd.DataFrame(records)

    df_bars, summary = job.run_event_time_tumbling_window_1m(df)

    assert summary["dlq_records_routed"] >= 1
    assert summary["valid_ticks_admitted"] >= 2
    assert not df_bars.empty
