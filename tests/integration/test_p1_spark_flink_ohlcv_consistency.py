"""P1.1: Spark-equivalent vs Flink-sim OHLCV consistency (dups + late arrivals)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from quant_edai.common.bar_schema import BAR_REQUIRED_FIELDS, missing_bar_fields, normalize_bar_record
from quant_edai.processing.consistency.spark_flink_ohlcv import (
    compare_ohlcv,
    evidence_path,
    flink_sim_windows,
    generate_synthetic_ticks,
    run_consistency,
    spark_equivalent_resample,
    validate_evidence,
)
from quant_edai.processing.flink.streaming_job import FlinkStreamingJob
from quant_edai.quant.market_replay import kafka_message_to_bar


@pytest.fixture(scope="module")
def ticks():
    return generate_synthetic_ticks(seed=7, n_ticks=1500)


@pytest.mark.p1
def test_ticks_contain_duplicates_and_late_arrivals(ticks) -> None:
    assert ticks["is_duplicate"].sum() > 0
    assert ticks["is_late"].sum() > 0


@pytest.mark.p1
def test_spark_flink_ohlcv_within_tolerance(ticks) -> None:
    spark = spark_equivalent_resample(ticks)
    flink, summary = flink_sim_windows(ticks)
    metrics, mismatches = compare_ohlcv(spark, flink)
    assert metrics["passed"], (metrics, mismatches[:3])
    assert metrics["bars_spark"] == metrics["bars_flink"] > 0
    # Late ticks are inside allowed lateness: watermark sim must not dump them to DLQ
    assert summary["dlq_records_routed"] == 0
    assert summary["watermark_mode"] == "PER_RECORD_RUNNING_MAX"


@pytest.mark.p1
def test_compare_detects_divergence(ticks) -> None:
    spark = spark_equivalent_resample(ticks)
    flink, _ = flink_sim_windows(ticks)
    broken = flink.copy()
    broken.loc[broken.index[0], "close_price"] += 1.0
    broken = broken.iloc[:-1]  # also drop a window
    metrics, mismatches = compare_ohlcv(spark, broken)
    assert not metrics["passed"]
    assert metrics["bars_value_mismatch"] >= 1
    assert metrics["bars_missing_in_flink"] == 1
    assert mismatches


@pytest.mark.p1
def test_consistency_evidence_structure(tmp_path: Path) -> None:
    ev = run_consistency(dataset_id="ds_unit_p1", seed=3, n_ticks=800, evidence_root=tmp_path, mode="smoke")
    path = evidence_path(tmp_path, "ds_unit_p1")
    assert path == tmp_path / "consistency" / "ds_unit_p1" / "spark_flink_ohlcv.json"
    assert path.exists()
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert validate_evidence(on_disk) == []
    assert on_disk["dataset_id"] == "ds_unit_p1"
    # Both engines simulated -> never a bare SUCCESS
    assert on_disk["status"] == "PASSED_SIMULATED"
    assert on_disk["execution_mode"] == "SIMULATED"
    assert ev["evidence_path"] == str(path)


@pytest.mark.p1
def test_flink_bars_follow_unified_schema_for_trader(ticks) -> None:
    bars, _ = FlinkStreamingJob().run_event_time_tumbling_window_1m(ticks)
    rec = bars.iloc[0].to_dict()
    assert not missing_bar_fields(rec), missing_bar_fields(rec)
    assert set(BAR_REQUIRED_FIELDS).issubset(rec)
    bar = kafka_message_to_bar(normalize_bar_record(rec))
    assert bar is not None
    assert bar.ticker_symbol == rec["ticker_symbol"]
    assert bar.close_price == pytest.approx(rec["close_price"])
    assert bar.vwap == pytest.approx(rec["vwap"])
    assert bar.timestamp == rec["event_timestamp"]
