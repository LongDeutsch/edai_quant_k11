"""
Gate 3 Live Integration Test: Real Kafka + PyFlink Streaming Integration.

Strict Requirements:
1. Replay raw ticks dataset_id of Spark via Kafka topic `market.ticks` (deterministic ordering).
2. Real PyFlink job running on Docker Flink cluster (quant_flink_jobmanager), NO simulation fallback.
3. RocksDB stateful deduplication, event-time watermarking, 1m tumbling windows, DLQ routing.
4. Output schema contains dataset_id, ticker_symbol, window_start, OHLCV, VWAP, trade_count, version.
5. Bar-by-bar exact mathematical reconciliation between Spark Iceberg Gold and Flink Kafka bars.
6. Replay idempotency, Flink checkpointing/recovery, and late event correction without duplicate logical bars.
7. Bounded replay window finalization via watermark flush mechanism.
8. Runtime evidence collected: Kafka offsets, Flink Job ID, checkpoints, DLQ counts, reconciliation manifest.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.ingestion.s3_uploader import S3SourceUploader
from quant_edai.processing.flink.tick_replay import replay_ticks_from_bronze
from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
from quant_edai.processing.spark.resample_job import SparkResampleJob
from quant_edai.processing.spark.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TestLive02Flink")

FLINK_REST_URL = "http://localhost:8081"
KAFKA_BOOTSTRAP = "localhost:9092"
EVIDENCE_ROOT = Path("evidence/live")


def consume_topic_messages(topic: str, dataset_id: str, timeout_sec: int = 8) -> List[Dict[str, Any]]:
    """Robustly consumes all messages from topic for dataset_id using direct partition assignment."""
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
    part_ids = consumer.partitions_for_topic(topic) or [0]
    partitions = [TopicPartition(topic, p) for p in part_ids]
    consumer.assign(partitions)
    for p in partitions:
        consumer.seek_to_beginning(p)

    records = []
    start_time = time.time()
    while time.time() - start_time < timeout_sec:
        batch = consumer.poll(timeout_ms=1000)
        if not batch:
            if records:
                break
            continue
        for tp, msgs in batch.items():
            for m in msgs:
                try:
                    payload = json.loads(m.value.decode("utf-8"))
                    if payload.get("dataset_id") == dataset_id:
                        records.append(payload)
                except Exception:
                    pass
    consumer.close()
    return records


def _get_flink_jobs() -> List[Dict[str, Any]]:
    req = urllib.request.urlopen(f"{FLINK_REST_URL}/jobs/overview", timeout=5)
    data = json.loads(req.read().decode("utf-8"))
    return data.get("jobs", [])


def _reset_topics_and_submit_flink(consumer_group: str) -> str:
    """Cancels old running jobs, purges test topics to ensure fresh watermark, and submits Flink job."""
    jobs = _get_flink_jobs()
    for j in jobs:
        if j.get("name") == "PyFlink-Market-Ticks-Optimized-Pipeline" and j.get("state") == "RUNNING":
            subprocess.run([
                "docker", "exec", "quant_flink_jobmanager",
                "/opt/flink/bin/flink", "cancel", "-m", "localhost:8081", j["jid"]
            ], check=False)
            time.sleep(2)

    # Recreate topics with 1 partition for clean event-time watermark progression
    subprocess.run(
        "docker exec quant_kafka /bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic market.ticks,market.bars.1m,market.features.5m,market.ticks.dlq",
        shell=True,
        check=False,
    )
    time.sleep(2)
    for t in ["market.ticks", "market.bars.1m", "market.features.5m", "market.ticks.dlq"]:
        subprocess.run(
            f"docker exec quant_kafka /bin/kafka-topics --bootstrap-server localhost:9092 --create --topic {t} --partitions 1 --replication-factor 1",
            shell=True,
            check=True,
        )

    # Submit job to cluster
    cmd = [
        "docker", "exec", "quant_flink_jobmanager",
        "/opt/flink/bin/flink", "run", "-m", "localhost:8081", "-d",
        "-py", "/opt/quant_edai/src/quant_edai/processing/flink/pyflink_streaming_job.py",
        "--parallelism", "1",
        "--consumer-group", consumer_group,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    logger.info(f"[Flink Submit] Output: {res.stdout}")

    # Wait for job to become RUNNING
    for _ in range(30):
        time.sleep(1)
        for j in _get_flink_jobs():
            if j.get("name") == "PyFlink-Market-Ticks-Optimized-Pipeline" and j.get("state") == "RUNNING":
                return j["jid"]

    raise TimeoutError("PyFlink streaming job failed to transition to RUNNING state")


@pytest.fixture(scope="session")
def gate3_dataset(tmp_path_factory) -> Dict[str, Any]:
    """
    Generates a deterministic dataset for Gate 3, runs Spark deduplication into
    Iceberg Silver, and runs Spark resample into Iceberg Gold.
    """
    base_tmp = tmp_path_factory.mktemp("lakehouse_gate3")
    run_ts = int(time.time())
    dataset_id = f"ds_gate3_{run_ts}"
    out_dir = base_tmp / "runs" / dataset_id

    # 1. Reset topics and submit clean Flink job with fresh event-time watermark
    flink_job_id = _reset_topics_and_submit_flink(consumer_group=f"pyflink-gate3-{dataset_id}")

    # 2. Generate Bronze ticks
    gen = QuantDataGenerator(
        mode="smoke",
        scale=0.003,
        seed=101,
        out_dir=str(base_tmp),
        dataset_id=dataset_id,
        start_date="2024-01-02",
        end_date="2024-01-05",
    )
    gen_stats = gen.run()
    bronze_ticks_dir = out_dir / "raw_market_ticks"

    # Upload to MinIO Bronze S3A
    uploader = S3SourceUploader(
        endpoint_url="http://localhost:9000",
        source_dir=str(base_tmp),
        dataset_id=dataset_id,
        mode="integration",
    )
    up_status = uploader.upload_dataset()
    assert up_status.get("status") in {"SUCCESS", "STAGED_LOCAL"}, f"MinIO upload failed: {up_status}"
    s3a_bronze = f"s3a://lakehouse/bronze/runs/{dataset_id}"

    # 3. Run Spark deduplication -> lakehouse.silver.stg_market_ticks
    dedup_job = DeduplicationAndSchemaEvolutionJob(
        source_dir=s3a_bronze,
        target_dir=str(out_dir / "stg_market_ohlcv"),
        iceberg_table="lakehouse.silver.stg_market_ticks",
        dataset_id=dataset_id,
    )
    dedup_stats = dedup_job.run(engine="spark")

    # 4. Run Spark resampling -> lakehouse.gold.fact_market_bars_1m
    resample_job = SparkResampleJob(
        source_ticks_dir="lakehouse.silver.stg_market_ticks",
        gold_1m_dir=str(out_dir / "gold_market_bars_1m"),
        gold_daily_dir=str(out_dir / "gold_market_bars_daily"),
        iceberg_1m_table="lakehouse.gold.fact_market_bars_1m",
        iceberg_daily_table="lakehouse.gold.fact_daily_ohlcv",
        dataset_id=dataset_id,
    )
    resample_stats = resample_job.run(engine="spark")

    return {
        "dataset_id": dataset_id,
        "bronze_ticks_dir": bronze_ticks_dir,
        "out_dir": out_dir,
        "flink_job_id": flink_job_id,
        "gen_stats": gen_stats,
        "dedup_stats": dedup_stats,
        "resample_stats": resample_stats,
    }


def test_live_flink_job_submission_and_checkpoints(gate3_dataset: Dict[str, Any]) -> None:
    """Verifies that the PyFlink job is running on the cluster and successfully completing checkpoints."""
    job_id = gate3_dataset["flink_job_id"]
    assert job_id, "Flink Job ID must be present"

    # Wait for at least 1 checkpoint to complete
    completed_checkpoints = 0
    latest_chk: Dict[str, Any] = {}
    for _ in range(25):
        try:
            req = urllib.request.urlopen(f"{FLINK_REST_URL}/jobs/{job_id}/checkpoints", timeout=5)
            chk_data = json.loads(req.read().decode("utf-8"))
            completed_checkpoints = chk_data.get("counts", {}).get("completed", 0)
            latest_chk = chk_data.get("latest", {}).get("completed", {})
            if completed_checkpoints >= 1:
                break
        except Exception as e:
            logger.warning(f"Error querying Flink checkpoints: {e}")
        time.sleep(1)

    assert completed_checkpoints >= 1, f"Expected >= 1 completed checkpoint, got {completed_checkpoints}"
    assert latest_chk.get("status") == "COMPLETED"
    assert latest_chk.get("state_size", 0) > 0, "Checkpoint state size must be > 0"
    logger.info(f"Verified Flink Job {job_id} Checkpoint {latest_chk.get('id')}: state_size={latest_chk.get('state_size')}B")


def test_live_replay_bronze_ticks_to_kafka_and_dlq(gate3_dataset: Dict[str, Any]) -> None:
    """Replays raw Bronze ticks to Kafka `market.ticks` and verifies clean DLQ routing without flush pollution."""
    ds_id = gate3_dataset["dataset_id"]
    bronze_dir = gate3_dataset["bronze_ticks_dir"]

    # Replay raw ticks with injected malformed ticks and watermark flush
    replay_stats = replay_ticks_from_bronze(
        dataset_id=ds_id,
        bronze_ticks_dir=bronze_dir,
        kafka_bootstrap=KAFKA_BOOTSTRAP,
        topic="market.ticks",
        inject_malformed=True,
        add_watermark_flush=True,
    )
    assert replay_stats["total_ticks_sent"] == 444
    assert replay_stats["malformed_sent"] == 2
    assert replay_stats["watermark_flush_sent"] == 1

    # Verify DLQ received EXACTLY the 2 legitimate malformed records (flush marker excluded)
    time.sleep(2)
    dlq_records = consume_topic_messages("market.ticks.dlq", ds_id, timeout_sec=5)

    assert len(dlq_records) == 2, f"Expected exactly 2 DLQ records (flush excluded), got {len(dlq_records)}: {dlq_records}"
    reasons = {r.get("dlq_reason") for r in dlq_records}
    assert any("INVALID_NEGATIVE_OR_ZERO_PRICE" in r for r in reasons)
    assert any("MISSING_TICKER_SYMBOL" in r for r in reasons)


def test_live_spark_gold_vs_flink_bars_exact_reconciliation(gate3_dataset: Dict[str, Any]) -> None:
    """
    Performs key-by-key, bar-by-bar exact mathematical reconciliation between
    Spark Iceberg Gold 1m bars and Flink Kafka 1m bars with strict VWAP tolerance < 1e-4.
    """
    ds_id = gate3_dataset["dataset_id"]

    # Allow Flink window operator to commit bars
    time.sleep(3)

    # 1. Read Flink bars from Kafka
    flink_bars = consume_topic_messages("market.bars.1m", ds_id, timeout_sec=8)
    assert len(flink_bars) >= 429, f"Expected at least 429 Flink 1m bars, got {len(flink_bars)}"

    df_flink = pd.DataFrame(flink_bars)
    # Deduplicate in case of corrections, keeping version 1 for base comparison
    df_flink_base = df_flink[df_flink["version"] == 1].copy()
    assert len(df_flink_base) == 429, f"Expected 429 base Flink 1m bars, got {len(df_flink_base)}"

    df_flink_base["window_start_dt"] = pd.to_datetime(df_flink_base["window_start"], utc=True)

    # Ensure required bar schema fields exist on every bar
    required_cols = {"dataset_id", "ticker_symbol", "window_start", "open", "high", "low", "close", "volume", "vwap", "trade_count", "version"}
    assert required_cols.issubset(set(df_flink_base.columns)), f"Missing columns in Flink bars: {required_cols - set(df_flink_base.columns)}"

    # 2. Read Spark Gold 1m bars from Iceberg
    spark = get_spark_session(app_name="TestLive02FlinkReconciliation")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")
    spark_rows = spark.sql(f"""
        SELECT
            dataset_id,
            ticker_symbol,
            window_start,
            open_price as open,
            high_price as high,
            low_price as low,
            close_price as close,
            volume,
            vwap,
            trade_count
        FROM lakehouse.gold.fact_market_bars_1m
        WHERE dataset_id = '{ds_id}'
    """).collect()

    assert len(spark_rows) == 429, f"Expected 429 Spark Gold 1m bars, got {len(spark_rows)}"

    df_spark = pd.DataFrame([r.asDict() for r in spark_rows])
    df_spark["window_start_dt"] = pd.to_datetime(df_spark["window_start"], utc=True)

    # 3. Bar-by-bar merge on (ticker_symbol, window_start_dt)
    merged = pd.merge(df_spark, df_flink_base, on=["ticker_symbol", "window_start_dt"], suffixes=("_spark", "_flink"))
    assert len(merged) == 429, f"Expected 429 matched bars, got {len(merged)}"

    # 4. Conservation & correctness gates (strict mathematical parity)
    vol_diff = int((merged["volume_spark"] != merged["volume_flink"]).sum())
    tc_diff = int((merged["trade_count_spark"] != merged["trade_count_flink"]).sum())
    max_open_diff = float((merged["open_spark"] - merged["open_flink"]).abs().max())
    max_high_diff = float((merged["high_spark"] - merged["high_flink"]).abs().max())
    max_low_diff = float((merged["low_spark"] - merged["low_flink"]).abs().max())
    max_close_diff = float((merged["close_spark"] - merged["close_flink"]).abs().max())
    max_vwap_diff = float((merged["vwap_spark"] - merged["vwap_flink"]).abs().max())

    assert vol_diff == 0, f"Volume mismatches found: {vol_diff}"
    assert tc_diff == 0, f"Trade count mismatches found: {tc_diff}"
    assert max_open_diff < 0.01, f"Open price diff exceeds tolerance: {max_open_diff}"
    assert max_high_diff < 0.01, f"High price diff exceeds tolerance: {max_high_diff}"
    assert max_low_diff < 0.01, f"Low price diff exceeds tolerance: {max_low_diff}"
    assert max_close_diff < 0.01, f"Close price diff exceeds tolerance: {max_close_diff}"
    assert max_vwap_diff < 1e-4, f"VWAP diff exceeds strict tolerance < 1e-4: {max_vwap_diff}"

    # 5. Save verification evidence manifest
    manifest_dir = EVIDENCE_ROOT / ds_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "dataset_id": ds_id,
        "engine": "flink",
        "flink_bars_count": len(df_flink_base),
        "spark_bars_count": len(df_spark),
        "matched_bars_count": len(merged),
        "volume_mismatches": vol_diff,
        "trade_count_mismatches": tc_diff,
        "max_open_diff": max_open_diff,
        "max_high_diff": max_high_diff,
        "max_low_diff": max_low_diff,
        "max_close_diff": max_close_diff,
        "max_vwap_diff": max_vwap_diff,
        "volume_conserved": True,
        "trade_count_conserved": True,
        "mathematical_consistency": "100.0%",
        "status": "VERIFIED",
    }
    with open(manifest_dir / "flink_kafka_reconciliation_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def test_live_kafka_replay_idempotency_and_correction_handling(gate3_dataset: Dict[str, Any]) -> None:
    """
    Verifies PyFlink DataStream KeyedProcessFunction Multi-Late Event Correction & Stage 2 Parity:
    1. Publishes 3 distinct late ticks to Kafka `market.ticks` targeting the same 1m window:
       - Late Tick 1: updates High and Close (price=195.0, size=100 at 14:38:50).
       - Late Tick 2: out-of-order earlier timestamp, updates Low and Open (price=170.0, size=50 at 14:38:05).
       - Late Tick 3: timestamp tie with Late 2, smaller trade_id wins Open (price=168.0, size=25 at 14:38:05).
    2. Re-sends Late Tick 2 to verify RocksDB stateful deduplication (no duplicate emission).
    3. Verifies Flink emits monotonically increasing versions (1 -> 2 -> 3 -> 4).
    4. Verifies logical bar deduplication keeps exactly 429 bars with the highest version=4.
    5. Performs Stage 2 Reconciliation: appends late ticks to Iceberg Silver, runs SparkResampleJob,
       and reconciles Spark Gold final vs Flink final bar with strict tolerance < 1e-4.
    """
    ds_id = gate3_dataset["dataset_id"]
    out_dir = gate3_dataset["out_dir"]

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    # Late Tick 1: Higher price, sets new High & Close (timestamp 14:38:55 > base tick 14:38:50.620)
    late_tick_1 = {
        "dataset_id": ds_id,
        "trade_id": f"late_1_{ds_id}",
        "ticker_symbol": "AAPL",
        "price": 195.0000,
        "size": 100,
        "exchange": "NASDAQ",
        "event_timestamp": "2024-01-02T14:38:55Z",
        "is_late": True,
        "is_duplicate": False,
    }

    # Late Tick 2: Out-of-order (earlier timestamp 14:38:05 < base), sets new Low & Open
    late_tick_2 = {
        "dataset_id": ds_id,
        "trade_id": f"late_2_{ds_id}",
        "ticker_symbol": "AAPL",
        "price": 170.0000,
        "size": 50,
        "exchange": "NASDAQ",
        "event_timestamp": "2024-01-02T14:38:05Z",
        "is_late": True,
        "is_duplicate": False,
    }

    # Late Tick 3: Timestamp tie with late_2 (14:38:05), smaller trade_id wins deterministic Open
    late_tick_3 = {
        "dataset_id": ds_id,
        "trade_id": f"late_0_tie_{ds_id}",
        "ticker_symbol": "AAPL",
        "price": 168.0000,
        "size": 25,
        "exchange": "NASDAQ",
        "event_timestamp": "2024-01-02T14:38:05Z",
        "is_late": True,
        "is_duplicate": False,
    }

    # Send Late 1, Late 2, Late 3, and duplicate Late 2
    logger.info("[Test Multi-Late] Publishing 3 late ticks + 1 duplicate to Kafka...")
    producer.send("market.ticks", key=f"{ds_id}_AAPL", value=late_tick_1)
    producer.send("market.ticks", key=f"{ds_id}_AAPL", value=late_tick_2)
    producer.send("market.ticks", key=f"{ds_id}_AAPL", value=late_tick_3)
    # Duplicate of Late 2
    producer.send("market.ticks", key=f"{ds_id}_AAPL", value=late_tick_2)
    producer.flush()
    producer.close()

    time.sleep(4)

    # 1. Consume all bars from Kafka market.bars.1m
    bars = consume_topic_messages("market.bars.1m", ds_id, timeout_sec=8)
    aapl_target_bars = [
        b for b in bars
        if b.get("ticker_symbol") == "AAPL" and "14:38:00" in str(b.get("window_start"))
    ]

    target_versions = sorted([int(b.get("version", 0)) for b in aapl_target_bars])
    logger.info(f"[Test Multi-Late] Observed versions for target window: {target_versions}")

    # Monotonic version assertion: must have witnessed versions 1, 2, 3, 4
    assert 1 in target_versions, "Base version 1 bar missing"
    assert 2 in target_versions, "Late tick 1 version 2 bar missing"
    assert 3 in target_versions, "Late tick 2 version 3 bar missing"
    assert 4 in target_versions, "Late tick 3 version 4 bar missing"
    # Duplicate late 2 must NOT create version 5
    assert 5 not in target_versions, f"Duplicate late tick triggered unexpected version 5: {target_versions}"

    # 2. Downstream compaction / deduplication keeping highest version
    df = pd.DataFrame(bars)
    df = df.sort_values(by="version", ascending=True)
    df_dedup = df.drop_duplicates(subset=["dataset_id", "ticker_symbol", "window_start"], keep="last")

    # Exactly 429 unique logical bars remain
    assert len(df_dedup) == 429, f"Expected 429 unique logical bars after compaction, got {len(df_dedup)}"

    # 3. Inspect the final corrected Flink bar
    final_flink_bar = df_dedup[
        (df_dedup["ticker_symbol"] == "AAPL") & (df_dedup["window_start"].str.contains("14:38:00"))
    ].iloc[0]

    assert int(final_flink_bar["version"]) == 4, f"Expected final version 4, got {final_flink_bar['version']}"
    assert float(final_flink_bar["open"]) == 168.0, f"Expected open 168.0 from tie-breaker, got {final_flink_bar['open']}"
    assert float(final_flink_bar["high"]) == 195.0, f"Expected high 195.0, got {final_flink_bar['high']}"
    assert float(final_flink_bar["low"]) == 168.0, f"Expected low 168.0, got {final_flink_bar['low']}"
    assert float(final_flink_bar["close"]) == 195.0, f"Expected close 195.0, got {final_flink_bar['close']}"
    assert int(final_flink_bar["trade_count"]) == 4, f"Expected trade count 4 (1 base + 3 late), got {final_flink_bar['trade_count']}"
    assert int(final_flink_bar["volume"]) == 71 + 100 + 50 + 25, f"Expected volume 246, got {final_flink_bar['volume']}"

    # 4. Stage 2 Mathematical Reconciliation against Spark Iceberg Gold
    logger.info("[Test Stage 2 Parity] Syncing late ticks to Spark Iceberg Silver and re-running SparkResampleJob...")
    spark = get_spark_session(app_name="TestLive02Stage2Reconciliation")

    # Insert 3 late ticks into lakehouse.silver.stg_market_ticks
    spark.sql(f"""
        INSERT INTO lakehouse.silver.stg_market_ticks
        SELECT
            'late_1_{ds_id}' AS trade_id,
            'AAPL' AS ticker_symbol,
            CAST(195.0 AS DOUBLE) AS price,
            CAST(100 AS BIGINT) AS `size`,
            TO_TIMESTAMP('2024-01-02T14:38:55Z') AS event_timestamp,
            current_timestamp() AS ingestion_timestamp,
            'NASDAQ' AS exchange,
            '{ds_id}' AS dataset_id,
            'NORMAL' AS market_regime,
            'REGULAR' AS quote_condition,
            CAST(194.9 AS DOUBLE) AS bid_price,
            CAST(195.1 AS DOUBLE) AS ask_price,
            '2024-01-02' AS trade_date,
            current_timestamp() AS processed_timestamp
        UNION ALL
        SELECT
            'late_2_{ds_id}', 'AAPL', 170.0, 50, TO_TIMESTAMP('2024-01-02T14:38:05Z'), current_timestamp(), 'NASDAQ', '{ds_id}', 'NORMAL', 'REGULAR', 169.9, 170.1, '2024-01-02', current_timestamp()
        UNION ALL
        SELECT
            'late_0_tie_{ds_id}', 'AAPL', 168.0, 25, TO_TIMESTAMP('2024-01-02T14:38:05Z'), current_timestamp(), 'NASDAQ', '{ds_id}', 'NORMAL', 'REGULAR', 167.9, 168.1, '2024-01-02', current_timestamp()
    """)

    # Re-run SparkResampleJob
    resample_job = SparkResampleJob(
        source_ticks_dir="lakehouse.silver.stg_market_ticks",
        gold_1m_dir=str(out_dir / "gold_market_bars_1m_reconciled"),
        gold_daily_dir=str(out_dir / "gold_market_bars_daily_reconciled"),
        iceberg_1m_table="lakehouse.gold.fact_market_bars_1m",
        iceberg_daily_table="lakehouse.gold.fact_daily_ohlcv",
        dataset_id=ds_id,
    )
    resample_job.run(engine="spark")

    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")
    spark_final_rows = spark.sql(f"""
        SELECT
            open_price as open,
            high_price as high,
            low_price as low,
            close_price as close,
            volume,
            vwap,
            trade_count
        FROM lakehouse.gold.fact_market_bars_1m
        WHERE dataset_id = '{ds_id}'
          AND ticker_symbol = 'AAPL'
          AND window_start LIKE '%14:38:00%'
    """).collect()

    assert len(spark_final_rows) == 1, f"Expected 1 Spark updated Gold bar, got {len(spark_final_rows)}"
    spark_final_bar = spark_final_rows[0].asDict()

    logger.info(f"[Test Stage 2 Parity] Spark Final Bar: {spark_final_bar}")
    logger.info(f"[Test Stage 2 Parity] Flink Final Bar: {final_flink_bar[['open', 'high', 'low', 'close', 'volume', 'vwap', 'trade_count']].to_dict()}")

    # Assert exact match between Spark Gold and Flink streaming final bar
    assert float(spark_final_bar["open"]) == float(final_flink_bar["open"])
    assert float(spark_final_bar["high"]) == float(final_flink_bar["high"])
    assert float(spark_final_bar["low"]) == float(final_flink_bar["low"])
    assert float(spark_final_bar["close"]) == float(final_flink_bar["close"])
    assert int(spark_final_bar["volume"]) == int(final_flink_bar["volume"])
    assert int(spark_final_bar["trade_count"]) == int(final_flink_bar["trade_count"])
    vwap_diff = abs(float(spark_final_bar["vwap"]) - float(final_flink_bar["vwap"]))
    assert vwap_diff < 1e-4, f"Stage 2 VWAP diff {vwap_diff} exceeds tolerance < 1e-4"

    # Save Stage 2 reconciliation manifest
    manifest_dir = EVIDENCE_ROOT / ds_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "dataset_id": ds_id,
        "test_type": "STAGE_2_MULTI_LATE_RECONCILIATION",
        "target_window": "2024-01-02 14:38:00",
        "ticker_symbol": "AAPL",
        "late_ticks_injected": 3,
        "duplicate_ticks_tested": 1,
        "observed_versions": target_versions,
        "final_version": int(final_flink_bar["version"]),
        "spark_final_bar": spark_final_bar,
        "flink_final_bar": {
            "open": float(final_flink_bar["open"]),
            "high": float(final_flink_bar["high"]),
            "low": float(final_flink_bar["low"]),
            "close": float(final_flink_bar["close"]),
            "volume": int(final_flink_bar["volume"]),
            "vwap": float(final_flink_bar["vwap"]),
            "trade_count": int(final_flink_bar["trade_count"]),
            "version": int(final_flink_bar["version"]),
        },
        "vwap_diff": vwap_diff,
        "mathematical_consistency": "100.0%",
        "status": "VERIFIED",
    }
    with open(manifest_dir / "flink_multi_late_reconciliation_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"[Test Stage 2 Parity] Multi-late reconciliation successfully verified with diff={vwap_diff}!")


def test_live_flink_taskmanager_recovery_under_active_stream(gate3_dataset: Dict[str, Any]) -> None:
    """
    Verifies Flink resilience: restarts TaskManager while pipeline is active,
    verifying state recovery from RocksDB checkpoint without data corruption.
    """
    job_id = gate3_dataset["flink_job_id"]
    subprocess.run(["docker", "restart", "quant_flink_taskmanager"], check=True)
    time.sleep(6)

    # Verify JobManager reconnected to TaskManager and restored job
    restored = False
    for _ in range(20):
        try:
            req = urllib.request.urlopen(f"{FLINK_REST_URL}/jobs/{job_id}/checkpoints", timeout=5)
            data = json.loads(req.read().decode("utf-8"))
            if data.get("counts", {}).get("restored", 0) > 0 or data.get("latest", {}).get("restored") is not None:
                restored = True
                break
        except Exception:
            pass
        time.sleep(1)

    assert restored, f"Expected Flink job {job_id} to record state restoration after TaskManager restart"
    logger.info(f"Verified RocksDB state restoration for Job {job_id}")


def test_gate2_regression_intact(gate3_dataset: Dict[str, Any]) -> None:
    """Verifies that Gate 2 Iceberg tables remain intact and accessible."""
    ds_id = gate3_dataset["dataset_id"]
    spark = get_spark_session(app_name="TestGate2Regression")
    spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")

    silver_count = spark.sql(f"SELECT COUNT(*) as cnt FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_id}'").collect()[0]["cnt"]
    gold_count = spark.sql(f"SELECT COUNT(*) as cnt FROM lakehouse.gold.fact_market_bars_1m WHERE dataset_id = '{ds_id}'").collect()[0]["cnt"]

    assert silver_count in {436, 439}, f"Expected 436 (base) or 439 (with 3 late ticks) Silver rows, got {silver_count}"
    assert gold_count == 429, f"Expected 429 Gold 1m rows, got {gold_count}"
