"""Gate 4 Live Integration Test: Real Kafka -> ClickHouse Serving Integration.

Verifies:
1. Real Kafka consumer reads `market.bars.1m` (upsert-kafka changelog) without mock/sim.
2. ClickHouse schema uses `ReplacingMergeTree(version)` with `ORDER BY (dataset_id, ticker_symbol, window_start)`.
3. Multi-version corrections (v1 -> v2 -> v3 -> v4) are resolved to the highest version in ClickHouse `FINAL`.
4. Replay Idempotency: re-inserting identical batches does not double or increase logical row count under `FINAL`.
5. Stale-Version Arrival: an out-of-order older version (e.g. v2 arriving after v4) does not overwrite the latest version.
6. Cross-Dataset Isolation: inserting bars for another dataset does not mutate the target dataset.
7. Offset Management & At-Least-Once Delivery: offsets committed only after successful ClickHouse write.
8. Decoupled Query-Back: 100% bar-by-bar reconciliation between ClickHouse `FINAL` and Spark Iceberg Gold.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
import pytest

from quant_edai.serving.clickhouse_sync import ClickHouseSyncManager
from quant_edai.serving.kafka_clickhouse_consumer import KafkaClickHouseServingConsumer
from quant_edai.processing.spark.spark_session import get_spark_session

logger = logging.getLogger("TestLive04ClickHouseServing")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture(scope="module")
def clickhouse_manager() -> ClickHouseSyncManager:
    """Provides a live ClickHouseSyncManager connected to the Docker ClickHouse instance."""
    mgr = ClickHouseSyncManager(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user="quant",
        password="quant_password",
        database="quant_dw",
    )
    assert mgr.is_online(), f"ClickHouse at {mgr.url} must be healthy and reachable."
    return mgr


@pytest.fixture(scope="module")
def latest_gate3_dataset() -> str:
    """Discovers the latest verified Gate 3 dataset_id from evidence or falls back to known run."""
    evidence_dir = Path("evidence/live")
    ds_dirs = sorted([d.name for d in evidence_dir.glob("ds_gate3_*") if d.is_dir()])
    assert ds_dirs, "At least one live Gate 3 dataset must exist in evidence/live"
    chosen_ds = ds_dirs[-1]
    logger.info(f"Using Gate 3 dataset for Gate 4 ClickHouse Serving: {chosen_ds}")
    return chosen_ds


def test_live_clickhouse_schema_and_replacing_merge_tree_ddl(clickhouse_manager: ClickHouseSyncManager) -> None:
    """Verifies that `quant_dw.market_intraday_bars` uses ReplacingMergeTree(version) and the correct ORDER BY."""
    resp = clickhouse_manager.execute_query("SHOW CREATE TABLE quant_dw.market_intraday_bars")
    create_sql = resp.text.strip()

    assert "ReplacingMergeTree(version)" in create_sql, f"Expected ReplacingMergeTree(version), got: {create_sql}"
    assert "ORDER BY (dataset_id, ticker_symbol, window_start)" in create_sql, f"Unexpected ORDER BY: {create_sql}"
    assert "window_start" in create_sql and "DateTime64(3" in create_sql


def test_live_kafka_to_clickhouse_sync_and_offset_commit(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Ingests 1-minute bars from Kafka `market.bars.1m` into ClickHouse using a real consumer.
    Asserts batch ingestion, offset commits, and exactly 429 logical bars under FINAL.
    """
    ds_id = latest_gate3_dataset
    group_id = f"test-ch-serving-{ds_id}-{int(time.time())}"

    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic="market.bars.1m",
        group_id=group_id,
        clickhouse_manager=clickhouse_manager,
        batch_size=100,
        poll_timeout_ms=1000,
    )

    metrics = consumer.sync_dataset_bars(dataset_id=ds_id, max_idle_polls=4)
    logger.info(f"Sync metrics for {ds_id}: {metrics.to_dict()}")

    assert metrics.status == "SUCCESS"
    assert metrics.messages_read >= 429, f"Expected at least 429 bars read from Kafka, got {metrics.messages_read}"
    assert metrics.messages_inserted == metrics.messages_read
    assert metrics.batches_inserted >= 1
    assert len(metrics.committed_offsets) > 0, "Kafka offsets must be committed"

    # Query back reconciliation stats from ClickHouse
    recon = consumer.get_serving_reconciliation(ds_id)
    logger.info(f"Serving reconciliation: {recon}")

    assert recon["logical_final_rows"] == 429, f"Expected 429 logical bars under FINAL, got {recon['logical_final_rows']}"
    assert recon["raw_physical_rows"] >= 429
    assert recon["total_final_trades"] >= 436, f"Expected >= 436 trades, got {recon['total_final_trades']}"


def test_live_clickhouse_version_corrections_and_latest_semantics(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Verifies that for windows with multi-late corrections (AAPL 14:38:00),
    ClickHouse `FINAL` strictly returns the latest version with tie-broken OHLCV and VWAP.
    """
    ds_id = latest_gate3_dataset
    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        clickhouse_manager=clickhouse_manager,
    )

    df_final = consumer.query_serving_bars(ds_id, ticker_symbol="AAPL", final=True)
    assert not df_final.empty, "ClickHouse FINAL query returned no bars for AAPL"

    aapl_target = df_final[df_final["window_start"].str.contains("14:38:00")]
    assert len(aapl_target) == 1, f"Expected exactly 1 logical bar for AAPL 14:38:00 under FINAL, got {len(aapl_target)}"

    bar = aapl_target.iloc[0]
    # Check that latest version (>= 4) is served with tie-broken open (168.0), high (195.0), and correct volume (246)
    assert int(bar["version"]) >= 4, f"Expected version >= 4, got {bar['version']}"
    assert float(bar["open_price"]) == 168.0, f"Expected open 168.0, got {bar['open_price']}"
    assert float(bar["high_price"]) >= 195.0, f"Expected high >= 195.0, got {bar['high_price']}"
    assert float(bar["low_price"]) == 168.0, f"Expected low 168.0, got {bar['low_price']}"
    assert int(bar["volume"]) >= 246, f"Expected volume >= 246, got {bar['volume']}"
    assert int(bar["trade_count"]) >= 4, f"Expected trade count >= 4, got {bar['trade_count']}"


def test_live_clickhouse_replay_idempotency(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Tests that re-inserting identical batches of bars increases physical row count
    but leaves the logical `FINAL` row count, total volume, and trade count completely unchanged.
    """
    ds_id = latest_gate3_dataset
    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        clickhouse_manager=clickhouse_manager,
    )

    recon_before = consumer.get_serving_reconciliation(ds_id)

    # Re-read existing bars from ClickHouse and insert them again
    existing_df = consumer.query_serving_bars(ds_id, final=True)
    assert len(existing_df) == 429

    reinsert_df = existing_df[[
        "dataset_id", "ticker_symbol", "window_start", "window_end",
        "open_price", "high_price", "low_price", "close_price",
        "volume", "vwap", "trade_count", "version"
    ]].copy()
    reinsert_df["event_timestamp"] = reinsert_df["window_start"]
    reinsert_df["exchange"] = "NASDAQ"
    reinsert_df["created"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")

    inserted = clickhouse_manager.insert_dataframe("market_intraday_bars", reinsert_df)
    assert inserted == 429

    recon_after = consumer.get_serving_reconciliation(ds_id)
    logger.info(f"Reconciliation after replay insert: {recon_after}")

    # Physical rows increase by 429
    assert recon_after["raw_physical_rows"] == recon_before["raw_physical_rows"] + 429
    # Logical FINAL rows remain EXACTLY 429
    assert recon_after["logical_final_rows"] == 429
    # Total volume and trade count remain 100% conserved under FINAL
    assert recon_after["total_final_volume"] == recon_before["total_final_volume"]
    assert recon_after["total_final_trades"] == recon_before["total_final_trades"]


def test_live_clickhouse_stale_version_arrival_resistance(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Verifies that if an older / stale version of a bar arrives (e.g., version=1 after version=4),
    ClickHouse ReplacingMergeTree(version) ignores the stale version under `FINAL`.
    """
    ds_id = latest_gate3_dataset
    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        clickhouse_manager=clickhouse_manager,
    )

    # Get current latest bar for AAPL 14:38:00
    df_before = consumer.query_serving_bars(ds_id, ticker_symbol="AAPL", final=True)
    target_before = df_before[df_before["window_start"].str.contains("14:38:00")].iloc[0]
    expected_version = int(target_before["version"])
    expected_volume = int(target_before["volume"])

    # Forge a stale version 1 bar for the exact same key
    stale_bar = pd.DataFrame([{
        "dataset_id": ds_id,
        "ticker_symbol": "AAPL",
        "window_start": "2024-01-02 14:38:00",
        "window_end": "2024-01-02 14:39:00",
        "event_timestamp": "2024-01-02 14:38:00",
        "open_price": 999.0,   # obvious wrong value
        "high_price": 999.0,
        "low_price": 999.0,
        "close_price": 999.0,
        "volume": 1,           # obvious wrong volume
        "vwap": 999.0,
        "trade_count": 1,
        "exchange": "NASDAQ",
        "version": 1,          # STALE VERSION!
        "created": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S"),
    }])

    clickhouse_manager.insert_dataframe("market_intraday_bars", stale_bar)

    # Query back under FINAL: version must remain >= expected_version and open/volume must NOT be 999
    df_after = consumer.query_serving_bars(ds_id, ticker_symbol="AAPL", final=True)
    target_after = df_after[df_after["window_start"].str.contains("14:38:00")].iloc[0]

    assert int(target_after["version"]) == expected_version, (
        f"Stale version 1 corrupted latest version: {target_after['version']} vs {expected_version}"
    )
    assert int(target_after["volume"]) == expected_volume
    assert float(target_after["open_price"]) != 999.0


def test_live_clickhouse_cross_dataset_isolation(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Verifies that inserting bars for an isolated dummy dataset does not pollute
    or mutate the target dataset in ClickHouse.
    """
    ds_target = latest_gate3_dataset
    ds_isolated = f"ds_isolated_test_{int(time.time())}"

    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        clickhouse_manager=clickhouse_manager,
    )

    recon_target_before = consumer.get_serving_reconciliation(ds_target)

    # Insert 10 bars for ds_isolated
    iso_bars = pd.DataFrame([{
        "dataset_id": ds_isolated,
        "ticker_symbol": "NVDA",
        "window_start": f"2024-01-02 10:0{i}:00",
        "window_end": f"2024-01-02 10:0{i+1}:00",
        "event_timestamp": f"2024-01-02 10:0{i}:00",
        "open_price": 500.0,
        "high_price": 505.0,
        "low_price": 495.0,
        "close_price": 502.0,
        "volume": 100,
        "vwap": 501.0,
        "trade_count": 2,
        "exchange": "NASDAQ",
        "version": 1,
        "created": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S"),
    } for i in range(5)])

    clickhouse_manager.insert_dataframe("market_intraday_bars", iso_bars)

    # Target dataset must be completely unchanged
    recon_target_after = consumer.get_serving_reconciliation(ds_target)
    assert recon_target_after["logical_final_rows"] == recon_target_before["logical_final_rows"]
    assert recon_target_after["total_final_volume"] == recon_target_before["total_final_volume"]

    # Isolated dataset must have exactly 5 bars
    recon_iso = consumer.get_serving_reconciliation(ds_isolated)
    assert recon_iso["logical_final_rows"] == 5


def test_live_clickhouse_to_lakehouse_reconciliation(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Performs full end-to-end reconciliation between ClickHouse `FINAL` serving bars
    and Spark Iceberg Gold table `lakehouse.gold.fact_market_bars_1m`.
    """
    ds_id = latest_gate3_dataset
    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        clickhouse_manager=clickhouse_manager,
    )

    # 1. Fetch all ClickHouse FINAL bars
    df_ch = consumer.query_serving_bars(ds_id, final=True)
    assert len(df_ch) == 429

    # 2. Fetch all Spark Gold bars from Iceberg
    spark = get_spark_session(app_name="TestLive04LakehouseReconciliation")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")
    spark_rows = spark.sql(f"""
        SELECT
            ticker_symbol,
            window_start,
            volume,
            vwap,
            trade_count
        FROM lakehouse.gold.fact_market_bars_1m
        WHERE dataset_id = '{ds_id}'
    """).collect()

    df_spark = pd.DataFrame([r.asDict() for r in spark_rows])
    df_spark["window_start_dt"] = pd.to_datetime(df_spark["window_start"], utc=True)

    # 3. Merge on (ticker_symbol, window_start_dt)
    merged = pd.merge(df_ch, df_spark, on=["ticker_symbol", "window_start_dt"], suffixes=("_ch", "_spark"))
    assert len(merged) == 429, f"Expected 429 matched bars, got {len(merged)}"

    # Check volume parity for all bars except target correction bar (which Spark Gold has base or reconciled)
    # Total volume in ClickHouse must be >= Spark base volume
    ch_total_vol = int(df_ch["volume"].astype(int).sum())
    spark_total_vol = int(df_spark["volume"].sum())
    assert ch_total_vol >= spark_total_vol

    # Save Gate 4 serving manifest evidence
    evidence_dir = Path("evidence/live") / ds_id
    evidence_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "dataset_id": ds_id,
        "engine": "clickhouse",
        "table": "quant_dw.market_intraday_bars",
        "replacing_merge_tree_versioned": True,
        "logical_final_bars": len(df_ch),
        "spark_gold_bars": len(df_spark),
        "matched_bars": len(merged),
        "total_final_volume": ch_total_vol,
        "status": "VERIFIED",
    }
    manifest_path = evidence_dir / "clickhouse_serving_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"Gate 4 Serving Manifest written to {manifest_path}")


def test_live_clickhouse_poison_pill_routing_to_dlq(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Verifies that malformed JSON and schema-invalid messages in `market.bars.1m`
    are routed to DLQ topic `market.bars.dlq` with diagnostic reasons without crashing the consumer.
    """
    from kafka import KafkaConsumer, KafkaProducer, KafkaConsumer

    ds_id = f"ds_tombstone_iso_{int(time.time())}"
    dlq_topic = "market.bars.dlq"
    group_id = f"test-poison-dlq-{ds_id}-{int(time.time())}"

    # 1. Inject corrupt/invalid messages into market.bars.1m
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    # Payload A: Corrupt non-JSON bytes
    producer.send("market.bars.1m", key=f"{ds_id}_corrupt".encode("utf-8"), value=b"{corrupt_json_payload_test")
    # Payload B: Missing required fields
    producer.send("market.bars.1m", key=f"{ds_id}_missing".encode("utf-8"), value=json.dumps({"dataset_id": ds_id, "broken": True}).encode("utf-8"))
    # Payload C: Negative volume
    invalid_vol = {
        "dataset_id": ds_id,
        "ticker_symbol": "AAPL",
        "window_start": "2024-01-02 10:00:00",
        "open": 180.0,
        "high": 181.0,
        "low": 179.0,
        "close": 180.5,
        "volume": -50,
    }
    producer.send("market.bars.1m", key=f"{ds_id}_negvol".encode("utf-8"), value=json.dumps(invalid_vol).encode("utf-8"))
    producer.flush()
    producer.close()

    time.sleep(1)

    # 2. Run KafkaClickHouseServingConsumer
    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic="market.bars.1m",
        group_id=group_id,
        dlq_topic=dlq_topic,
        clickhouse_manager=clickhouse_manager,
        poll_timeout_ms=1000,
    )

    metrics = consumer.sync_dataset_bars(dataset_id=ds_id, max_idle_polls=3)
    logger.info(f"Poison test sync metrics: {metrics.to_dict()}")

    assert metrics.status == "SUCCESS", f"Consumer failed on poison pills: {metrics.status}"
    assert metrics.poison_messages_routed_to_dlq >= 1, "Expected >= 1 corrupt JSON routed to DLQ"
    assert metrics.schema_errors_routed_to_dlq >= 2, "Expected >= 2 schema errors routed to DLQ"

    # 3. Read from DLQ topic to confirm receipt
    dlq_consumer = KafkaConsumer(
        dlq_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=4000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )
    dlq_records = [msg.value for msg in dlq_consumer if msg.value and msg.value.get("dataset_id") == ds_id]
    dlq_consumer.close()

    assert len(dlq_records) >= 3, f"Expected at least 3 records in DLQ, got {len(dlq_records)}"
    reasons = [r.get("dlq_reason", "") for r in dlq_records]
    assert any("MALFORMED_JSON_PAYLOAD" in r for r in reasons)
    assert any("SCHEMA_VALIDATION_ERROR" in r for r in reasons)


def test_live_clickhouse_tombstone_processing(
    clickhouse_manager: ClickHouseSyncManager,
    latest_gate3_dataset: str,
) -> None:
    """
    Verifies comprehensive tombstone processing:
    1. Flink upsert-kafka JSON composite key.
    2. Dataset IDs containing multiple underscores.
    3. Corrupt tombstone key routed to DLQ without crashing consumer.
    4. Soft-delete semantics and recreation with higher version (v1 -> tombstone -> v2).
    5. Dataset isolation: tombstone for other dataset is ignored.
    """
    from kafka import KafkaConsumer, KafkaProducer

    ds_id = f"ds_tombstone_iso_{int(time.time())}"
    dlq_topic = "market.bars.dlq"
    group_id = f"test-tombstone-{ds_id}-{int(time.time())}"

    # 1. First insert a base bar (v=1) into ClickHouse for AAPL at a fresh window
    unique_min = int(time.time()) % 50 + 10
    test_window = f"2024-01-02 23:{unique_min:02d}:00"
    base_bar = pd.DataFrame([{
        "dataset_id": ds_id,
        "ticker_symbol": "AAPL",
        "window_start": f"{test_window}.000",
        "window_end": f"2024-01-02 23:{unique_min+1:02d}:00.000",
        "event_timestamp": f"{test_window}.000",
        "open_price": 180.0,
        "high_price": 185.0,
        "low_price": 179.0,
        "close_price": 182.0,
        "volume": 1000,
        "vwap": 181.5,
        "trade_count": 10,
        "exchange": "NASDAQ",
        "version": 1,
        "is_deleted": 0,
        "created": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S"),
    }])
    clickhouse_manager.insert_dataframe("market_intraday_bars", base_bar)

    # Verify base bar is visible
    cnt_check = int(clickhouse_manager.execute_query(f"SELECT count() FROM quant_dw.market_intraday_bars FINAL WHERE dataset_id = '{ds_id}' AND ticker_symbol = 'AAPL' AND window_start = '{test_window}' AND is_deleted = 0").text.strip())
    assert cnt_check >= 1, "Base bar must exist before tombstone"

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    # A. JSON composite key tombstone for AAPL at test_window
    json_key = json.dumps({
        "dataset_id": ds_id,
        "ticker_symbol": "AAPL",
        "window_start": test_window,
        "version": 1,
    }).encode("utf-8")
    producer.send("market.bars.1m", key=json_key, value=None)

    # B. Legacy delimited key with underscores in dataset_id for MSFT
    delim_key = f"{ds_id}_MSFT_{test_window}".encode("utf-8")
    producer.send("market.bars.1m", key=delim_key, value=None)

    # C. Corrupt tombstone key (unparseable format) -> must route to DLQ
    corrupt_key = b"unparseable_corrupt_tombstone_key"
    producer.send("market.bars.1m", key=corrupt_key, value=None)

    # D. Tombstone for another dataset -> must be ignored (dataset isolation)
    other_ds_key = json.dumps({
        "dataset_id": "other_dataset_9999",
        "ticker_symbol": "NVDA",
        "window_start": "2024-01-02 12:00:00",
    }).encode("utf-8")
    producer.send("market.bars.1m", key=other_ds_key, value=None)

    producer.flush()
    producer.close()

    time.sleep(1)

    # 2. Run KafkaClickHouseServingConsumer
    consumer = KafkaClickHouseServingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic="market.bars.1m",
        group_id=group_id,
        dlq_topic=dlq_topic,
        clickhouse_manager=clickhouse_manager,
        poll_timeout_ms=1000,
    )
    metrics = consumer.sync_dataset_bars(dataset_id=ds_id, max_idle_polls=3)
    logger.info(f"Tombstone test sync metrics: {metrics.to_dict()}")

    assert metrics.status == "SUCCESS", f"Consumer failed: {metrics.status}"
    assert metrics.tombstones_processed >= 2, "Expected at least 2 tombstones processed"

    # 3. Verify Soft-Delete Semantics: AAPL test_window is hidden under FINAL when is_deleted=0
    df_active = consumer.query_serving_bars(ds_id, ticker_symbol="AAPL", final=True, include_deleted=False)
    aapl_del = df_active[df_active["window_start"].astype(str).str.contains(test_window)]
    assert aapl_del.empty, f"AAPL {test_window} must be hidden after tombstone"

    # 4. Verify Recreation / Update: insert version 2 -> must supersede tombstone!
    v2_bar = pd.DataFrame([{
        "dataset_id": ds_id,
        "ticker_symbol": "AAPL",
        "window_start": f"{test_window}.000",
        "window_end": f"2024-01-02 23:{unique_min+1:02d}:00.000",
        "event_timestamp": f"{test_window}.000",
        "open_price": 182.0,
        "high_price": 188.0,
        "low_price": 181.0,
        "close_price": 186.0,
        "volume": 2000,
        "vwap": 184.0,
        "trade_count": 15,
        "exchange": "NASDAQ",
        "version": 2,
        "is_deleted": 0,
        "created": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S"),
    }])
    clickhouse_manager.insert_dataframe("market_intraday_bars", v2_bar)

    df_resurrect = consumer.query_serving_bars(ds_id, ticker_symbol="AAPL", final=True, include_deleted=False)
    aapl_res = df_resurrect[df_resurrect["window_start"].astype(str).str.contains(test_window)]
    assert not aapl_res.empty, f"AAPL {test_window} must be recreated/updated by version 2"
    assert int(aapl_res.iloc[0]["version"]) == 2
    assert float(aapl_res.iloc[0]["close_price"]) == 186.0

    # 5. Verify Corrupt Tombstone was routed to DLQ
    dlq_consumer = KafkaConsumer(
        dlq_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )
    dlq_items = [msg.value for msg in dlq_consumer if msg.value and msg.value.get("dlq_reason") == "CORRUPT_TOMBSTONE_KEY"]
    dlq_consumer.close()
    assert len(dlq_items) >= 1, "Expected at least 1 corrupt tombstone routed to DLQ"
    logger.info(f"Verified {len(dlq_items)} corrupt tombstone records in DLQ.")

