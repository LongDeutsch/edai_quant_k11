"""Gate 2 Live Integration Test: Real PySpark S3A MinIO -> Iceberg REST Catalog.

Verifies:
1. Real PySpark reads Bronze from MinIO via S3A.
2. Real PySpark deduplicates Bronze into Silver Iceberg table (lakehouse.silver.stg_market_ticks).
3. SparkResampleJob strictly reads from Silver Iceberg table (lakehouse.silver.stg_market_ticks),
   NOT raw Bronze ticks.
4. Total trade_count in Gold 1m and Daily equals Silver deduplicated count (NOT Bronze raw count).
5. Mathematical volume and trade_count conservation across 1m and daily bars.
6. Bar-by-bar reconciliation: OHLC, VWAP, Volume, trade_count against Silver ticks.
7. 100% Rerun Idempotency (no row doubling or volume amplification).
8. Cross-Dataset Isolation (processing ds_beta does not mutate ds_alpha).
9. Bronze SHA-256 Immutability and fail-on-overwrite verification.
10. Iceberg snapshots verified directly from REST Catalog.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
import pytest
import pyspark.sql.functions as F

from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.ingestion.s3_uploader import S3SourceUploader
from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
from quant_edai.processing.spark.resample_job import SparkResampleJob
from quant_edai.processing.spark.spark_session import get_spark_session


@pytest.fixture(scope="module")
def dual_datasets(tmp_path_factory):
    """Generates and stages two separate immutable dataset runs to MinIO."""
    lake_root = tmp_path_factory.mktemp("lakehouse")
    run_ts = int(time.time())
    ds_a_id = f"ds_gate2_alpha_{run_ts}"
    ds_b_id = f"ds_gate2_beta_{run_ts}"

    # 1. Dataset Alpha (scale=0.003 to include multi-tick windows and duplicates)
    gen_a = QuantDataGenerator(
        mode="smoke",
        scale=0.003,
        seed=101,
        out_dir=str(lake_root),
        dataset_id=ds_a_id,
        start_date="2024-01-02",
        end_date="2024-01-05",
    )
    res_a = gen_a.run()

    # Upload Alpha to MinIO
    up_a = S3SourceUploader(
        endpoint_url="http://localhost:9000",
        source_dir=str(lake_root),
        dataset_id=ds_a_id,
        mode="integration",
    )
    status_a = up_a.upload_dataset()
    assert status_a.get("status") in {"SUCCESS", "STAGED_LOCAL"}, f"MinIO upload failed: {status_a}"

    # 2. Dataset Beta
    gen_b = QuantDataGenerator(
        mode="smoke",
        scale=0.003,
        seed=202,
        out_dir=str(lake_root),
        dataset_id=ds_b_id,
        start_date="2024-01-02",
        end_date="2024-01-05",
    )
    res_b = gen_b.run()

    # Upload Beta to MinIO
    up_b = S3SourceUploader(
        endpoint_url="http://localhost:9000",
        source_dir=str(lake_root),
        dataset_id=ds_b_id,
        mode="integration",
    )
    status_b = up_b.upload_dataset()
    assert status_b.get("status") in {"SUCCESS", "STAGED_LOCAL"}, f"MinIO upload failed: {status_b}"

    return {
        "lake_root": lake_root,
        "ds_a": ds_a_id,
        "ds_b": ds_b_id,
        "dir_a": lake_root / "runs" / ds_a_id,
        "dir_b": lake_root / "runs" / ds_b_id,
        "s3a_bronze_a": f"s3a://lakehouse/bronze/runs/{ds_a_id}",
        "s3a_bronze_b": f"s3a://lakehouse/bronze/runs/{ds_b_id}",
    }


def test_spark_dedup_and_resample_pipeline(dual_datasets) -> None:
    """Verifies that Dedup and Resample jobs produce mathematically valid Silver and Gold Iceberg tables."""
    ds_a = dual_datasets["ds_a"]
    s3a_bronze_a = dual_datasets["s3a_bronze_a"]
    dir_a = dual_datasets["dir_a"]

    # 1. Run PySpark Dedup reading directly from MinIO Bronze S3A
    dedup_job = DeduplicationAndSchemaEvolutionJob(
        source_dir=s3a_bronze_a,
        target_dir=str(dir_a / "stg_market_ohlcv"),
        iceberg_table="lakehouse.silver.stg_market_ticks",
        dataset_id=ds_a,
    )
    dedup_stats = dedup_job.run(engine="spark")
    assert dedup_stats["deduplicated_rows"] > 0
    assert dedup_stats["duplicates_removed"] > 0, "Expected duplicates to be eliminated by Dedup job"
    assert len(dedup_stats.get("iceberg_snapshots", [])) > 0, "Silver Iceberg table missing snapshot"

    # 2. Run PySpark Resample reading exclusively from Silver Iceberg table (stg_market_ticks)
    resample_job = SparkResampleJob(
        source_ticks_dir="lakehouse.silver.stg_market_ticks",
        gold_1m_dir=str(dir_a / "gold_market_bars_1m"),
        gold_daily_dir=str(dir_a / "gold_market_bars_daily"),
        iceberg_1m_table="lakehouse.gold.fact_market_bars_1m",
        iceberg_daily_table="lakehouse.gold.fact_daily_ohlcv",
        dataset_id=ds_a,
    )
    resample_stats = resample_job.run(engine="spark")

    # Critical Assertion: Resample MUST process exactly the Silver deduplicated count, NOT Bronze raw count
    assert resample_stats["total_ticks_processed"] == dedup_stats["deduplicated_rows"], (
        f"Lineage violation: Resample processed {resample_stats['total_ticks_processed']} ticks, "
        f"expected {dedup_stats['deduplicated_rows']} (Silver count)"
    )
    assert resample_stats["bars_1m_generated"] > 0
    assert resample_stats["bars_daily_generated"] > 0
    assert resample_stats["volume_conserved"], "Volume not conserved between 1m and daily bars"

    # 3. Query back from Iceberg REST catalog directly using PySpark
    spark = get_spark_session(app_name="verify_gate2_iceberg", master="local[*]")
    spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_daily_ohlcv")

    df_silver = spark.sql(f"SELECT * FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_a}'")
    silver_count = df_silver.count()
    assert silver_count == dedup_stats["deduplicated_rows"]

    df_gold_1m = spark.sql(f"SELECT * FROM lakehouse.gold.fact_market_bars_1m WHERE dataset_id = '{ds_a}'")
    gold_1m_count = df_gold_1m.count()
    assert gold_1m_count == resample_stats["bars_1m_generated"]

    df_gold_daily = spark.sql(f"SELECT * FROM lakehouse.gold.fact_daily_ohlcv WHERE dataset_id = '{ds_a}'")
    gold_daily_count = df_gold_daily.count()
    assert gold_daily_count == resample_stats["bars_daily_generated"]

    # Total trade_count in Gold must equal Silver deduplicated count (436, not 444)
    trades_1m = df_gold_1m.agg(F.sum("trade_count")).collect()[0][0]
    trades_daily = df_gold_daily.agg(F.sum("trade_count")).collect()[0][0]
    assert trades_1m == silver_count, f"Gold 1m trade_count ({trades_1m}) does not match Silver ({silver_count})"
    assert trades_daily == silver_count, f"Gold daily trade_count ({trades_daily}) does not match Silver ({silver_count})"

    # Verify Iceberg Snapshots metadata
    snap_silver = spark.sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.silver.stg_market_ticks.snapshots ORDER BY committed_at DESC").collect()
    snap_1m = spark.sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.gold.fact_market_bars_1m.snapshots ORDER BY committed_at DESC").collect()
    snap_daily = spark.sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.gold.fact_daily_ohlcv.snapshots ORDER BY committed_at DESC").collect()

    assert len(snap_silver) > 0
    assert len(snap_1m) > 0
    assert len(snap_daily) > 0

    # Write evidence
    evidence_dir = Path("evidence/live") / ds_a
    evidence_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "dataset_id": ds_a,
        "engine": "spark",
        "silver_ticks_rows": silver_count,
        "gold_1m_rows": gold_1m_count,
        "gold_daily_rows": gold_daily_count,
        "total_trade_count_1m": trades_1m,
        "total_trade_count_daily": trades_daily,
        "volume_conserved": True,
        "latest_silver_snapshot": str(snap_silver[0]["snapshot_id"]),
        "latest_gold_1m_snapshot": str(snap_1m[0]["snapshot_id"]),
        "latest_gold_daily_snapshot": str(snap_daily[0]["snapshot_id"]),
    }
    with open(evidence_dir / "iceberg_tables_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def test_live_bar_by_bar_reconciliation(dual_datasets) -> None:
    """Reconciles sample 1m bars from Gold directly against underlying Silver ticks."""
    ds_a = dual_datasets["ds_a"]
    spark = get_spark_session(app_name="verify_gate2_reconciliation", master="local[*]")
    spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")

    df_silver = spark.sql(f"SELECT * FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_a}'")
    df_gold_1m = spark.sql(f"SELECT * FROM lakehouse.gold.fact_market_bars_1m WHERE dataset_id = '{ds_a}'")

    # Sample bars to verify
    sample_bars = df_gold_1m.limit(10).collect()
    assert len(sample_bars) > 0

    reconciled_count = 0
    for b in sample_bars:
        ticker = b["ticker_symbol"]
        w_start = b["window_start"]
        w_end = b["window_end"]

        # Retrieve ticks in window
        ticks = df_silver.filter(
            (F.col("ticker_symbol") == ticker) &
            (F.col("event_timestamp") >= F.to_timestamp(F.lit(w_start))) &
            (F.col("event_timestamp") < F.to_timestamp(F.lit(w_end)))
        ).orderBy(F.col("event_timestamp").asc(), F.col("trade_id").asc()).collect()

        assert len(ticks) > 0, f"No matching silver ticks for bar {b['bar_id']}"

        # OHLCV & VWAP calculations
        expected_open = ticks[0]["price"]
        expected_close = ticks[-1]["price"]
        expected_high = max(t["price"] for t in ticks)
        expected_low = min(t["price"] for t in ticks)
        expected_vol = sum(t["size"] for t in ticks)
        expected_vwap = round(sum(t["price"] * t["size"] for t in ticks) / max(1, expected_vol), 4)
        expected_trades = len(ticks)

        assert abs(b["open_price"] - expected_open) < 1e-4, f"Open mismatch for {b['bar_id']}"
        assert abs(b["close_price"] - expected_close) < 1e-4, f"Close mismatch for {b['bar_id']}"
        assert abs(b["high_price"] - expected_high) < 1e-4, f"High mismatch for {b['bar_id']}"
        assert abs(b["low_price"] - expected_low) < 1e-4, f"Low mismatch for {b['bar_id']}"
        assert b["volume"] == expected_vol, f"Volume mismatch for {b['bar_id']}"
        assert abs(b["vwap"] - expected_vwap) < 1e-2, f"VWAP mismatch for {b['bar_id']}"
        assert b["trade_count"] == expected_trades, f"Trade count mismatch for {b['bar_id']}"
        reconciled_count += 1

    assert reconciled_count == len(sample_bars)


def test_rerun_idempotency_no_row_doubling(dual_datasets) -> None:
    """Verifies that re-running Dedup and Resample on the same dataset NEVER doubles rows or volume."""
    ds_a = dual_datasets["ds_a"]
    s3a_bronze_a = dual_datasets["s3a_bronze_a"]
    dir_a = dual_datasets["dir_a"]
    spark = get_spark_session(app_name="verify_gate2_idempotency", master="local[*]")

    # Run 1: pipeline execution
    dedup_job = DeduplicationAndSchemaEvolutionJob(
        source_dir=s3a_bronze_a,
        target_dir=str(dir_a / "stg_market_ohlcv"),
        iceberg_table="lakehouse.silver.stg_market_ticks",
        dataset_id=ds_a,
    )
    dedup_job.run(engine="spark")

    resample_job = SparkResampleJob(
        source_ticks_dir="lakehouse.silver.stg_market_ticks",
        gold_1m_dir=str(dir_a / "gold_market_bars_1m"),
        gold_daily_dir=str(dir_a / "gold_market_bars_daily"),
        iceberg_1m_table="lakehouse.gold.fact_market_bars_1m",
        iceberg_daily_table="lakehouse.gold.fact_daily_ohlcv",
        dataset_id=ds_a,
    )
    resample_job.run(engine="spark")

    spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_daily_ohlcv")

    # Counts after Run 1
    run1_silver = spark.sql(f"SELECT COUNT(*) as cnt, SUM(size) as vol FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_a}'").collect()[0]
    run1_1m = spark.sql(f"SELECT COUNT(*) as cnt, SUM(volume) as vol FROM lakehouse.gold.fact_market_bars_1m WHERE dataset_id = '{ds_a}'").collect()[0]
    run1_daily = spark.sql(f"SELECT COUNT(*) as cnt, SUM(volume) as vol FROM lakehouse.gold.fact_daily_ohlcv WHERE dataset_id = '{ds_a}'").collect()[0]

    assert run1_silver["cnt"] > 0
    assert run1_1m["cnt"] > 0
    assert run1_daily["cnt"] > 0

    # Run 2: identical rerun
    dedup_job.run(engine="spark")
    resample_job.run(engine="spark")

    spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_market_bars_1m")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_daily_ohlcv")

    # Counts after Run 2
    run2_silver = spark.sql(f"SELECT COUNT(*) as cnt, SUM(size) as vol FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_a}'").collect()[0]
    run2_1m = spark.sql(f"SELECT COUNT(*) as cnt, SUM(volume) as vol FROM lakehouse.gold.fact_market_bars_1m WHERE dataset_id = '{ds_a}'").collect()[0]
    run2_daily = spark.sql(f"SELECT COUNT(*) as cnt, SUM(volume) as vol FROM lakehouse.gold.fact_daily_ohlcv WHERE dataset_id = '{ds_a}'").collect()[0]

    assert run2_silver["cnt"] == run1_silver["cnt"], f"Silver count changed on rerun: {run2_silver['cnt']} vs {run1_silver['cnt']}"
    assert run2_silver["vol"] == run1_silver["vol"], f"Silver volume changed on rerun: {run2_silver['vol']} vs {run1_silver['vol']}"

    assert run2_1m["cnt"] == run1_1m["cnt"], f"Gold 1m count changed on rerun: {run2_1m['cnt']} vs {run1_1m['cnt']}"
    assert run2_1m["vol"] == run1_1m["vol"], f"Gold 1m volume changed on rerun: {run2_1m['vol']} vs {run1_1m['vol']}"

    assert run2_daily["cnt"] == run1_daily["cnt"], f"Gold daily count changed on rerun: {run2_daily['cnt']} vs {run1_daily['cnt']}"
    assert run2_daily["vol"] == run1_daily["vol"], f"Gold daily volume changed on rerun: {run2_daily['vol']} vs {run1_daily['vol']}"


def test_cross_dataset_isolation(dual_datasets) -> None:
    """Verifies that running pipeline on Dataset B does not mutate or delete Dataset A's data."""
    ds_a = dual_datasets["ds_a"]
    ds_b = dual_datasets["ds_b"]
    s3a_bronze_b = dual_datasets["s3a_bronze_b"]
    dir_b = dual_datasets["dir_b"]
    spark = get_spark_session(app_name="verify_gate2_isolation", master="local[*]")
    spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_daily_ohlcv")

    # Snapshot ds_a stats prior to ds_b execution
    ds_a_silver_before = spark.sql(f"SELECT COUNT(*) as cnt, SUM(size) as vol FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_a}'").collect()[0]
    ds_a_daily_before = spark.sql(f"SELECT COUNT(*) as cnt, SUM(volume) as vol FROM lakehouse.gold.fact_daily_ohlcv WHERE dataset_id = '{ds_a}'").collect()[0]

    # Process Dataset B
    dedup_b = DeduplicationAndSchemaEvolutionJob(
        source_dir=s3a_bronze_b,
        target_dir=str(dir_b / "stg_market_ohlcv"),
        iceberg_table="lakehouse.silver.stg_market_ticks",
        dataset_id=ds_b,
    )
    dedup_b.run(engine="spark")

    resample_b = SparkResampleJob(
        source_ticks_dir="lakehouse.silver.stg_market_ticks",
        gold_1m_dir=str(dir_b / "gold_market_bars_1m"),
        gold_daily_dir=str(dir_b / "gold_market_bars_daily"),
        iceberg_1m_table="lakehouse.gold.fact_market_bars_1m",
        iceberg_daily_table="lakehouse.gold.fact_daily_ohlcv",
        dataset_id=ds_b,
    )
    resample_b.run(engine="spark")

    spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
    spark.sql("REFRESH TABLE lakehouse.gold.fact_daily_ohlcv")

    # Assert Dataset B exists
    ds_b_silver = spark.sql(f"SELECT COUNT(*) as cnt FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_b}'").collect()[0]["cnt"]
    assert ds_b_silver > 0, "Dataset B silver data missing"

    # Assert Dataset A remained 100% UNTOUCHED
    ds_a_silver_after = spark.sql(f"SELECT COUNT(*) as cnt, SUM(size) as vol FROM lakehouse.silver.stg_market_ticks WHERE dataset_id = '{ds_a}'").collect()[0]
    ds_a_daily_after = spark.sql(f"SELECT COUNT(*) as cnt, SUM(volume) as vol FROM lakehouse.gold.fact_daily_ohlcv WHERE dataset_id = '{ds_a}'").collect()[0]

    assert ds_a_silver_after["cnt"] == ds_a_silver_before["cnt"], "Dataset A silver rows were altered by Dataset B!"
    assert ds_a_silver_after["vol"] == ds_a_silver_before["vol"], "Dataset A silver volume was altered by Dataset B!"
    assert ds_a_daily_after["cnt"] == ds_a_daily_before["cnt"], "Dataset A daily bars were altered by Dataset B!"
    assert ds_a_daily_after["vol"] == ds_a_daily_before["vol"], "Dataset A daily volume was altered by Dataset B!"


def test_bronze_immutability_and_fail_on_overwrite(dual_datasets) -> None:
    """Verifies Bronze SHA-256 immutability and enforces fail-on-overwrite."""
    dir_a = dual_datasets["dir_a"]
    ds_a = dual_datasets["ds_a"]
    raw_ticks = dir_a / "raw_market_ticks"
    assert raw_ticks.exists()

    checksums = {}
    for p in raw_ticks.glob("**/*.parquet"):
        if p.is_file():
            hasher = hashlib.sha256()
            with open(p, "rb") as f:
                hasher.update(f.read())
            checksums[str(p.relative_to(raw_ticks))] = hasher.hexdigest()

    assert len(checksums) > 0, "No raw tick parquet files found to verify SHA-256"

    # Test fail-on-overwrite: attempting to re-generate with existing dataset_id in integration mode
    with pytest.raises(FileExistsError, match="strictly immutable"):
        gen_dup = QuantDataGenerator(
            mode="integration",
            scale=0.003,
            seed=101,
            out_dir=str(dual_datasets["lake_root"]),
            dataset_id=ds_a,
        )
        gen_dup.run()

    # Verify existing Bronze Parquet files remain 100% byte-for-byte identical (unmutated)
    for p in raw_ticks.glob("**/*.parquet"):
        if p.is_file() and str(p.relative_to(raw_ticks)) in checksums:
            hasher = hashlib.sha256()
            with open(p, "rb") as f:
                hasher.update(f.read())
            assert hasher.hexdigest() == checksums[str(p.relative_to(raw_ticks))], f"Bronze immutability violated: {p.relative_to(raw_ticks)} changed!"

    evidence_dir = Path("evidence/live") / ds_a
    evidence_dir.mkdir(parents=True, exist_ok=True)
    with open(evidence_dir / "bronze_checksums_manifest.json", "w") as f:
        json.dump({"dataset_id": ds_a, "sha256_checksums": checksums, "immutable": True}, f, indent=2)
