"""DP2: Silver & Gold Lakehouse Processing DAG.

Executes:
1. PySpark Deduplication & Schema Evolution (Bronze -> Silver stg_market_ohlcv)
2. SCD Type 2 Dimension Builder for dim_companies (Silver -> Gold gold_dim_companies)
3. Lakehouse Iceberg binpack compaction
4. Great Expectations Quality Gate for Gold Market Data
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path

from quant_paths import repo_path, repo_root

from typing import Any

logger = logging.getLogger("DP2_Silver_Gold")

default_args = {
    "owner": "quant_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}


def get_dag_param(kwargs: dict, key: str, default: Any = None) -> Any:
    """Extract runtime parameter prioritizing dag_run.conf over os.environ."""
    dag_run = kwargs.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None) and key in dag_run.conf:
        return dag_run.conf[key]
    return os.environ.get(key, default)


def _dataset_root(kwargs: dict | None = None) -> str:
    from quant_edai.common.dataset_paths import resolve_dataset_root

    kw = kwargs or {}
    out = get_dag_param(kw, "OUT_DIR", "/tmp/lakehouse_source")
    ds = get_dag_param(kw, "DATASET_ID")
    try:
        return str(resolve_dataset_root(out, ds, allow_legacy=True))
    except Exception:
        return out


def task_dedup_and_evolve(**kwargs) -> dict:
    """Execute deduplication and schema evolution from bronze to silver."""
    source_dir = _dataset_root(kwargs)
    target_dir = os.path.join(source_dir, "stg_market_ohlcv")
    engine = get_dag_param(kwargs, "SPARK_ENGINE") or get_dag_param(kwargs, "ENGINE", "auto")
    dataset_id = get_dag_param(kwargs, "DATASET_ID")

    from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
    logger.info(f"Running Deduplication & Schema Evolution ({engine}) from {source_dir} to {target_dir} (dataset_id={dataset_id})")
    job = DeduplicationAndSchemaEvolutionJob(source_dir=source_dir, target_dir=target_dir, dataset_id=dataset_id)
    stats = job.run(engine=engine)
    return stats


def task_resample_gold_bars(**kwargs) -> dict:
    """Resample Silver ticks into Gold 1m and Daily bars."""
    source_dir = _dataset_root(kwargs)
    source_ticks = os.path.join(source_dir, "stg_market_ticks")
    gold_1m = os.path.join(source_dir, "gold_market_bars_1m")
    gold_daily = os.path.join(source_dir, "gold_market_bars_daily")
    engine = get_dag_param(kwargs, "SPARK_ENGINE") or get_dag_param(kwargs, "ENGINE", "auto")
    dataset_id = get_dag_param(kwargs, "DATASET_ID")

    from quant_edai.processing.spark.resample_job import SparkResampleJob
    logger.info(f"Running Spark Resampler ({engine}) from {source_ticks} to {gold_1m} and {gold_daily} (dataset_id={dataset_id})")
    job = SparkResampleJob(source_ticks_dir=source_ticks, gold_1m_dir=gold_1m, gold_daily_dir=gold_daily, dataset_id=dataset_id)
    stats = job.run(engine=engine)

    # Capture Iceberg snapshot IDs for lineage and audit
    try:
        from pyiceberg.catalog import load_catalog
        rest_url = os.environ.get("ICEBERG_REST_URL", "http://iceberg-rest:8181")
        minio_url = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        cat = load_catalog("rest", **{
            "type": "rest",
            "uri": rest_url,
            "s3.endpoint": minio_url,
            "s3.access-key-id": "quant_admin",
            "s3.secret-access-key": "quant_minio_password",
        })
        snap_map = {}
        for tbl_name in ["silver.stg_market_ticks", "gold.fact_daily_ohlcv", "gold.fact_market_bars_1m"]:
            try:
                t = cat.load_table(tbl_name)
                curr = t.current_snapshot()
                if curr:
                    snap_map[tbl_name] = curr.snapshot_id
            except Exception:
                pass
        stats["iceberg_snapshots"] = snap_map
        logger.info(f"Recorded Iceberg snapshot IDs: {snap_map}")
    except Exception as e:
        logger.warning(f"Could not capture Iceberg snapshots via pyiceberg: {e}")

    return stats


def task_scd2_dimension(**kwargs) -> dict:
    """Build SCD Type 2 dimension table for dim_companies."""
    source_dir = _dataset_root(kwargs)
    source_file = os.path.join(source_dir, "dim_companies", "dim_companies.parquet")
    target_dir = os.path.join(source_dir, "gold_dim_companies")
    engine = get_dag_param(kwargs, "SPARK_ENGINE") or get_dag_param(kwargs, "ENGINE", "auto")

    from quant_edai.processing.spark.scd2_builder import SCD2DimensionBuilder
    logger.info(f"Running SCD2 Dimension Builder ({engine}) from {source_file} to {target_dir}")
    builder = SCD2DimensionBuilder(source_file=source_file, target_dir=target_dir)
    return builder.run(engine=engine)


def task_iceberg_compaction(**kwargs) -> dict:
    """Execute Iceberg binpack compaction on raw_market_ohlcv."""
    source_dir = _dataset_root(kwargs)
    table_dir = os.path.join(source_dir, "raw_market_ohlcv")
    compacted_dir = os.path.join(source_dir, "compacted_market_ohlcv")
    engine = get_dag_param(kwargs, "SPARK_ENGINE") or get_dag_param(kwargs, "ENGINE", "auto")

    from quant_edai.processing.spark.iceberg_compaction import IcebergCompactionJob
    logger.info(f"Running Iceberg Binpack Compaction ({engine}) on {table_dir}")
    job = IcebergCompactionJob(table_dir=table_dir, compacted_dir=compacted_dir)
    return job.run(engine=engine)


def task_quality_gate_gold(**kwargs) -> dict:
    """Run Great Expectations quality gate for Gold market data (prefer Gold bars)."""
    source_dir = _dataset_root(kwargs)
    data_path = Path(source_dir) / "gold_market_bars_daily"
    if not data_path.exists():
        data_path = Path(source_dir) / "stg_market_ohlcv"
    if not data_path.exists():
        data_path = Path(source_dir) / "raw_market_ohlcv"

    suite_path = repo_path("configs", "quality", "suite_gold_market_data.json")
    doc_out = repo_path("evidence", "phase3", "dp2_gx_report.md")

    from quant_edai.quality.gx_runner import run_gx_checkpoint
    logger.info(f"Running Gold Quality Gate against {suite_path} (repo={repo_root()})")
    results = run_gx_checkpoint(data_path=str(data_path), suite_path=suite_path, doc_out_path=doc_out)

    # Save consolidated metrics
    evidence_dir = Path(repo_path("evidence", "phase3"))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    with open(evidence_dir / "dp2_silver_gold_metrics.json", "w") as f:
        json.dump(results, f, indent=2)

    if not results.get("success", False):
        raise ValueError(f"Gold Quality Gate failed: {results.get('unsuccessful_expectations')} failed expectations")
    return results


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="dag_dp2_silver_gold",
        default_args=default_args,
        description="DP2: Silver & Gold Lakehouse Processing (Dedup -> SCD2 -> Compaction -> Quality Gate)",
        schedule_interval="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["phase3", "silver", "gold", "scd2", "compaction", "gx"],
    ) as dag:
        dedup_task = PythonOperator(
            task_id="dedup_and_evolve_schema",
            python_callable=task_dedup_and_evolve,
        )

        resample_task = PythonOperator(
            task_id="resample_gold_bars",
            python_callable=task_resample_gold_bars,
        )

        scd2_task = PythonOperator(
            task_id="scd2_dimension_builder",
            python_callable=task_scd2_dimension,
        )

        compaction_task = PythonOperator(
            task_id="iceberg_binpack_compaction",
            python_callable=task_iceberg_compaction,
        )

        quality_task = PythonOperator(
            task_id="quality_gate_gold",
            python_callable=task_quality_gate_gold,
        )

        # DP2: dedup → resample Gold bars + SCD2 + compaction → GX on Gold/Silver
        dedup_task >> [resample_task, scd2_task, compaction_task] >> quality_task

except ImportError:
    logger.info("Airflow package not installed in environment; DAG definition available in Airflow container.")
