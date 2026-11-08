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

logger = logging.getLogger("DP2_Silver_Gold")

default_args = {
    "owner": "quant_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def task_dedup_and_evolve(**kwargs) -> dict:
    """Execute deduplication and schema evolution from bronze to silver."""
    source_dir = os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
    target_dir = os.path.join(source_dir, "stg_market_ohlcv")
    engine = os.environ.get("ENGINE", "auto")

    from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
    logger.info(f"Running Deduplication & Schema Evolution ({engine}) from {source_dir} to {target_dir}")
    job = DeduplicationAndSchemaEvolutionJob(source_dir=source_dir, target_dir=target_dir)
    return job.run(engine=engine)


def task_scd2_dimension(**kwargs) -> dict:
    """Build SCD Type 2 dimension table for dim_companies."""
    source_dir = os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
    source_file = os.path.join(source_dir, "dim_companies", "dim_companies.parquet")
    target_dir = os.path.join(source_dir, "gold_dim_companies")
    engine = os.environ.get("ENGINE", "auto")

    from quant_edai.processing.spark.scd2_builder import SCD2DimensionBuilder
    logger.info(f"Running SCD2 Dimension Builder ({engine}) from {source_file} to {target_dir}")
    builder = SCD2DimensionBuilder(source_file=source_file, target_dir=target_dir)
    return builder.run(engine=engine)


def task_iceberg_compaction(**kwargs) -> dict:
    """Execute Iceberg binpack compaction on raw_market_ohlcv."""
    source_dir = os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
    table_dir = os.path.join(source_dir, "raw_market_ohlcv")
    compacted_dir = os.path.join(source_dir, "compacted_market_ohlcv")
    engine = os.environ.get("ENGINE", "auto")

    from quant_edai.processing.spark.iceberg_compaction import IcebergCompactionJob
    logger.info(f"Running Iceberg Binpack Compaction ({engine}) on {table_dir}")
    job = IcebergCompactionJob(table_dir=table_dir, compacted_dir=compacted_dir)
    return job.run(engine=engine)


def task_quality_gate_gold(**kwargs) -> dict:
    """Run Great Expectations quality gate for Gold market data."""
    source_dir = os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
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

        dedup_task >> [scd2_task, compaction_task] >> quality_task

except ImportError:
    logger.info("Airflow package not installed in environment; DAG definition available in Airflow container.")
