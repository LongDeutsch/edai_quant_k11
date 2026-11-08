"""DP1: Ingestion & Quality Gate DAG.

Ingests raw market data from MinIO external-source into Lakehouse Bronze tables
and runs Great Expectations validation suite to enforce Bronze quality gate.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path

from quant_paths import repo_path, repo_root

logger = logging.getLogger("DP1_Raw_Ingestion")

default_args = {
    "owner": "quant_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def task_ingest_external_source(**kwargs) -> dict:
    """Ingest external raw market files into lakehouse bronze."""
    source_dir = os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    engine = os.environ.get("ENGINE", "auto")

    from quant_edai.ingestion.s3_uploader import sync_lakehouse_data
    logger.info(f"Running Ingestion from {source_dir} (MinIO: {endpoint}, Engine: {engine})")
    stats = sync_lakehouse_data(source_dir=source_dir, endpoint=endpoint)
    
    evidence_dir = Path(repo_path("evidence", "phase3"))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    with open(evidence_dir / "dp1_ingestion_metrics.json", "w") as f:
        json.dump(stats, f, indent=2)
    return stats


def task_quality_gate_raw(**kwargs) -> dict:
    """Validate bronze ingestion against Great Expectations suite."""
    source_dir = os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
    raw_path = Path(source_dir) / "raw_market_ohlcv"
    suite_path = repo_path("configs", "quality", "suite_raw_market_data.json")
    doc_out = repo_path("evidence", "phase3", "dp1_gx_report.md")

    from quant_edai.quality.gx_runner import run_gx_checkpoint
    logger.info(f"Running Bronze Quality Gate against {suite_path} (repo={repo_root()})")
    results = run_gx_checkpoint(data_path=str(raw_path), suite_path=suite_path, doc_out_path=doc_out)
    if not results.get("success", False):
        raise ValueError(f"Bronze Quality Gate failed: {results.get('unsuccessful_expectations')} failed expectations")
    return results


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="dag_dp1_raw_ingestion",
        default_args=default_args,
        description="DP1: Ingestion & Quality Gate (External Source -> Bronze -> Great Expectations)",
        schedule_interval="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["phase3", "bronze", "ingestion", "gx"],
    ) as dag:
        ingest_task = PythonOperator(
            task_id="ingest_external_source",
            python_callable=task_ingest_external_source,
        )

        quality_task = PythonOperator(
            task_id="quality_gate_bronze",
            python_callable=task_quality_gate_raw,
        )

        ingest_task >> quality_task

except ImportError:
    logger.info("Airflow package not installed in environment; DAG definition available in Airflow container.")
