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


def task_ingest_external_source(**kwargs) -> dict:
    """Ingest external raw market files into lakehouse bronze (MinIO preferred)."""
    source_dir = _dataset_root(kwargs)
    endpoint = get_dag_param(kwargs, "MINIO_ENDPOINT", "http://minio:9000")
    engine = get_dag_param(kwargs, "SPARK_ENGINE") or get_dag_param(kwargs, "ENGINE", "auto")
    mode = get_dag_param(kwargs, "MODE", "smoke")
    dataset_id = get_dag_param(kwargs, "DATASET_ID")

    from quant_edai.ingestion.s3_uploader import S3SourceUploader
    logger.info(f"Running Ingestion from {source_dir} (MinIO: {endpoint}, Engine: {engine}, MODE={mode}, DS={dataset_id})")
    uploader = S3SourceUploader(source_dir=source_dir, endpoint_url=endpoint, mode=mode, dataset_id=dataset_id)
    stats = uploader.upload_dataset()
    if stats.get("status") in {"FAILED", "ERROR"}:
        raise RuntimeError(stats.get("detail") or stats.get("message") or "ingestion failed")

    evidence_dir = Path(repo_path("evidence", "phase3"))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    with open(evidence_dir / "dp1_ingestion_metrics.json", "w") as f:
        json.dump(stats, f, indent=2)
    return stats


def task_quality_gate_raw(**kwargs) -> dict:
    """Validate bronze ingestion against Great Expectations suite."""
    source_dir = _dataset_root(kwargs)
    raw_ticks = Path(source_dir) / "raw_market_ticks"
    raw_ohlcv = Path(source_dir) / "raw_market_ohlcv"
    raw_path = raw_ohlcv if raw_ohlcv.exists() else raw_ticks
    suite_path = repo_path("configs", "quality", "suite_raw_market_data.json")
    doc_out = repo_path("evidence", "phase3", "dp1_gx_report.md")

    from quant_edai.quality.gx_runner import run_gx_checkpoint
    logger.info(f"Running Bronze Quality Gate against {suite_path} on {raw_path} (repo={repo_root()})")
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
