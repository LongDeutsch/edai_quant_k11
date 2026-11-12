"""DP3: Quant Technical Features, Lineage & Serving DAG.

Executes:
1. Technical Indicators calculation (SMA20/50, RSI14, MACD, Volatility, Intraday Return)
2. Great Expectations Quality Gate for Features (M1-34 audit columns)
3. DataHub lineage emission (Bronze -> Silver -> Gold -> ClickHouse)
4. ClickHouse OLAP serving synchronization
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path

from quant_paths import repo_path, repo_root

from typing import Any

logger = logging.getLogger("DP3_Features")

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


def task_calculate_features(**kwargs) -> dict:
    """Compute offline technical indicators from Gold daily bars or Silver stg table."""
    source_dir = Path(_dataset_root(kwargs))
    gold_daily_path = source_dir / "gold_market_bars_daily"
    silver_path = source_dir / "stg_market_ohlcv"
    bronze_path = source_dir / "raw_market_ohlcv"

    if gold_daily_path.exists():
        data_path = gold_daily_path
        source_zone = "gold_daily"
    elif silver_path.exists():
        data_path = silver_path
        source_zone = "silver"
    else:
        data_path = bronze_path
        source_zone = "bronze_fallback"
        logger.warning(
            "Gold daily and Silver stg missing; DP3 falling back to bronze raw_market_ohlcv."
        )

    out_dir = Path(repo_path("evidence", "phase3"))
    out_dir.mkdir(parents=True, exist_ok=True)
    target_path = source_dir / "gold_features" / "feat_ticker_technical_indicators.parquet"

    from quant_edai.processing.features.feature_calculator import run_feature_calculation
    engine = get_dag_param(kwargs, "SPARK_ENGINE") or get_dag_param(kwargs, "ENGINE", "auto")
    logger.info(
        "Computing technical indicators from %s (%s, engine=%s) -> %s",
        data_path,
        source_zone,
        engine,
        target_path,
    )
    metrics = run_feature_calculation(
        source_path=str(data_path),
        target_path=str(target_path),
        engine=engine,
        out_file=repo_path("evidence", "phase3", "feature_computation_metrics.json"),
    )
    metrics["source_zone"] = source_zone
    metrics["source_path"] = str(data_path)
    return metrics


def task_quality_gate_features(**kwargs) -> dict:
    """Run Great Expectations quality gate for computed features."""
    source_dir = _dataset_root(kwargs)
    data_path = Path(source_dir) / "gold_features" / "feat_ticker_technical_indicators.parquet"
    if not data_path.exists():
        data_path = Path(repo_path("evidence", "phase3", "feat_ticker_technical_indicators.parquet"))

    suite_path = repo_path("configs", "quality", "suite_features.json")
    doc_out = repo_path("evidence", "phase3", "dp3_gx_report.md")

    from quant_edai.quality.gx_runner import run_gx_checkpoint
    logger.info(f"Running Features Quality Gate against {suite_path} (repo={repo_root()})")
    results = run_gx_checkpoint(data_path=str(data_path), suite_path=suite_path, doc_out_path=doc_out)

    evidence_dir = Path(repo_path("evidence", "phase3"))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    with open(evidence_dir / "dp3_features_metrics.json", "w") as f:
        json.dump(results, f, indent=2)

    if not results.get("success", False):
        raise ValueError(f"Features Quality Gate failed: {results.get('unsuccessful_expectations')} failed expectations")
    return results


def task_emit_datahub_lineage(**kwargs) -> dict:
    """Emit end-to-end dataset lineage to DataHub / proof report."""
    from quant_edai.quality.datahub_emitter import emit_pipeline_lineage
    logger.info("Emitting DataHub lineage and markdown graph...")
    lineage = emit_pipeline_lineage(
        out_json=repo_path("evidence", "phase3", "lineage_proof.json"),
        out_md=repo_path("evidence", "phase3", "lineage_proof.md"),
    )
    return lineage


def task_sync_clickhouse(**kwargs) -> dict:
    """Synchronize Gold datasets to ClickHouse serving layer."""
    source_dir = os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
    from quant_edai.serving.clickhouse_sync import sync_clickhouse_gold
    logger.info(f"Synchronizing Gold datasets from {source_dir} to ClickHouse...")
    metrics = sync_clickhouse_gold(
        source_dir=source_dir,
        engine=os.environ.get("ENGINE", "auto"),
    )
    return metrics


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="dag_dp3_features",
        default_args=default_args,
        description="DP3: Quant Technical Features & Lineage (Compute -> Quality Gate -> Lineage)",
        schedule_interval="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["phase3", "gold", "features", "lineage", "gx"],
    ) as dag:
        features_task = PythonOperator(
            task_id="calculate_technical_features",
            python_callable=task_calculate_features,
        )

        quality_task = PythonOperator(
            task_id="quality_gate_features",
            python_callable=task_quality_gate_features,
        )

        lineage_task = PythonOperator(
            task_id="emit_datahub_lineage",
            python_callable=task_emit_datahub_lineage,
        )

        features_task >> quality_task >> lineage_task

except ImportError:
    logger.info("Airflow package not installed in environment; DAG definition available in Airflow container.")
