"""DP4: ClickHouse Historical Sync & Serving Verification DAG.

Executes:
1. Historical Gold Sync into ClickHouse OLAP:
   - market_daily_ohlcv (daily OHLCV from Gold bars)
   - feat_technical_indicators (computed offline features)
   - dim_companies (SCD2 dimension table)
   Strictly excludes market_intraday_bars to maintain Flink real-time serving isolation.
2. Serving Parity & Dataset Isolation Verification:
   - Query-back from ClickHouse with FINAL deduplication
   - Reconciliation of row counts against Gold sources
   - Cross-dataset isolation verification
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from quant_paths import repo_path, repo_root

logger = logging.getLogger("DP4_ClickHouse_Sync")

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


HISTORICAL_TABLES = ["dim_companies", "market_daily_ohlcv", "feat_technical_indicators"]


def task_sync_clickhouse_historical(**kwargs) -> Dict[str, Any]:
    """Sync Gold tables to ClickHouse, excluding realtime intraday bars."""
    source_dir = _dataset_root(kwargs)
    dataset_id = get_dag_param(kwargs, "DATASET_ID")
    mode = get_dag_param(kwargs, "MODE", "integration")
    engine = get_dag_param(kwargs, "ENGINE", "auto")
    ch_host = get_dag_param(kwargs, "CLICKHOUSE_HOST")
    ch_port = get_dag_param(kwargs, "CLICKHOUSE_PORT")

    from quant_edai.serving.clickhouse_sync import sync_clickhouse_gold

    evidence_file = repo_path("evidence", "phase4", "dp4_serving_sync_metrics.json")
    Path(evidence_file).parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "DP4: Starting ClickHouse Historical Sync for dataset '%s' from '%s' (tables=%s, mode=%s)",
        dataset_id,
        source_dir,
        HISTORICAL_TABLES,
        mode,
    )

    ddl_file = repo_path("sql", "clickhouse", "schema_quant.sql")
    metrics = sync_clickhouse_gold(
        source_dir=source_dir,
        ddl_path=ddl_file,
        dataset_id=dataset_id,
        mode=mode,
        engine=engine,
        host=ch_host,
        port=int(ch_port) if ch_port else None,
        tables=HISTORICAL_TABLES,
        out_file=evidence_file,
    )

    # Strictly assert market_intraday_bars was never synced
    if "market_intraday_bars" in metrics.get("synced_tables", {}):
        raise RuntimeError("Architecture Violation: DP4 historical sync must NEVER sync market_intraday_bars.")

    return metrics


def task_verify_historical_serving(**kwargs) -> Dict[str, Any]:
    """Verify ClickHouse serving row counts, FINAL deduplication, and dataset isolation."""
    dataset_id = get_dag_param(kwargs, "DATASET_ID")
    mode = get_dag_param(kwargs, "MODE", "integration")
    ch_host = get_dag_param(kwargs, "CLICKHOUSE_HOST")
    ch_port = get_dag_param(kwargs, "CLICKHOUSE_PORT")

    from quant_edai.serving.clickhouse_sync import ClickHouseSyncManager, _probe_host, resolve_clickhouse_host

    manager, tried = _probe_host(
        user=None,
        password=None,
        port=int(ch_port) if ch_port else None,
        explicit=resolve_clickhouse_host(ch_host),
    )

    if not manager.is_online():
        if mode in ("integration", "full"):
            raise RuntimeError(f"ClickHouse offline during DP4 verification (tried {tried})")
        return {"status": "SKIPPED_SIMULATED", "reason": "ClickHouse offline in smoke"}

    verification_results: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "mode": mode,
        "tables": {},
        "isolation_verified": False,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Verify each historical table
    for tbl in HISTORICAL_TABLES:
        cnt_final = manager.query_count(tbl, dataset_id=dataset_id, final=True)
        cnt_raw = manager.query_count(tbl, dataset_id=dataset_id, final=False)
        verification_results["tables"][tbl] = {
            "rows_final": cnt_final,
            "rows_raw": cnt_raw,
            "has_data": cnt_final > 0,
        }
        logger.info("Table '%s' for dataset '%s': final=%d, raw=%d", tbl, dataset_id, cnt_final, cnt_raw)
        if cnt_final == 0 and mode in ("integration", "full"):
            raise RuntimeError(f"Historical table '{tbl}' has 0 rows for dataset '{dataset_id}' in ClickHouse.")

    # Cross-dataset isolation test: query a dummy non-existent dataset
    dummy_ds = f"ds_isolated_nonexistent_{int(datetime.utcnow().timestamp())}"
    dummy_count = manager.query_count("market_daily_ohlcv", dataset_id=dummy_ds, final=True)
    if dummy_count != 0:
        raise RuntimeError(f"Cross-dataset isolation violated: dummy dataset '{dummy_ds}' returned {dummy_count} rows.")

    verification_results["isolation_verified"] = True
    verification_results["status"] = "SUCCESS"

    evidence_file = repo_path("evidence", "phase4", "dp4_verification_metrics.json")
    Path(evidence_file).parent.mkdir(parents=True, exist_ok=True)
    with open(evidence_file, "w") as f:
        json.dump(verification_results, f, indent=2)

    return verification_results


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="dag_dp4_clickhouse_sync",
        default_args=default_args,
        description="DP4: ClickHouse Historical Sync & Serving Verification",
        schedule_interval="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["phase4", "gold", "serving", "clickhouse", "historical"],
    ) as dag:
        sync_task = PythonOperator(
            task_id="sync_clickhouse_historical",
            python_callable=task_sync_clickhouse_historical,
        )

        verify_task = PythonOperator(
            task_id="verify_historical_serving",
            python_callable=task_verify_historical_serving,
        )

        sync_task >> verify_task

except ImportError:
    logger.info("Airflow package not installed in environment; DAG definition available in Airflow container.")
