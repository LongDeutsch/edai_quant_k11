"""Master E2E Pipeline Orchestration DAG.

Orchestrates the entire Quant Lakehouse & Serving lifecycle in sequence:
DP1 (Raw Ingestion -> Bronze -> GX)
  >> DP2 (Deduplication -> Resample -> SCD2 -> Compaction -> GX)
    >> DP3 (Technical Features -> Feature GX -> Lineage)
      >> DP4 (ClickHouse Historical Sync & Serving Verification)

Propagates dataset_id, execution mode, engine, and storage paths throughout.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
from typing import Any, Dict

logger = logging.getLogger("Master_E2E_Quant_Pipeline")

default_args = {
    "owner": "quant_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}


def task_verify_e2e_summary(**kwargs) -> Dict[str, Any]:
    """Verify that all pipeline phases completed and log summary metadata."""
    dag_run = kwargs.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}
    dataset_id = conf.get("DATASET_ID", "unknown")
    mode = conf.get("MODE", "unknown")
    logger.info(
        "Master E2E Pipeline completed successfully for dataset '%s' (mode=%s).",
        dataset_id,
        mode,
    )
    return {
        "status": "SUCCESS",
        "dataset_id": dataset_id,
        "mode": mode,
        "timestamp": datetime.utcnow().isoformat(),
        "phases_executed": [
            "dag_dp1_raw_ingestion",
            "dag_dp2_silver_gold",
            "dag_dp3_features",
            "dag_dp4_clickhouse_sync",
        ],
    }


try:
    from airflow import DAG
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.operators.python import PythonOperator

    pipeline_conf = {
        "DATASET_ID": "{{ (dag_run.conf or {}).get('DATASET_ID', '') }}",
        "MODE": "{{ (dag_run.conf or {}).get('MODE', 'integration') }}",
        "ENGINE": "{{ (dag_run.conf or {}).get('ENGINE', 'spark') }}",
        "OUT_DIR": "{{ (dag_run.conf or {}).get('OUT_DIR', '/tmp/lakehouse_source') }}",
    }

    with DAG(
        dag_id="dag_e2e_quant_pipeline",
        default_args=default_args,
        description="Master Orchestration: DP1 (Bronze) -> DP2 (Silver/Gold) -> DP3 (Features) -> DP4 (ClickHouse)",
        schedule_interval=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["master", "e2e", "orchestration", "phase3", "phase4"],
    ) as dag:
        dp1_task = TriggerDagRunOperator(
            task_id="trigger_dp1_raw_ingestion",
            trigger_dag_id="dag_dp1_raw_ingestion",
            conf=pipeline_conf,
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=2,
            allowed_states=["success"],
            failed_states=["failed"],
        )

        dp2_task = TriggerDagRunOperator(
            task_id="trigger_dp2_silver_gold",
            trigger_dag_id="dag_dp2_silver_gold",
            conf=pipeline_conf,
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=2,
            allowed_states=["success"],
            failed_states=["failed"],
        )

        dp3_task = TriggerDagRunOperator(
            task_id="trigger_dp3_features",
            trigger_dag_id="dag_dp3_features",
            conf=pipeline_conf,
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=2,
            allowed_states=["success"],
            failed_states=["failed"],
        )

        dp4_task = TriggerDagRunOperator(
            task_id="trigger_dp4_clickhouse_sync",
            trigger_dag_id="dag_dp4_clickhouse_sync",
            conf=pipeline_conf,
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=2,
            allowed_states=["success"],
            failed_states=["failed"],
        )

        summary_task = PythonOperator(
            task_id="verify_e2e_summary",
            python_callable=task_verify_e2e_summary,
        )

        dp1_task >> dp2_task >> dp3_task >> dp4_task >> summary_task

except ImportError:
    logger.info("Airflow package not installed in environment; DAG definition available in Airflow container.")
