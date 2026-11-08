"""Airflow DAG trigger and execution orchestrator.

Triggers DAGs via Airflow REST API when services are running,
and executes pipeline tasks with end-to-end evidence collection.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AirflowTrigger")


class AirflowDAGTrigger:
    """Trigger Airflow DAGs via REST API with fallback."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080/api/v1",
        user: str = "admin",
        password: str = "admin",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (user, password)

    def trigger_dag(self, dag_id: str, conf: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Trigger a DAG run via Airflow REST API."""
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        payload = {"conf": conf or {}}
        try:
            resp = requests.post(url, json=payload, auth=self.auth, timeout=5)
            if resp.status_code in [200, 201]:
                data = resp.json()
                logger.info(f"Successfully triggered Airflow DAG '{dag_id}' (run_id: {data.get('dag_run_id')})")
                return {"status": "TRIGGERED", "dag_id": dag_id, "data": data}
            else:
                logger.warning(f"Airflow trigger returned status {resp.status_code}: {resp.text}")
                return {"status": "WARNING", "status_code": resp.status_code, "detail": resp.text}
        except Exception as e:
            logger.info(f"Airflow service not reachable at {self.base_url} ({e}). Falling back to local execution.")
            return {"status": "OFFLINE_FALLBACK", "error": str(e)}


def run_dp(dag_name: str, engine: str = "auto") -> Dict[str, Any]:
    """Execute DP pipeline tasks and record evidence."""
    trigger = AirflowDAGTrigger()
    api_res = trigger.trigger_dag(dag_name)

    os.environ["ENGINE"] = engine
    evidence_dir = Path("evidence/phase3")
    evidence_dir.mkdir(parents=True, exist_ok=True)

    summary: Dict[str, Any] = {
        "pipeline": dag_name,
        "engine": engine,
        "airflow_trigger": api_res,
        "task_results": {},
    }

    if dag_name == "dag_dp1_raw_ingestion":
        from orchestration.dags.dag_dp1_raw_ingestion import task_ingest_external_source, task_quality_gate_raw
        logger.info("Executing DP1: Ingestion & Quality Gate...")
        ingest_res = task_ingest_external_source()
        gx_res = task_quality_gate_raw()
        summary["task_results"] = {"ingest": ingest_res, "quality_gate": gx_res}

    elif dag_name == "dag_dp2_silver_gold":
        from orchestration.dags.dag_dp2_silver_gold import (
            task_dedup_and_evolve,
            task_scd2_dimension,
            task_iceberg_compaction,
            task_quality_gate_gold,
        )
        logger.info("Executing DP2: Silver/Gold Processing & Compaction...")
        dedup_res = task_dedup_and_evolve()
        scd2_res = task_scd2_dimension()
        compact_res = task_iceberg_compaction()
        gx_res = task_quality_gate_gold()
        summary["task_results"] = {
            "dedup": dedup_res,
            "scd2": scd2_res,
            "compaction": compact_res,
            "quality_gate": gx_res,
        }

    elif dag_name == "dag_dp3_features":
        from orchestration.dags.dag_dp3_features import (
            task_calculate_features,
            task_quality_gate_features,
            task_emit_datahub_lineage,
            task_sync_clickhouse,
        )
        logger.info("Executing DP3: Quant Technical Features & Serving...")
        feat_res = task_calculate_features()
        gx_res = task_quality_gate_features()
        lineage_res = task_emit_datahub_lineage()
        ch_res = task_sync_clickhouse()
        summary["task_results"] = {
            "features": feat_res,
            "quality_gate": gx_res,
            "lineage": lineage_res,
            "clickhouse": ch_res,
        }

    out_file = evidence_dir / f"{dag_name}_execution.json"
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info(f"Execution summary for {dag_name} saved to {out_file}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Trigger and run Airflow Data Pipelines.")
    parser.add_argument("--dag", required=True, choices=["dag_dp1_raw_ingestion", "dag_dp2_silver_gold", "dag_dp3_features"])
    parser.add_argument("--engine", default=os.getenv("ENGINE", "auto"))
    args = parser.parse_args()

    run_dp(dag_name=args.dag, engine=args.engine)


if __name__ == "__main__":
    main()
