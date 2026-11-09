"""Airflow DAG trigger and optional local fallback executor.

When Airflow REST API is reachable, only trigger the DAG (UI/scheduler runs tasks).
When offline, fall back to executing Python task callables locally for smoke evidence.
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


def _ensure_dags_on_path() -> None:
    """Allow `import orchestration.dags.*` and `quant_paths` from host runs."""
    repo_root = Path(__file__).resolve().parents[3]
    dags_dir = repo_root / "orchestration" / "dags"
    for path in (repo_root, dags_dir, repo_root / "src"):
        p = str(path)
        if p not in sys.path:
            sys.path.insert(0, p)


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
                logger.info(
                    "Successfully triggered Airflow DAG '%s' (run_id: %s)",
                    dag_id,
                    data.get("dag_run_id"),
                )
                return {"status": "TRIGGERED", "dag_id": dag_id, "data": data}
            logger.warning("Airflow trigger returned status %s: %s", resp.status_code, resp.text)
            return {"status": "WARNING", "status_code": resp.status_code, "detail": resp.text}
        except Exception as e:
            logger.info(
                "Airflow service not reachable at %s (%s). Falling back to local execution.",
                self.base_url,
                e,
            )
            return {"status": "OFFLINE_FALLBACK", "error": str(e)}


def _run_local_tasks(dag_name: str) -> Dict[str, Any]:
    """Execute pipeline Python callables locally (Airflow offline smoke path)."""
    _ensure_dags_on_path()

    if dag_name == "dag_dp1_raw_ingestion":
        from orchestration.dags.dag_dp1_raw_ingestion import (
            task_ingest_external_source,
            task_quality_gate_raw,
        )

        logger.info("Executing DP1 locally: Ingestion & Quality Gate...")
        return {
            "ingest": task_ingest_external_source(),
            "quality_gate": task_quality_gate_raw(),
        }

    if dag_name == "dag_dp2_silver_gold":
        from orchestration.dags.dag_dp2_silver_gold import (
            task_dedup_and_evolve,
            task_scd2_dimension,
            task_iceberg_compaction,
            task_quality_gate_gold,
        )

        logger.info("Executing DP2 locally: Silver/Gold Processing & Compaction...")
        return {
            "dedup": task_dedup_and_evolve(),
            "scd2": task_scd2_dimension(),
            "compaction": task_iceberg_compaction(),
            "quality_gate": task_quality_gate_gold(),
        }

    if dag_name == "dag_dp3_features":
        from orchestration.dags.dag_dp3_features import (
            task_calculate_features,
            task_quality_gate_features,
            task_emit_datahub_lineage,
            task_sync_clickhouse,
        )

        logger.info("Executing DP3 locally: Quant Technical Features & Serving...")
        return {
            "features": task_calculate_features(),
            "quality_gate": task_quality_gate_features(),
            "lineage": task_emit_datahub_lineage(),
            "clickhouse": task_sync_clickhouse(),
        }

    raise ValueError(f"Unsupported DAG: {dag_name}")


def run_dp(dag_name: str, engine: str = "auto", force_local: bool = False) -> Dict[str, Any]:
    """Trigger DAG via Airflow API; only run local tasks if API offline or force_local."""
    os.environ["ENGINE"] = engine
    evidence_dir = Path("evidence/phase3")
    evidence_dir.mkdir(parents=True, exist_ok=True)

    trigger = AirflowDAGTrigger()
    api_res = trigger.trigger_dag(dag_name)

    summary: Dict[str, Any] = {
        "pipeline": dag_name,
        "engine": engine,
        "airflow_trigger": api_res,
        "task_results": {},
        "execution_mode": "airflow_api",
    }

    if force_local or api_res.get("status") == "OFFLINE_FALLBACK":
        summary["execution_mode"] = "local_fallback"
        summary["task_results"] = _run_local_tasks(dag_name)
    elif api_res.get("status") == "TRIGGERED":
        logger.info(
            "DAG '%s' triggered on Airflow. Monitor progress at http://localhost:8080 "
            "(local Python re-run skipped to avoid double execution).",
            dag_name,
        )
    else:
        logger.warning("Unexpected Airflow trigger status; attempting local fallback.")
        summary["execution_mode"] = "local_fallback"
        summary["task_results"] = _run_local_tasks(dag_name)

    out_file = evidence_dir / f"{dag_name}_execution.json"
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info("Execution summary for %s saved to %s", dag_name, out_file)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Trigger and run Airflow Data Pipelines.")
    parser.add_argument(
        "--dag",
        required=True,
        choices=["dag_dp1_raw_ingestion", "dag_dp2_silver_gold", "dag_dp3_features"],
    )
    parser.add_argument("--engine", default=os.getenv("ENGINE", "auto"))
    parser.add_argument(
        "--local",
        action="store_true",
        help="Force local Python task execution even if Airflow API is up.",
    )
    args = parser.parse_args()
    run_dp(dag_name=args.dag, engine=args.engine, force_local=args.local)


if __name__ == "__main__":
    main()
