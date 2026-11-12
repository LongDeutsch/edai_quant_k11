"""Airflow DAG trigger with terminal-state wait (DP1 → DP2 → DP3).

When Airflow REST API is reachable, trigger and poll until success|failed.
When offline: smoke → local SIMULATED fallback; integration|full → FAILED.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

import requests

from quant_edai.common.execution_status import build_status, is_integration_mode, normalize_mode

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AirflowTrigger")

TERMINAL_STATES = frozenset({"success", "failed", "upstream_failed"})


def _ensure_dags_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    dags_dir = repo_root / "orchestration" / "dags"
    for path in (repo_root, dags_dir, repo_root / "src"):
        p = str(path)
        if p not in sys.path:
            sys.path.insert(0, p)


class AirflowDAGTrigger:
    def __init__(
        self,
        base_url: str = "http://localhost:8080/api/v1",
        user: str = "admin",
        password: str = "admin",
        poll_interval: float = 3.0,
        timeout_sec: float = 600.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (user, password)
        self.poll_interval = poll_interval
        self.timeout_sec = timeout_sec

    def trigger_dag(self, dag_id: str, conf: Dict[str, Any] | None = None) -> Dict[str, Any]:
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        payload = {"conf": conf or {}}
        last_err = None
        for attempt in range(1, 4):
            try:
                resp = requests.post(url, json=payload, auth=self.auth, timeout=15)
                if resp.status_code in [200, 201]:
                    data = resp.json()
                    logger.info(
                        "Triggered Airflow DAG '%s' (run_id: %s)",
                        dag_id,
                        data.get("dag_run_id"),
                    )
                    return {"status": "TRIGGERED", "dag_id": dag_id, "data": data}
                return {"status": "WARNING", "status_code": resp.status_code, "detail": resp.text}
            except (requests.RequestException, ConnectionError, TimeoutError) as e:
                last_err = e
                logger.warning("Attempt %d/3 triggering DAG '%s' failed: %s", attempt, dag_id, e)
                time.sleep(1.0)
        return {"status": "OFFLINE_FALLBACK", "error": str(last_err)}

    def wait_for_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}"
        deadline = time.time() + self.timeout_sec
        last_state = "queued"
        consecutive_errors = 0
        max_consecutive_errors = 6
        while time.time() < deadline:
            try:
                resp = requests.get(url, auth=self.auth, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                consecutive_errors = 0
                last_state = (data.get("state") or "").lower()
                logger.info("DAG %s run %s state=%s", dag_id, dag_run_id, last_state)
                if last_state in TERMINAL_STATES:
                    return {
                        "dag_id": dag_id,
                        "dag_run_id": dag_run_id,
                        "state": last_state,
                        "data": data,
                    }
            except (requests.RequestException, ConnectionError, TimeoutError) as e:
                consecutive_errors += 1
                logger.warning(
                    "Transient polling error for %s/%s (%d/%d): %s",
                    dag_id,
                    dag_run_id,
                    consecutive_errors,
                    max_consecutive_errors,
                    e,
                )
                if consecutive_errors >= max_consecutive_errors:
                    raise RuntimeError(
                        f"Airflow API unreachable after {max_consecutive_errors} consecutive polling attempts for {dag_id}/{dag_run_id}: {e}"
                    ) from e
                time.sleep(min(self.poll_interval * 2, 5.0))
                continue
            time.sleep(self.poll_interval)
        raise TimeoutError(f"Timed out waiting for {dag_id}/{dag_run_id} (last={last_state})")

    def trigger_and_wait(self, dag_id: str, conf: Dict[str, Any] | None = None) -> Dict[str, Any]:
        triggered = self.trigger_dag(dag_id, conf=conf)
        if triggered.get("status") != "TRIGGERED":
            return triggered
        run_id = triggered["data"].get("dag_run_id")
        if not run_id:
            raise RuntimeError(f"No dag_run_id for {dag_id}")
        waited = self.wait_for_dag_run(dag_id, run_id)
        triggered["wait"] = waited
        triggered["terminal_state"] = waited["state"]
        triggered["status"] = "SUCCESS" if waited["state"] == "success" else "FAILED"
        return triggered


def _run_local_tasks(dag_name: str) -> Dict[str, Any]:
    _ensure_dags_on_path()

    if dag_name == "dag_dp1_raw_ingestion":
        from orchestration.dags.dag_dp1_raw_ingestion import (
            task_ingest_external_source,
            task_quality_gate_raw,
        )

        return {"ingest": task_ingest_external_source(), "quality_gate": task_quality_gate_raw()}

    if dag_name == "dag_dp2_silver_gold":
        from orchestration.dags.dag_dp2_silver_gold import (
            task_dedup_and_evolve,
            task_resample_gold_bars,
            task_scd2_dimension,
            task_iceberg_compaction,
            task_quality_gate_gold,
        )

        return {
            "dedup": task_dedup_and_evolve(),
            "resample": task_resample_gold_bars(),
            "scd2": task_scd2_dimension(),
            "compaction": task_iceberg_compaction(),
            "quality_gate": task_quality_gate_gold(),
        }

    if dag_name == "dag_dp3_features":
        from orchestration.dags.dag_dp3_features import (
            task_calculate_features,
            task_quality_gate_features,
            task_emit_datahub_lineage,
        )

        return {
            "features": task_calculate_features(),
            "quality_gate": task_quality_gate_features(),
            "lineage": task_emit_datahub_lineage(),
        }

    if dag_name == "dag_dp4_clickhouse_sync":
        from orchestration.dags.dag_dp4_clickhouse_sync import (
            task_sync_clickhouse_historical,
            task_verify_historical_serving,
        )

        return {
            "sync": task_sync_clickhouse_historical(),
            "verify": task_verify_historical_serving(),
        }

    if dag_name == "dag_e2e_quant_pipeline":
        r1 = _run_local_tasks("dag_dp1_raw_ingestion")
        r2 = _run_local_tasks("dag_dp2_silver_gold")
        r3 = _run_local_tasks("dag_dp3_features")
        r4 = _run_local_tasks("dag_dp4_clickhouse_sync")
        return {"dp1": r1, "dp2": r2, "dp3": r3, "dp4": r4, "status": "SUCCESS"}

    raise ValueError(f"Unsupported DAG: {dag_name}")


def _conf_from_env() -> Dict[str, Any]:
    return {
        "DATASET_ID": os.environ.get("DATASET_ID"),
        "ENGINE": os.environ.get("ENGINE", "sim"),
        "MODE": os.environ.get("MODE", "smoke"),
        "OUT_DIR": os.environ.get("OUT_DIR", "/tmp/lakehouse_source"),
    }


def _write_evidence(dag_name: str, summary: Dict[str, Any]) -> Path:
    evidence_dir = Path("evidence/phase3")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    out_file = evidence_dir / f"{dag_name}_execution.json"
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info("Execution summary for %s saved to %s", dag_name, out_file)
    return out_file


def run_dp(
    dag_name: str,
    engine: str = "auto",
    force_local: bool = False,
    wait: bool = True,
) -> Dict[str, Any]:
    os.environ["ENGINE"] = engine
    mode = normalize_mode()
    conf = _conf_from_env()
    dataset_id = conf.get("DATASET_ID")
    trigger = AirflowDAGTrigger()

    if force_local:
        api_res: Dict[str, Any] = {"status": "FORCE_LOCAL"}
    elif wait:
        api_res = trigger.trigger_and_wait(dag_name, conf=conf)
    else:
        api_res = trigger.trigger_dag(dag_name, conf=conf)

    base = {
        "pipeline": dag_name,
        "engine": engine,
        "mode": mode,
        "dataset_id": dataset_id,
        "airflow_trigger": api_res,
        "dag_run_id": (api_res.get("data") or {}).get("dag_run_id")
        or (api_res.get("wait") or {}).get("dag_run_id"),
        "terminal_state": api_res.get("terminal_state"),
    }

    if api_res.get("status") in {"SUCCESS", "FAILED"} and api_res.get("terminal_state"):
        if api_res["status"] == "FAILED":
            summary = {
                **base,
                **build_status(
                    ok=False, mode=mode, engine=engine, degraded_reason="Airflow DAG failed"
                ).as_dict(),
                "task_results": {},
            }
            _write_evidence(dag_name, summary)
            raise RuntimeError(f"Airflow DAG {dag_name} failed: {api_res.get('terminal_state')}")
        summary = {**base, "status": "SUCCESS", "execution_mode": "LIVE", "task_results": {}}
        _write_evidence(dag_name, summary)
        return summary

    if api_res.get("status") == "TRIGGERED" and not wait:
        summary = {**base, "status": "TRIGGERED", "execution_mode": "LIVE", "task_results": {}}
        _write_evidence(dag_name, summary)
        return summary

    # Offline / unreachable
    if is_integration_mode(mode) and not force_local:
        summary = {
            **base,
            **build_status(
                ok=False,
                mode=mode,
                engine="airflow",
                degraded_reason="Airflow unreachable; MODE=integration|full forbids silent local SUCCESS",
            ).as_dict(),
            "task_results": {},
        }
        _write_evidence(dag_name, summary)
        raise RuntimeError(summary["detail"])

    summary = {
        **base,
        "task_results": _run_local_tasks(dag_name),
        "execution_mode": "SIMULATED",
        "status": "PASSED_SIMULATED",
    }
    _write_evidence(dag_name, summary)
    return summary


def run_dp_chain(engine: str = "auto", force_local: bool = False) -> Dict[str, Any]:
    results = {}
    for dag in ("dag_dp1_raw_ingestion", "dag_dp2_silver_gold", "dag_dp3_features"):
        results[dag] = run_dp(dag_name=dag, engine=engine, force_local=force_local, wait=True)
    return {
        "status": "SUCCESS" if not is_integration_mode() or force_local else "SUCCESS",
        "chain": results,
        "dataset_id": os.environ.get("DATASET_ID"),
        "execution_mode": "SIMULATED" if force_local or not is_integration_mode() else "LIVE",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Trigger and run Airflow Data Pipelines.")
    parser.add_argument(
        "--dag",
        required=False,
        choices=["dag_dp1_raw_ingestion", "dag_dp2_silver_gold", "dag_dp3_features", "chain"],
        default="dag_dp1_raw_ingestion",
    )
    parser.add_argument("--engine", default=os.getenv("ENGINE", "auto"))
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--no-wait", action="store_true")
    args = parser.parse_args()
    if args.dag == "chain":
        summary = run_dp_chain(engine=args.engine, force_local=args.local)
    else:
        summary = run_dp(
            dag_name=args.dag,
            engine=args.engine,
            force_local=args.local,
            wait=not args.no_wait,
        )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
