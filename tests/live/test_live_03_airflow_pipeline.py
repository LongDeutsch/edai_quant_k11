"""Gate 5 Live Integration Test: Real Airflow DAG Orchestration & ClickHouse Historical Sync.

Verifies end-to-end execution of:
1. DP1: Ingestion to MinIO & Bronze Great Expectations quality gate
2. DP2: PySpark Silver/Gold processing, Iceberg snapshots, compaction, Gold quality gate
3. DP3: Offline feature generation, technical indicators, DataHub lineage
4. DP4: ClickHouse Historical Sync (dim_companies, market_daily_ohlcv, feat_technical_indicators)
   - Excludes market_intraday_bars to maintain Flink serving isolation
   - Rerun idempotency (no logical duplicate rows)
   - Cross-dataset isolation
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import pytest
import requests

from quant_edai.orchestration.airflow_trigger import AirflowDAGTrigger, _ensure_dags_on_path, run_dp
from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.serving.clickhouse_sync import ClickHouseSyncManager, _probe_host, resolve_clickhouse_host

_ensure_dags_on_path()
from orchestration.dags.dag_dp1_raw_ingestion import get_dag_param, _dataset_root


def _is_airflow_online() -> bool:
    try:
        r = requests.get("http://localhost:8080/api/v1/dags", auth=("admin", "admin"), timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def test_get_dag_param_precedence() -> None:
    """Verifies that dag_run.conf takes strict priority over static os.environ."""
    mock_run = type("MockDagRun", (), {"conf": {"DATASET_ID": "ds_conf_123", "MODE": "integration"}})()
    kwargs = {"dag_run": mock_run}

    assert get_dag_param(kwargs, "DATASET_ID", "default") == "ds_conf_123"
    assert get_dag_param(kwargs, "MODE", "default") == "integration"
    assert get_dag_param(kwargs, "NON_EXISTENT", "fallback") == "fallback"


def test_airflow_trigger_and_wait_polling() -> None:
    """Verifies that trigger_and_wait polls until terminal state and returns status."""
    trigger = AirflowDAGTrigger(poll_interval=0.1, timeout_sec=2.0)

    with patch.object(trigger, "trigger_dag", return_value={"status": "TRIGGERED", "data": {"dag_run_id": "manual__1"}}):
        with patch.object(trigger, "wait_for_dag_run", return_value={"dag_id": "dag_dp1_raw_ingestion", "dag_run_id": "manual__1", "state": "success"}):
            res = trigger.trigger_and_wait("dag_dp1_raw_ingestion", conf={"DATASET_ID": "ds_test"})
            assert res["status"] == "SUCCESS"
            assert res["terminal_state"] == "success"


def test_airflow_fail_hard_when_unreachable_in_integration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifies that under MODE=integration, unreachable Airflow raises RuntimeError (no silent fallback)."""
    monkeypatch.setenv("MODE", "integration")
    with patch.object(
        AirflowDAGTrigger,
        "trigger_and_wait",
        return_value={"status": "OFFLINE_FALLBACK", "error": "Connection refused to :8080"},
    ):
        with pytest.raises(RuntimeError, match="Airflow unreachable"):
            run_dp(dag_name="dag_dp1_raw_ingestion", engine="sim", force_local=False, wait=True)


def test_airflow_simulated_when_unreachable_in_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifies that under MODE=smoke, offline Airflow is clearly labelled SIMULATED."""
    monkeypatch.setenv("MODE", "smoke")
    with patch.object(
        AirflowDAGTrigger,
        "trigger_and_wait",
        return_value={"status": "OFFLINE_FALLBACK", "error": "Connection refused to :8080"},
    ):
        with patch(
            "quant_edai.orchestration.airflow_trigger._run_local_tasks",
            return_value={"task_1": {"status": "PASSED_SIMULATED"}},
        ):
            summary = run_dp(dag_name="dag_dp1_raw_ingestion", engine="sim", force_local=False, wait=True)
            assert summary["status"] == "PASSED_SIMULATED"
            assert summary["execution_mode"] == "SIMULATED"


@pytest.mark.live
def test_live_airflow_e2e_pipeline_dp1_to_dp4() -> None:
    """Live E2E test triggering DP1 -> DP2 -> DP3 -> DP4 on running Airflow scheduler."""
    if not _is_airflow_online():
        pytest.skip("Airflow REST API is offline at http://localhost:8080/api/v1")

    # 1. Mint fresh dataset for live Gate 5 pipeline
    ts = int(time.time())
    ds_id = f"ds_g5_live_{ts}"
    out_dir = "/tmp/lakehouse_source"

    gen = QuantDataGenerator(
        seed=42,
        scale=0.01,
        mode="integration",
        dataset_id=ds_id,
        out_dir=out_dir,
    )
    gen_stats = gen.run()
    assert gen_stats.get("dataset_id") == ds_id

    trigger = AirflowDAGTrigger(
        base_url="http://localhost:8080/api/v1",
        poll_interval=2.0,
        timeout_sec=300.0,
    )
    conf = {
        "DATASET_ID": ds_id,
        "MODE": "integration",
        "ENGINE": "spark",
        "OUT_DIR": out_dir,
    }

    # 2. Trigger DP1: Raw Ingestion -> MinIO & Bronze Quality Gate
    dp1_res = trigger.trigger_and_wait("dag_dp1_raw_ingestion", conf=conf)
    assert dp1_res["status"] == "SUCCESS", f"DP1 failed: {dp1_res}"
    assert dp1_res["terminal_state"] == "success"

    # 3. Trigger DP2: PySpark Silver/Gold Lakehouse Processing & Quality Gate
    dp2_res = trigger.trigger_and_wait("dag_dp2_silver_gold", conf=conf)
    assert dp2_res["status"] == "SUCCESS", f"DP2 failed: {dp2_res}"
    assert dp2_res["terminal_state"] == "success"

    # 4. Trigger DP3: Technical Features & DataHub Lineage
    dp3_res = trigger.trigger_and_wait("dag_dp3_features", conf=conf)
    assert dp3_res["status"] == "SUCCESS", f"DP3 failed: {dp3_res}"
    assert dp3_res["terminal_state"] == "success"

    # 5. Trigger DP4: ClickHouse Historical Sync & Serving Verification
    dp4_res = trigger.trigger_and_wait("dag_dp4_clickhouse_sync", conf=conf)
    assert dp4_res["status"] == "SUCCESS", f"DP4 failed: {dp4_res}"
    assert dp4_res["terminal_state"] == "success"

    # 6. Query ClickHouse Serving Layer Directly
    ch_mgr, _ = _probe_host(user=None, password=None, port=8123, explicit=resolve_clickhouse_host(None))
    assert ch_mgr.is_online(), "ClickHouse must be online for Gate 5 verification"

    daily_count = ch_mgr.query_count("market_daily_ohlcv", dataset_id=ds_id, final=True)
    feats_count = ch_mgr.query_count("feat_technical_indicators", dataset_id=ds_id, final=True)
    dim_count = ch_mgr.query_count("dim_companies", dataset_id=ds_id, final=True)

    assert daily_count > 0, f"Expected market_daily_ohlcv > 0, got {daily_count}"
    assert feats_count > 0, f"Expected feat_technical_indicators > 0, got {feats_count}"
    assert dim_count > 0, f"Expected dim_companies > 0, got {dim_count}"
    assert daily_count == feats_count, f"Daily bars ({daily_count}) should match features ({feats_count})"

    # 7. Strictly verify market_intraday_bars was NOT touched by historical sync
    intraday_count = ch_mgr.query_count("market_intraday_bars", dataset_id=ds_id, final=True)
    assert intraday_count == 0, f"DP4 historical sync must not populate market_intraday_bars, got {intraday_count}"

    # 8. Record Evidence
    evidence_dir = Path("evidence/live") / ds_id
    evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_file = evidence_dir / "gate5_airflow_orchestration_evidence.json"
    evidence_data = {
        "dataset_id": ds_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "runs": {
            "dag_dp1_raw_ingestion": dp1_res,
            "dag_dp2_silver_gold": dp2_res,
            "dag_dp3_features": dp3_res,
            "dag_dp4_clickhouse_sync": dp4_res,
        },
        "clickhouse_serving": {
            "market_daily_ohlcv": daily_count,
            "feat_technical_indicators": feats_count,
            "dim_companies": dim_count,
            "market_intraday_bars": intraday_count,
        },
        "status": "PASSED",
    }
    with open(evidence_file, "w") as f:
        json.dump(evidence_data, f, indent=2, default=str)


@pytest.mark.live
def test_live_airflow_rerun_idempotency_and_cross_dataset_isolation() -> None:
    """Verifies rerun idempotency (same rows after re-trigger) and cross-dataset isolation."""
    if not _is_airflow_online():
        pytest.skip("Airflow REST API is offline at http://localhost:8080/api/v1")

    ch_mgr, _ = _probe_host(user=None, password=None, port=8123, explicit=resolve_clickhouse_host(None))
    assert ch_mgr.is_online()

    trigger = AirflowDAGTrigger(
        base_url="http://localhost:8080/api/v1",
        poll_interval=2.0,
        timeout_sec=300.0,
    )

    # Use first dataset ds_A
    ts = int(time.time())
    ds_a = f"ds_g5_idemp_a_{ts}"
    out_dir = "/tmp/lakehouse_source"

    gen_a = QuantDataGenerator(seed=101, scale=0.01, mode="integration", dataset_id=ds_a, out_dir=out_dir)
    gen_a.run()

    conf_a = {"DATASET_ID": ds_a, "MODE": "integration", "ENGINE": "spark", "OUT_DIR": out_dir}
    trigger.trigger_and_wait("dag_dp1_raw_ingestion", conf=conf_a)
    trigger.trigger_and_wait("dag_dp2_silver_gold", conf=conf_a)
    trigger.trigger_and_wait("dag_dp3_features", conf=conf_a)
    trigger.trigger_and_wait("dag_dp4_clickhouse_sync", conf=conf_a)

    count_a_daily_1 = ch_mgr.query_count("market_daily_ohlcv", dataset_id=ds_a, final=True)
    count_a_feats_1 = ch_mgr.query_count("feat_technical_indicators", dataset_id=ds_a, final=True)
    assert count_a_daily_1 > 0

    # Rerun DP4 on ds_a -> must be idempotent (count_final must not double)
    dp4_rerun = trigger.trigger_and_wait("dag_dp4_clickhouse_sync", conf=conf_a)
    assert dp4_rerun["status"] == "SUCCESS"

    count_a_daily_2 = ch_mgr.query_count("market_daily_ohlcv", dataset_id=ds_a, final=True)
    count_a_feats_2 = ch_mgr.query_count("feat_technical_indicators", dataset_id=ds_a, final=True)
    assert count_a_daily_2 == count_a_daily_1, f"Rerun changed row count: {count_a_daily_1} -> {count_a_daily_2}"
    assert count_a_feats_2 == count_a_feats_1, f"Rerun changed features count: {count_a_feats_1} -> {count_a_feats_2}"

    # Run ds_B -> must NOT mutate ds_A
    ds_b = f"ds_g5_idemp_b_{ts}"
    gen_b = QuantDataGenerator(seed=202, scale=0.01, mode="integration", dataset_id=ds_b, out_dir=out_dir)
    gen_b.run()

    conf_b = {"DATASET_ID": ds_b, "MODE": "integration", "ENGINE": "spark", "OUT_DIR": out_dir}
    trigger.trigger_and_wait("dag_dp1_raw_ingestion", conf=conf_b)
    trigger.trigger_and_wait("dag_dp2_silver_gold", conf=conf_b)
    trigger.trigger_and_wait("dag_dp3_features", conf=conf_b)
    trigger.trigger_and_wait("dag_dp4_clickhouse_sync", conf=conf_b)

    count_a_daily_after_b = ch_mgr.query_count("market_daily_ohlcv", dataset_id=ds_a, final=True)
    count_a_feats_after_b = ch_mgr.query_count("feat_technical_indicators", dataset_id=ds_a, final=True)
    count_b_daily = ch_mgr.query_count("market_daily_ohlcv", dataset_id=ds_b, final=True)
    count_b_feats = ch_mgr.query_count("feat_technical_indicators", dataset_id=ds_b, final=True)

    assert count_a_daily_after_b == count_a_daily_1, "Cross-dataset isolation violated: ds_A changed after syncing ds_B!"
    assert count_a_feats_after_b == count_a_feats_1, "Cross-dataset isolation violated: ds_A features changed after syncing ds_B!"
    assert count_b_daily > 0, "ds_B rows missing in ClickHouse"
    assert count_b_feats > 0, "ds_B features missing in ClickHouse"


@pytest.mark.live
def test_live_airflow_master_dag_e2e_orchestration() -> None:
    """Live verification of the Master DAG dag_e2e_quant_pipeline orchestrating DP1 -> DP2 -> DP3 -> DP4 natively."""
    if not _is_airflow_online():
        pytest.skip("Airflow REST API is offline at http://localhost:8080/api/v1")

    ch_mgr, _ = _probe_host(user=None, password=None, port=8123, explicit=resolve_clickhouse_host(None))
    assert ch_mgr.is_online()

    ts = int(time.time())
    ds_master = f"ds_g5_master_{ts}"
    out_dir = "/tmp/lakehouse_source"

    # 1. Generate Bronze ticks
    gen = QuantDataGenerator(seed=777, scale=0.01, mode="integration", dataset_id=ds_master, out_dir=out_dir)
    gen.run()

    # 2. Trigger Master DAG ONCE
    trigger = AirflowDAGTrigger(
        base_url="http://localhost:8080/api/v1",
        poll_interval=3.0,
        timeout_sec=480.0,
    )
    conf = {
        "DATASET_ID": ds_master,
        "MODE": "integration",
        "ENGINE": "spark",
        "OUT_DIR": out_dir,
    }
    master_res = trigger.trigger_and_wait("dag_e2e_quant_pipeline", conf=conf)
    assert master_res["status"] == "SUCCESS", f"Master DAG failed: {master_res}"
    assert master_res["terminal_state"] == "success"

    # 3. Query ClickHouse Serving Layer Directly
    daily_count = ch_mgr.query_count("market_daily_ohlcv", dataset_id=ds_master, final=True)
    feats_count = ch_mgr.query_count("feat_technical_indicators", dataset_id=ds_master, final=True)
    dim_count = ch_mgr.query_count("dim_companies", dataset_id=ds_master, final=True)
    intraday_count = ch_mgr.query_count("market_intraday_bars", dataset_id=ds_master, final=True)

    assert daily_count > 0, f"Expected market_daily_ohlcv > 0, got {daily_count}"
    assert feats_count > 0, f"Expected feat_technical_indicators > 0, got {feats_count}"
    assert dim_count > 0, f"Expected dim_companies > 0, got {dim_count}"
    assert intraday_count == 0, f"Realtime table must not be populated by master historical DAG, got {intraday_count}"
