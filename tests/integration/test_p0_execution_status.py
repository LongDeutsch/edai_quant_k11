"""P0.4: SIMULATED vs FAILED contract by MODE."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from quant_edai.common.execution_status import build_status
from quant_edai.ingestion.s3_uploader import S3SourceUploader
from quant_edai.orchestration.airflow_trigger import AirflowDAGTrigger, run_dp


@pytest.mark.p0
def test_smoke_sim_is_simulated() -> None:
    r = build_status(ok=True, mode="smoke", engine="sim")
    assert r.execution_mode == "SIMULATED"
    assert r.status == "PASSED_SIMULATED"


@pytest.mark.p0
def test_integration_service_down_is_failed() -> None:
    r = build_status(ok=False, mode="integration", engine="minio", degraded_reason="down")
    assert r.status == "FAILED"
    assert r.execution_mode == "LIVE_REQUIRED"


@pytest.mark.p0
def test_s3_uploader_integration_fails_without_minio(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "integration")
    root = tmp_path / "runs" / "ds_test"
    root.mkdir(parents=True)
    (root / "dummy.txt").write_text("x", encoding="utf-8")
    (tmp_path / "CURRENT_DATASET_ID").write_text("ds_test\n", encoding="utf-8")
    up = S3SourceUploader(
        source_dir=str(tmp_path),
        endpoint_url="http://127.0.0.1:1",
        dataset_id="ds_test",
        mode="integration",
    )
    result = up.upload_dataset()
    assert result["status"] == "FAILED"


@pytest.mark.p0
def test_s3_uploader_smoke_staged_local(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "smoke")
    root = tmp_path / "runs" / "ds_smoke"
    root.mkdir(parents=True)
    (root / "dummy.txt").write_text("x", encoding="utf-8")
    (tmp_path / "CURRENT_DATASET_ID").write_text("ds_smoke\n", encoding="utf-8")
    up = S3SourceUploader(
        source_dir=str(tmp_path),
        endpoint_url="http://127.0.0.1:1",
        dataset_id="ds_smoke",
        mode="smoke",
    )
    result = up.upload_dataset()
    assert result["status"] == "STAGED_LOCAL"
    assert result["execution_mode"] == "SIMULATED"


@pytest.mark.p0
def test_airflow_offline_integration_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "integration")
    monkeypatch.setenv("ENGINE", "sim")
    with patch.object(
        AirflowDAGTrigger,
        "trigger_and_wait",
        return_value={"status": "OFFLINE_FALLBACK", "error": "connection refused"},
    ):
        with pytest.raises(RuntimeError, match="Airflow unreachable"):
            run_dp(dag_name="dag_dp1_raw_ingestion", engine="sim", force_local=False, wait=True)


@pytest.mark.p0
def test_airflow_offline_smoke_simulated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "smoke")
    monkeypatch.setenv("ENGINE", "sim")
    with patch.object(
        AirflowDAGTrigger,
        "trigger_and_wait",
        return_value={"status": "OFFLINE_FALLBACK", "error": "connection refused"},
    ):
        with patch(
            "quant_edai.orchestration.airflow_trigger._run_local_tasks",
            return_value={"ingest": {"status": "PASSED_SIMULATED"}},
        ):
            summary = run_dp(
                dag_name="dag_dp1_raw_ingestion", engine="sim", force_local=False, wait=True
            )
    assert summary["execution_mode"] == "SIMULATED"
    assert summary["status"] == "PASSED_SIMULATED"
