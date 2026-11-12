"""P2.1: benchmark-pipeline smoke evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from quant_edai.benchmark.pipeline_benchmark import run_benchmark


@pytest.mark.p2
def test_benchmark_pipeline_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "smoke")
    monkeypatch.setenv("ENGINE", "sim")
    out = tmp_path / "lakehouse"
    report = run_benchmark(
        out_dir=str(out),
        scale=0.001,
        mode="smoke",
        seed=7,
        skip_heavy=False,
    )
    assert report["dataset_id"]
    assert report["execution_mode"] == "SIMULATED"
    assert report["status"] in {"PASSED_SIMULATED", "SUCCESS", "FAILED"}
    assert report["checklist"]["latency_recorded"] is True
    evidence = Path(report["evidence_path"])
    assert evidence.exists()
    loaded = json.loads(evidence.read_text(encoding="utf-8"))
    assert loaded["total_latency_seconds"] >= 0
    assert any(s["step"] == "generate" for s in loaded["steps"])
