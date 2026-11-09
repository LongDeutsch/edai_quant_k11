"""Airflow DAG-folder shim; prefers package import when available on PYTHONPATH."""

from __future__ import annotations

try:
    from quant_edai.orchestration.paths import repo_path, repo_root
except ImportError:  # pragma: no cover - container parse without package install
    import os
    from pathlib import Path

    def repo_root() -> Path:
        env = os.environ.get("QUANT_EDAI_ROOT")
        if env:
            return Path(env)
        candidate = Path("/opt/quant_edai")
        if (candidate / "configs").exists():
            return candidate
        return Path(__file__).resolve().parents[2]

    def repo_path(*parts: str) -> str:
        return str(repo_root().joinpath(*parts))
