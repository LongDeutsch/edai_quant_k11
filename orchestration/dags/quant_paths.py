"""Shared path helpers for Airflow DAGs running inside Compose containers."""

from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    """Resolve Quant EDAI repo root inside Airflow containers or local runs."""
    env = os.environ.get("QUANT_EDAI_ROOT")
    if env:
        return Path(env)
    candidate = Path("/opt/quant_edai")
    if candidate.exists():
        return candidate
    # Local/dev fallback: orchestration/dags -> repo root
    return Path(__file__).resolve().parents[2]


def repo_path(*parts: str) -> str:
    return str(repo_root().joinpath(*parts))
