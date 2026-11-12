"""Honest execution status: SIMULATED for smoke/sim; FAILED when integration lacks services."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


INTEGRATION_MODES = frozenset({"integration", "full"})
SMOKE_MODES = frozenset({"smoke", "test", "local"})


@dataclass
class ExecutionResult:
    status: str
    execution_mode: str
    detail: Optional[str] = None
    extras: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        out = {
            "status": self.status,
            "execution_mode": self.execution_mode,
        }
        if self.detail:
            out["detail"] = self.detail
        out.update(self.extras)
        return out


def normalize_mode(mode: Optional[str] = None) -> str:
    return (mode or os.environ.get("MODE", "smoke")).strip().lower()


def is_integration_mode(mode: Optional[str] = None) -> bool:
    return normalize_mode(mode) in INTEGRATION_MODES


def is_simulated_engine(engine: Optional[str] = None) -> bool:
    eng = (engine or os.environ.get("ENGINE", "sim")).strip().lower()
    return eng in {"sim", "auto", "simulation", ""}


def build_status(
    *,
    ok: bool,
    mode: Optional[str] = None,
    engine: Optional[str] = None,
    degraded_reason: Optional[str] = None,
    forced_simulated: bool = False,
    **extras: Any,
) -> ExecutionResult:
    """
    Contract:
    - smoke + sim/local fallback → status PASSED_SIMULATED, execution_mode=SIMULATED
    - integration|full + failure/missing → status FAILED (never SUCCESS)
    - integration|full + real success → status SUCCESS, execution_mode=LIVE
    """
    m = normalize_mode(mode)
    simulated = forced_simulated or is_simulated_engine(engine) or m in SMOKE_MODES

    if not ok:
        if is_integration_mode(m):
            return ExecutionResult(
                status="FAILED",
                execution_mode="LIVE_REQUIRED",
                detail=degraded_reason or "required service or data missing",
                extras=extras,
            )
        return ExecutionResult(
            status="DEGRADED" if degraded_reason else "FAILED",
            execution_mode="SIMULATED",
            detail=degraded_reason,
            extras=extras,
        )

    if is_integration_mode(m) and not forced_simulated and not is_simulated_engine(engine):
        return ExecutionResult(status="SUCCESS", execution_mode="LIVE", extras=extras)

    # Smoke/CI or explicit sim engine
    return ExecutionResult(
        status="PASSED_SIMULATED",
        execution_mode="SIMULATED",
        detail=degraded_reason,
        extras=extras,
    )


def assert_integration_ready(
    *,
    mode: Optional[str] = None,
    condition: bool,
    message: str,
) -> None:
    """Raise RuntimeError in integration/full when a required dependency is missing."""
    if is_integration_mode(mode) and not condition:
        raise RuntimeError(f"[MODE={normalize_mode(mode)}] {message}")
