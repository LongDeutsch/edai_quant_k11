"""Engine selection with MODE-aware fail-hard (no silent sim in integration/full)."""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Optional

from quant_edai.common.execution_status import build_status, is_integration_mode, normalize_mode


def run_with_engine(
    *,
    engine: Optional[str],
    run_sim: Callable[[], Dict[str, Any]],
    run_live: Callable[[], Dict[str, Any]],
    live_token: str = "spark",
    job_name: str = "job",
) -> Dict[str, Any]:
    selected = (engine or os.environ.get("ENGINE", "sim")).strip().lower()
    mode = normalize_mode()

    if selected == live_token:
        try:
            result = run_live()
            result.setdefault("execution_mode", "LIVE" if is_integration_mode(mode) else result.get("execution_mode", "LIVE"))
            result.setdefault("status", "SUCCESS")
            result["dataset_id"] = result.get("dataset_id") or os.environ.get("DATASET_ID")
            return result
        except Exception as e:
            if is_integration_mode(mode):
                err = build_status(
                    ok=False,
                    mode=mode,
                    engine=live_token,
                    degraded_reason=f"{job_name} {live_token} failed: {e}",
                ).as_dict()
                raise RuntimeError(err["detail"]) from e
            print(f"[{job_name}] {live_token} unavailable ({e}); SIMULATED fallback (MODE={mode}).")
            sim = run_sim()
            sim["execution_mode"] = "SIMULATED"
            sim["status"] = "PASSED_SIMULATED"
            sim["simulation_fallback_reason"] = str(e)
            return sim

    # sim / auto
    if is_integration_mode(mode) and selected in {"sim", "auto", ""}:
        # integration may still allow sim only when ENGINE explicitly sim for smoke-labelled path
        if selected == "sim" and os.environ.get("ALLOW_SIM_IN_INTEGRATION", "").lower() in {"1", "true"}:
            sim = run_sim()
            sim["execution_mode"] = "SIMULATED"
            sim["status"] = "PASSED_SIMULATED"
            return sim
        # Prefer live when ENGINE=auto under integration
        if selected in {"auto", ""}:
            try:
                result = run_live()
                result["execution_mode"] = "LIVE"
                result.setdefault("status", "SUCCESS")
                return result
            except Exception as e:
                err = build_status(
                    ok=False,
                    mode=mode,
                    engine=live_token,
                    degraded_reason=f"{job_name} required live engine under MODE={mode}: {e}",
                ).as_dict()
                raise RuntimeError(err["detail"]) from e

    sim = run_sim()
    sim["execution_mode"] = "SIMULATED"
    sim.setdefault("status", "PASSED_SIMULATED")
    return sim
