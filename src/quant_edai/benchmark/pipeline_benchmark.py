"""P2: scale pipeline benchmark — latency, correctness, recovery evidence."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from quant_edai.common.dataset_paths import read_current_dataset_id, resolve_dataset_root
from quant_edai.common.execution_status import build_status, normalize_mode


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timed(name: str, fn) -> Dict[str, Any]:
    t0 = time.perf_counter()
    try:
        result = fn()
        ok = True
        err = None
    except Exception as e:
        result = None
        ok = False
        err = str(e)
    elapsed = round(time.perf_counter() - t0, 4)
    return {
        "step": name,
        "ok": ok,
        "latency_seconds": elapsed,
        "error": err,
        "result": result if isinstance(result, dict) else {"value": result},
    }


def run_benchmark(
    *,
    out_dir: str,
    scale: float,
    mode: str,
    seed: int = 42,
    dataset_id: Optional[str] = None,
    skip_heavy: bool = False,
) -> Dict[str, Any]:
    """Run generate → dedup → flink-sim → clickhouse(sim) and collect evidence."""
    from quant_edai.generator.data_generator import QuantDataGenerator
    from quant_edai.generator.tick_corpus import materialize_tick_corpus
    from quant_edai.processing.consistency.spark_flink_ohlcv import run_consistency
    from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob

    os.environ["MODE"] = mode
    os.environ["SCALE"] = str(scale)
    os.environ["SEED"] = str(seed)
    steps: List[Dict[str, Any]] = []

    def _gen():
        gen = QuantDataGenerator(
            mode="smoke" if mode == "smoke" else mode,
            scale=scale,
            seed=seed,
            out_dir=out_dir,
            start_date="2024-01-02",
            end_date="2024-01-10",
            dataset_id=dataset_id,
        )
        return gen.run()

    steps.append(_timed("generate", _gen))
    ds = read_current_dataset_id(out_dir) or (steps[0].get("result") or {}).get("dataset_id")
    run_root = resolve_dataset_root(out_dir, ds, allow_legacy=True)

    def _online():
        return materialize_tick_corpus(
            out_dir=out_dir,
            start_date=os.getenv("ONLINE_START", "2025-11-01"),
            end_date=os.getenv("ONLINE_END", "2025-11-05"),
            scale=min(scale, 0.05),
            seed=seed,
            corpus_name="online",
            dataset_id=ds,
        )

    steps.append(_timed("generate_online_ticks", _online))

    def _dedup():
        job = DeduplicationAndSchemaEvolutionJob(
            source_dir=str(run_root),
            target_dir=str(run_root / "stg_market_ohlcv"),
        )
        return job.run(engine=os.getenv("ENGINE", "sim"))

    steps.append(_timed("spark_dedup", _dedup))

    def _consistency():
        n_ticks = max(200, int(1200 * min(scale, 1.0)))
        return run_consistency(
            dataset_id=ds or "bench",
            evidence_root=Path("evidence"),
            n_ticks=n_ticks,
            seed=seed,
            mode=mode,
        )

    if not skip_heavy:
        steps.append(_timed("spark_flink_ohlcv_consistency", _consistency))

    # Optional recovery probe: re-run dedup after "fault" (delete silver, rewrite)
    def _recovery():
        silver = run_root / "stg_market_ohlcv"
        if silver.exists():
            import shutil

            shutil.rmtree(silver)
        job = DeduplicationAndSchemaEvolutionJob(
            source_dir=str(run_root),
            target_dir=str(silver),
        )
        return job.run(engine=os.getenv("ENGINE", "sim"))

    steps.append(_timed("recovery_rededup", _recovery))

    all_ok = all(s["ok"] for s in steps)
    total_latency = round(sum(s["latency_seconds"] for s in steps), 4)
    byte_sum = sum(p.stat().st_size for p in run_root.rglob("*") if p.is_file()) if run_root.exists() else 0

    status = build_status(
        ok=all_ok,
        mode=mode,
        engine=os.getenv("ENGINE", "sim"),
        degraded_reason=None if all_ok else "one or more benchmark steps failed",
        dataset_id=ds,
        forced_simulated=normalize_mode(mode) == "smoke" or os.getenv("ENGINE", "sim") == "sim",
    ).as_dict()

    report = {
        **status,
        "benchmark": True,
        "dataset_id": ds,
        "run_id": ds,
        "scale": scale,
        "mode": mode,
        "seed": seed,
        "started_at": _now(),
        "total_latency_seconds": total_latency,
        "approx_bytes": byte_sum,
        "approx_mb": round(byte_sum / (1024 * 1024), 3),
        "steps": [{k: v for k, v in s.items() if k != "result" or s["ok"]} for s in steps],
        "checklist": {
            "latency_recorded": True,
            "correctness_consistency": any(
                s["step"] == "spark_flink_ohlcv_consistency" and s["ok"] for s in steps
            ),
            "recovery_rededup": any(s["step"] == "recovery_rededup" and s["ok"] for s in steps),
            "physical_100gb": byte_sum >= 100 * 1024**3,
            "honest_footprint_note": (
                "Physical 100GB not required in CI; report scale curve + tick budget."
            ),
        },
        "paths": {"run_root": str(run_root), "out_dir": out_dir},
    }

    evidence_dir = Path("evidence/benchmark")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    out_path = evidence_dir / f"{ds or 'unknown'}_report.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    with open(evidence_dir / "latest_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    report["evidence_path"] = str(out_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline scale benchmark")
    parser.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    parser.add_argument("--scale", type=float, default=float(os.getenv("SCALE", "0.01")))
    parser.add_argument("--mode", default=os.getenv("MODE", "smoke"))
    parser.add_argument("--seed", type=int, default=int(os.getenv("SEED", "42")))
    parser.add_argument("--dataset-id", default=os.getenv("DATASET_ID"))
    parser.add_argument("--skip-heavy", action="store_true")
    args = parser.parse_args()
    report = run_benchmark(
        out_dir=args.out_dir,
        scale=args.scale,
        mode=args.mode,
        seed=args.seed,
        dataset_id=args.dataset_id,
        skip_heavy=args.skip_heavy,
    )
    print(json.dumps(report, indent=2, default=str))
    if report.get("status") == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
