"""Spark-vs-Flink OHLCV consistency harness (P1.1 stub).

Feeds the *same* synthetic ticks (duplicates + late arrivals) to:
  * a pandas Spark-equivalent batch resample (dedup → 1m tumbling → OHLCV/VWAP), and
  * the Flink simulation (``FlinkStreamingJob``: dedup + event-time windows + watermark),
then compares per (ticker, 1m window) OHLCV within tolerance.

Evidence layout (one file per dataset_id, never overwritten across runs):
    {evidence_root}/consistency/{dataset_id}/spark_flink_ohlcv.json

Both engines are simulated here, so evidence is labelled SIMULATED
(``PASSED_SIMULATED``), never bare SUCCESS. A LIVE variant should feed real
Spark Silver/Gold bars and Kafka ``market.bars.1m`` into ``compare_ohlcv``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from quant_edai.common.execution_status import build_status, normalize_mode
from quant_edai.processing.flink.streaming_job import FlinkStreamingJob

EVIDENCE_SCHEMA_VERSION = "consistency_evidence.v1"
CHECK_NAME = "spark_flink_ohlcv"

PRICE_FIELDS = ("open_price", "high_price", "low_price", "close_price", "vwap")
EXACT_FIELDS = ("volume", "trade_count")
DEFAULT_TOLERANCE = {"price_abs": 0.01, "volume_abs": 0, "trade_count_abs": 0}

# Stub evidence schema for the consistency harness (field -> expected python type(s)).
CONSISTENCY_EVIDENCE_SCHEMA: Dict[str, Any] = {
    "schema_version": str,
    "check": str,
    "dataset_id": str,
    "status": str,
    "execution_mode": str,
    "engines": dict,
    "tolerance": dict,
    "input": dict,
    "metrics": dict,
    "mismatches": list,
    "generated_at": str,
}


def evidence_path(evidence_root: str | Path, dataset_id: str) -> Path:
    return Path(evidence_root) / "consistency" / dataset_id / f"{CHECK_NAME}.json"


def validate_evidence(evidence: Dict[str, Any]) -> List[str]:
    """Return a list of schema violations (empty = valid)."""
    errors = [f"missing field: {k}" for k in CONSISTENCY_EVIDENCE_SCHEMA if k not in evidence]
    errors += [
        f"bad type for {k}: {type(evidence[k]).__name__}"
        for k, t in CONSISTENCY_EVIDENCE_SCHEMA.items()
        if k in evidence and not isinstance(evidence[k], t)
    ]
    return errors


def generate_synthetic_ticks(
    seed: int = 42,
    n_ticks: int = 1200,
    tickers: Tuple[str, ...] = ("AAPL", "MSFT", "NVDA"),
    start: str = "2024-03-15T14:30:00+00:00",
    duplicate_ratio: float = 0.015,
    late_ratio: float = 0.05,
    late_min_seconds: int = 300,
    late_max_seconds: int = 600,
) -> pd.DataFrame:
    """Deterministic ticks: unique per-ticker timestamps (1-2 s apart), dups and late arrivals.

    Late ticks keep their ``event_timestamp`` but get a delayed ``published_timestamp``
    (arrival time), mirroring ``FaultsInjector.inject_streaming_late_arrivals``.
    """
    rng = np.random.default_rng(seed)
    t0 = pd.Timestamp(start)
    rows: List[Dict[str, Any]] = []
    clock = {t: 0.0 for t in tickers}
    price = {t: 100.0 + 25.0 * i for i, t in enumerate(tickers)}
    for i in range(n_ticks):
        tk = tickers[int(rng.integers(0, len(tickers)))]
        clock[tk] += float(rng.integers(1, 3)) + (i % 7) * 0.001  # strictly increasing, no ties
        price[tk] = max(1.0, price[tk] + float(rng.normal(0, 0.15)))
        ts = t0 + pd.to_timedelta(clock[tk], unit="s")
        rows.append(
            {
                "trade_id": f"syn_{tk}_{i}",
                "ticker_symbol": tk,
                "event_timestamp": ts.isoformat(),
                "price": round(price[tk], 2),
                "size": int(rng.choice([10, 50, 100, 200, 500])),
                "exchange": "NASDAQ",
                "published_timestamp": ts,
                "is_late": False,
                "is_duplicate": False,
            }
        )
    df = pd.DataFrame(rows)
    df["published_timestamp"] = pd.to_datetime(df["published_timestamp"], utc=True)

    n_late = int(len(df) * late_ratio)
    if n_late:
        idx = rng.choice(len(df), size=n_late, replace=False)
        delays = rng.integers(late_min_seconds, late_max_seconds + 1, size=n_late)
        for i, d in zip(idx, delays):
            df.loc[df.index[i], "published_timestamp"] += pd.Timedelta(int(d), unit="s")
            df.loc[df.index[i], "is_late"] = True

    n_dup = int(len(df) * duplicate_ratio)
    if n_dup:
        dup = df.iloc[rng.choice(len(df), size=n_dup, replace=False)].copy()
        dup["is_duplicate"] = True
        # Producer retry: same trade_id/event_ts, arrives slightly later
        dup["published_timestamp"] = dup["published_timestamp"] + pd.Timedelta(1, unit="s")
        df = pd.concat([df, dup], ignore_index=True)

    # Stream arrival order
    return df.sort_values("published_timestamp", kind="stable").reset_index(drop=True)


def spark_equivalent_resample(df_ticks: pd.DataFrame) -> pd.DataFrame:
    """Batch semantics of Spark dedup + 1m tumbling resample (no watermark: all data counted)."""
    df = df_ticks.copy()
    df = df.drop_duplicates(subset=["ticker_symbol", "trade_id", "event_timestamp"], keep="first")
    df["event_dt"] = pd.to_datetime(df["event_timestamp"], utc=True, format="ISO8601")
    df["window_start"] = df["event_dt"].dt.floor("1min")
    df = df.sort_values(["ticker_symbol", "window_start", "event_dt"], kind="stable")
    df["px_size"] = df["price"] * df["size"]
    g = df.groupby(["ticker_symbol", "window_start"], sort=True)
    out = g.agg(
        open_price=("price", "first"),
        high_price=("price", "max"),
        low_price=("price", "min"),
        close_price=("price", "last"),
        volume=("size", "sum"),
        px_size=("px_size", "sum"),
        trade_count=("price", "size"),
    ).reset_index()
    out["vwap"] = out["px_size"] / out["volume"].clip(lower=1)
    out["window_start"] = out["window_start"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return out.drop(columns="px_size")


def flink_sim_windows(
    df_ticks: pd.DataFrame,
    watermark_delay_seconds: int = 600,
    allowed_lateness_seconds: int = 600,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Run the Flink simulation and normalise its bars to the shared comparison schema."""
    job = FlinkStreamingJob(
        watermark_delay_seconds=watermark_delay_seconds,
        allowed_lateness_seconds=allowed_lateness_seconds,
        enable_dedup=True,
    )
    bars, summary = job.run_event_time_tumbling_window_1m(df_ticks)
    if bars.empty:
        return bars, summary
    bars = bars.copy()
    bars["window_start"] = pd.to_datetime(bars["window_start"], utc=True, format="mixed").dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    return bars[["ticker_symbol", "window_start", *PRICE_FIELDS, *EXACT_FIELDS]], summary


def compare_ohlcv(
    spark_bars: pd.DataFrame,
    flink_bars: pd.DataFrame,
    tolerance: Optional[Dict[str, float]] = None,
    max_mismatches: int = 25,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    tol = {**DEFAULT_TOLERANCE, **(tolerance or {})}
    key = ["ticker_symbol", "window_start"]
    merged = spark_bars.merge(flink_bars, on=key, how="outer", suffixes=("_spark", "_flink"), indicator=True)
    only_spark = merged[merged["_merge"] == "left_only"]
    only_flink = merged[merged["_merge"] == "right_only"]
    both = merged[merged["_merge"] == "both"]

    max_diff: Dict[str, float] = {}
    bad = pd.Series(False, index=both.index)
    for f in (*PRICE_FIELDS, *EXACT_FIELDS):
        diff = (both[f"{f}_spark"].astype(float) - both[f"{f}_flink"].astype(float)).abs()
        limit = tol["price_abs"] if f in PRICE_FIELDS else tol.get(f"{f}_abs", 0)
        max_diff[f] = float(diff.max()) if len(diff) else 0.0
        bad |= diff > limit + 1e-9

    mismatches: List[Dict[str, Any]] = []
    for _, row in both[bad].head(max_mismatches).iterrows():
        mismatches.append({k: (row[k] if not isinstance(row[k], (np.generic,)) else row[k].item()) for k in key})
        for f in (*PRICE_FIELDS, *EXACT_FIELDS):
            mismatches[-1][f"{f}_spark"] = float(row[f"{f}_spark"])
            mismatches[-1][f"{f}_flink"] = float(row[f"{f}_flink"])
    for _, row in pd.concat([only_spark, only_flink]).head(max_mismatches).iterrows():
        mismatches.append({**{k: row[k] for k in key}, "presence": row["_merge"]})

    return {
        "bars_spark": int(len(spark_bars)),
        "bars_flink": int(len(flink_bars)),
        "bars_matched": int(len(both) - int(bad.sum())),
        "bars_value_mismatch": int(bad.sum()),
        "bars_missing_in_flink": int(len(only_spark)),
        "bars_missing_in_spark": int(len(only_flink)),
        "max_abs_diff": max_diff,
        "passed": bool(bad.sum() == 0 and len(only_spark) == 0 and len(only_flink) == 0),
    }, mismatches[:max_mismatches]


def run_consistency(
    dataset_id: str = "synthetic_consistency",
    seed: int = 42,
    n_ticks: int = 1200,
    tolerance: Optional[Dict[str, float]] = None,
    evidence_root: Optional[str | Path] = "evidence",
    mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Run both engines on the same ticks, compare, and (optionally) write evidence JSON."""
    ticks = generate_synthetic_ticks(seed=seed, n_ticks=n_ticks)
    spark_bars = spark_equivalent_resample(ticks)
    flink_bars, flink_summary = flink_sim_windows(ticks)
    metrics, mismatches = compare_ohlcv(spark_bars, flink_bars, tolerance=tolerance)

    status = build_status(ok=metrics["passed"], mode=mode, engine="sim", forced_simulated=True)
    if not metrics["passed"]:
        status_str, exec_mode = "FAILED", "SIMULATED"
    else:
        status_str, exec_mode = status.status, status.execution_mode  # PASSED_SIMULATED / SIMULATED

    evidence: Dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "check": CHECK_NAME,
        "dataset_id": dataset_id,
        "status": status_str,
        "execution_mode": exec_mode,
        "mode": normalize_mode(mode),
        "engines": {"spark": "pandas_equivalent_resample", "flink": "flink_sim"},
        "tolerance": {**DEFAULT_TOLERANCE, **(tolerance or {})},
        "input": {
            "seed": seed,
            "ticks_total": int(len(ticks)),
            "ticks_late": int(ticks["is_late"].sum()),
            "ticks_duplicate": int(ticks["is_duplicate"].sum()),
            "flink_dlq_records": int(flink_summary.get("dlq_records_routed", 0)),
            "flink_watermark_mode": flink_summary.get("watermark_mode"),
        },
        "metrics": metrics,
        "mismatches": mismatches,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    if evidence_root is not None:
        path = evidence_path(evidence_root, dataset_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
        evidence["evidence_path"] = str(path)
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark vs Flink OHLCV consistency harness (simulated)")
    parser.add_argument("--dataset-id", default="synthetic_consistency")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-ticks", type=int, default=1200)
    parser.add_argument("--evidence-root", default="evidence")
    args = parser.parse_args()
    ev = run_consistency(
        dataset_id=args.dataset_id, seed=args.seed, n_ticks=args.n_ticks, evidence_root=args.evidence_root
    )
    print(json.dumps({k: ev[k] for k in ("status", "execution_mode", "metrics", "evidence_path")}, indent=2, default=str))
    return 0 if ev["status"] != "FAILED" else 1


if __name__ == "__main__":
    sys.exit(main())
