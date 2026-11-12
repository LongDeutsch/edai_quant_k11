"""Tick corpus materializer for online (1y) and optional offline streaming benches.

SCALE controls tick *row budget / density* only (not OHLCV trade volume):
  target_ticks ≈ BASE_TICKS_PER_DAY * n_tickers * n_sessions * SCALE

Mega-caps receive ~80% of ticks (row skew for Spark demos). Late arrivals (~5%)
and streaming duplicates (~1.5%) are injected at materialize time.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from quant_edai.common.dataset_paths import (
    generate_dataset_id,
    read_current_dataset_id,
    resolve_dataset_root,
    write_current_pointer,
)
from quant_edai.generator.faults_injector import FaultsInjector

BASE_TICKS_PER_DAY = 50
DEFAULT_ONLINE_START = "2025-11-01"
DEFAULT_ONLINE_END = "2026-11-10"
DEFAULT_OFFLINE_START = "2022-11-01"
DEFAULT_OFFLINE_END = "2025-10-31"
MEGA_TICK_SHARE = 0.80


def _load_tickers(seed_file: Optional[str] = None) -> Dict[str, Any]:
    path = seed_file or str(Path(__file__).parents[3] / "configs" / "seeds" / "tickers.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "mega_caps": ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA"],
        "tickers": [
            {"ticker": t} for t in ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "SPY"]
        ],
    }


def _session_calendar(start: str, end: str) -> pd.DatetimeIndex:
    return pd.bdate_range(start=start, end=end)


def _ticker_weights(
    tickers: Sequence[str],
    mega_caps: Sequence[str],
    mega_share: float = MEGA_TICK_SHARE,
) -> np.ndarray:
    tickers = list(tickers)
    mega = [t for t in tickers if t in set(mega_caps)]
    other = [t for t in tickers if t not in set(mega_caps)]
    weights = np.zeros(len(tickers), dtype=float)
    if mega and other:
        mega_w = mega_share / len(mega)
        other_w = (1.0 - mega_share) / len(other)
        for i, t in enumerate(tickers):
            weights[i] = mega_w if t in mega else other_w
    else:
        weights[:] = 1.0 / max(1, len(tickers))
    return weights / weights.sum()


def materialize_tick_corpus(
    out_dir: str,
    start_date: str,
    end_date: str,
    scale: float = 0.1,
    seed: int = 42,
    corpus_name: str = "online",
    seed_file: Optional[str] = None,
    base_ticks_per_day: float = BASE_TICKS_PER_DAY,
    dataset_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Write partitioned tick parquet corpus under immutable runs/{dataset_id}/."""
    scale = max(0.001, float(scale))
    meta = _load_tickers(seed_file)
    tickers = [str(t.get("ticker") or t.get("ticker_symbol")) for t in meta["tickers"]]
    mega_caps = list(meta.get("mega_caps") or [])
    prices = {
        str(t.get("ticker") or t.get("ticker_symbol")): float(t.get("base_price", 100.0))
        for t in meta["tickers"]
    }

    sessions = _session_calendar(start_date, end_date)
    n_sessions = len(sessions)
    n_tickers = len(tickers)
    target_ticks = int(base_ticks_per_day * n_tickers * n_sessions * scale)
    target_ticks = max(n_sessions, target_ticks)  # at least 1 tick/day average floor

    rng = np.random.default_rng(seed)
    faults = FaultsInjector(seed=seed)
    weights = _ticker_weights(tickers, mega_caps)

    ticks_per_day = max(1, target_ticks // max(1, n_sessions))
    remainder = target_ticks - ticks_per_day * n_sessions

    base_out = Path(out_dir)
    ds = (
        dataset_id
        or os.environ.get("DATASET_ID")
        or read_current_dataset_id(base_out)
        or generate_dataset_id(seed=seed, mode=os.environ.get("MODE", "smoke"), scale=scale)
    )
    write_current_pointer(base_out, ds)
    root = resolve_dataset_root(base_out, ds, create=True)
    if corpus_name == "online":
        corpus_dir = root / "raw_market_ticks_online"
        manifest_name = "online_tick_corpus_manifest.json"
    else:
        corpus_dir = root / "raw_market_ticks_offline"
        manifest_name = "offline_tick_corpus_manifest.json"

    if corpus_dir.exists():
        # Refresh only this corpus slice inside the current run (never delete sibling runs/)
        import shutil

        shutil.rmtree(corpus_dir)
    corpus_dir.mkdir(parents=True, exist_ok=True)

    records: List[Dict[str, Any]] = []
    tick_counter = 0
    for day_i, day in enumerate(sessions):
        day_n = ticks_per_day + (1 if day_i < remainder else 0)
        day_str = day.strftime("%Y-%m-%d")
        chosen = rng.choice(tickers, size=day_n, replace=True, p=weights)
        # Spread across regular-hours-ish timestamps (13:30–20:00 UTC proxy for US equity)
        session_start = pd.Timestamp(f"{day_str}T13:30:00Z")
        step_ms = max(1, 23400000 // max(1, day_n))
        for j, ticker in enumerate(chosen):
            px = prices.get(ticker, 100.0) * float(1.0 + rng.normal(0, 0.002))
            event_ts = (session_start + pd.Timedelta(int(j * step_ms), unit="ms")).floor("s")
            records.append(
                {
                    "trade_id": f"tck_{corpus_name}_{day.strftime('%Y%m%d')}_{tick_counter}",
                    "ticker_symbol": ticker,
                    "trade_date": day_str,
                    "event_timestamp": event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "price": round(px, 4),
                    "size": int(rng.choice([10, 50, 100, 200, 500, 1000])),
                    "exchange": "NASDAQ",
                }
            )
            tick_counter += 1

    df_all = pd.DataFrame(records)
    df_all = faults.inject_streaming_late_arrivals(
        df_all, late_ratio=0.05, min_delay_seconds=300, max_delay_seconds=600
    )
    df_all = faults.inject_streaming_duplicates(df_all, duplicate_ratio=0.015)
    if "is_late" not in df_all.columns:
        df_all["is_late"] = False
    if "is_duplicate" not in df_all.columns:
        df_all["is_duplicate"] = False
    if "published_timestamp" not in df_all.columns:
        df_all["published_timestamp"] = df_all["event_timestamp"]

    df_all.to_parquet(corpus_dir, partition_cols=["trade_date"], index=False)
    total = len(df_all)
    late_n = int(df_all["is_late"].sum()) if "is_late" in df_all.columns else 0
    dup_n = int(df_all["is_duplicate"].sum()) if "is_duplicate" in df_all.columns else 0
    mega_n = int(df_all["ticker_symbol"].isin(mega_caps).sum()) if mega_caps else 0

    # Approx on-disk bytes
    byte_sum = 0
    for p in corpus_dir.rglob("*.parquet"):
        byte_sum += p.stat().st_size

    manifest = {
        "status": "SUCCESS",
        "dataset_id": ds,
        "run_id": ds,
        "corpus_name": corpus_name,
        "scale": scale,
        "scale_mode": "tick_budget",
        "seed": seed,
        "start_date": str(pd.to_datetime(start_date).date()),
        "end_date": str(pd.to_datetime(end_date).date()),
        "n_sessions": int(n_sessions),
        "n_tickers": int(n_tickers),
        "base_ticks_per_day": base_ticks_per_day,
        "target_ticks": target_ticks,
        "total_ticks": int(total),
        "approx_bytes": int(byte_sum),
        "late_arrival_ratio": round(late_n / max(1, total), 4),
        "duplicate_ratio": round(dup_n / max(1, total), 4),
        "mega_cap_tick_row_share": round(mega_n / max(1, total), 4),
        "corpus_dir": str(corpus_dir),
        "run_root": str(root),
        "mega_caps": mega_caps,
    }
    manifest_path = root / manifest_name
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    # Convenience mirrors under base OUT_DIR for make recipes
    with open(base_out / manifest_name, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"[TickCorpus] Wrote {total} ticks dataset_id={ds} -> {corpus_dir} (SCALE={scale})")
    return manifest


def load_tick_corpus(
    corpus_dir: str,
    max_ticks: Optional[int] = None,
) -> pd.DataFrame:
    """Load tick corpus chronologically for streaming replay."""
    path = Path(corpus_dir)
    if not path.exists():
        raise FileNotFoundError(f"Tick corpus not found at {path}")
    df = pd.read_parquet(path)
    if "event_timestamp" in df.columns:
        df = df.sort_values("event_timestamp")
    elif "trade_date" in df.columns:
        df = df.sort_values(["trade_date"])
    if max_ticks is not None and max_ticks > 0:
        df = df.head(int(max_ticks))
    return df.reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize SCALE-budgeted tick corpora")
    parser.add_argument("--out-dir", default=os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    parser.add_argument("--scale", type=float, default=float(os.getenv("SCALE", "0.1")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("SEED", "42")))
    parser.add_argument(
        "--corpus",
        choices=["online", "offline"],
        default="online",
        help="online=1y stream corpus; offline=optional heavy tick sample inside offline window",
    )
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--seed-file", default=None)
    parser.add_argument("--dataset-id", default=None, help="Reuse/create immutable dataset run id")
    args = parser.parse_args()

    if args.corpus == "online":
        start = args.start_date or os.getenv("ONLINE_START", DEFAULT_ONLINE_START)
        end = args.end_date or os.getenv("ONLINE_END", DEFAULT_ONLINE_END)
    else:
        start = args.start_date or os.getenv("OFFLINE_START", DEFAULT_OFFLINE_START)
        end = args.end_date or os.getenv("OFFLINE_END", DEFAULT_OFFLINE_END)

    manifest = materialize_tick_corpus(
        out_dir=args.out_dir,
        start_date=start,
        end_date=end,
        scale=args.scale,
        seed=args.seed,
        corpus_name=args.corpus,
        seed_file=args.seed_file,
        dataset_id=args.dataset_id or os.getenv("DATASET_ID"),
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
