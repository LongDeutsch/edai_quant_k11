"""Seed 10 alpha strategies into alpha_registry for INTRADAY and DAILY horizons.

Also publishes daily T->T+1 signals into alpha_signals_daily for paper trading.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from quant_edai.common.dataset_paths import resolve_dataset_root
from quant_edai.common.execution_status import assert_integration_ready, is_integration_mode
from quant_edai.common.ml_provenance import (
    dataset_id_from_path,
    default_alpha_model_version,
    resolve_provenance,
)
from quant_edai.quant.alpha_catalog import (
    ALPHA_VERSION,
    DEFAULT_ENTRY_Z,
    list_alpha_specs,
    score_row_features,
)
from quant_edai.quant.alpha_registry import AlphaStatus
from quant_edai.quant.paper_db import PaperDatabase

logger = logging.getLogger(__name__)

DEFAULT_TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "PG", "SPY"]


def _next_business_day(asof: str) -> str:
    dt = datetime.strptime(asof, "%Y-%m-%d")
    nxt = dt + timedelta(days=1)
    while nxt.weekday() >= 5:  # skip Sat/Sun
        nxt += timedelta(days=1)
    return nxt.strftime("%Y-%m-%d")


def _feature_candidates(source_dir: str) -> List[Path]:
    """Dataset-scoped Gold first (OUT_DIR/runs/{dataset_id}); legacy flat/evidence paths only in smoke."""
    roots: List[Path] = []
    try:
        roots.append(resolve_dataset_root(source_dir))
    except FileNotFoundError:
        pass
    roots.append(Path(source_dir))
    candidates: List[Path] = []
    for root in roots:
        candidates += [
            root / "gold_features" / "feat_ticker_technical_indicators.parquet",
            root / "feat_ticker_technical_indicators.parquet",
            root / "gold" / "feat_ticker_technical_indicators.parquet",
        ]
    if not is_integration_mode():
        candidates.append(Path("evidence/phase3/feat_ticker_technical_indicators.parquet"))
    return candidates


def _load_feature_rows(source_dir: str) -> Tuple[pd.DataFrame, str]:
    """Load Gold feature parquet; synthetic panel only in smoke (integration|full raises)."""
    for path in _feature_candidates(source_dir):
        if path.exists():
            try:
                df = pd.read_parquet(path)
                logger.info("Loaded feature rows from %s (%s rows)", path, len(df))
                return df, str(path)
            except Exception as e:
                logger.warning("Failed reading %s: %s", path, e)

    assert_integration_ready(
        condition=False,
        message=f"Gold features (feat_ticker_technical_indicators) not found under {source_dir}; "
        "run `make features` first (synthetic seed panel is smoke-only).",
    )
    # Synthetic panel for offline seed (smoke only -> SIMULATED)
    rows = []
    asof = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for i, ticker in enumerate(DEFAULT_TICKERS):
        rows.append(
            {
                "ticker_symbol": ticker,
                "event_timestamp": asof,
                "close_price": 100.0 + i * 10.0,
                "volume": 1_000_000 + i * 50_000,
                "rsi_14d": 45.0 + i,
                "macd_histogram": 0.1 * (i - 4),
                "sma_ratio_20_50": 1.0 + 0.005 * (i - 4),
                "intraday_return_1d": 0.002 * (i - 4),
                "rolling_volatility_30d": 0.15 + 0.01 * (i % 3),
                "p_ml": 0.45 + 0.02 * i,
            }
        )
    return pd.DataFrame(rows), "synthetic_panel"


def seed_alpha_registry(
    db: PaperDatabase,
    status: str = AlphaStatus.PAPER_TRIAL.value,
    promote: bool = False,
    provenance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Upsert 10 strategies x INTRADAY + DAILY = 20 registry rows (each with ML provenance)."""
    provenance = provenance or resolve_provenance(model_version=default_alpha_model_version(ALPHA_VERSION))
    now = datetime.now(timezone.utc).isoformat()
    final_status = AlphaStatus.PROMOTED_ACTIVE.value if promote else status
    specs = list_alpha_specs()
    written = 0

    for spec in specs:
        cfg = spec.registry_config()
        metrics = {
            "ic_mean": 0.06,
            "sharpe": 1.6,
            "max_drawdown": -0.12,
            "seeded": True,
            "strategy": spec.strategy,
        }
        for horizon in ("INTRADAY", "DAILY"):
            record = {
                "alpha_id": spec.alpha_id,
                "version": ALPHA_VERSION,
                "alpha_type": horizon,
                "status": final_status,
                "description": f"{spec.description} [{horizon}]",
                "config_json": cfg,
                "metrics_snapshot": metrics,
                "created_at": now,
                "promoted_at": now if final_status in (
                    AlphaStatus.PAPER_TRIAL.value,
                    AlphaStatus.PROMOTED_ACTIVE.value,
                ) else None,
                "promoted_by": "alpha_seed" if final_status != AlphaStatus.RESEARCH.value else None,
                "retired_at": None,
                **{k: v for k, v in provenance.items() if v},
            }
            # Unique PK is (alpha_id, version) — store horizon-specific ids
            # so INTRADAY and DAILY do not overwrite each other.
            record["alpha_id"] = f"{spec.alpha_id}_{horizon.lower()}"
            # Keep strategy family id inside config for ensemble readability
            cfg_h = dict(cfg)
            cfg_h["family_alpha_id"] = spec.alpha_id
            cfg_h["horizon"] = horizon
            record["config_json"] = cfg_h
            db.upsert_alpha(record)
            written += 1

    return {
        "registry_rows": written,
        "strategies": len(specs),
        "status": final_status,
        "version": ALPHA_VERSION,
    }


def _seed_provenance(source: str) -> Dict[str, Any]:
    """model_version / dataset_id / feature_source for rows derived from ``source`` features."""
    synthetic = source.startswith("synthetic")
    return resolve_provenance(
        model_version=default_alpha_model_version(ALPHA_VERSION),
        dataset_id=dataset_id_from_path(source),
        feature_source=None if synthetic else source,
    )


def seed_daily_signals(
    db: PaperDatabase,
    source_dir: str = "/tmp/lakehouse_source",
    asof_date: Optional[str] = None,
    loaded: Optional[Tuple[pd.DataFrame, str]] = None,
) -> Dict[str, Any]:
    """Publish daily signals for all DAILY seeded alphas with effective_date = T+1."""
    df, source = loaded if loaded is not None else _load_feature_rows(source_dir)
    provenance = _seed_provenance(source)
    if "event_timestamp" in df.columns:
        try:
            ts = pd.to_datetime(df["event_timestamp"])
            asof = asof_date or ts.max().strftime("%Y-%m-%d")
            # Prefer latest cross-section
            df = df.copy()
            df["_d"] = pd.to_datetime(df["event_timestamp"]).dt.strftime("%Y-%m-%d")
            latest = df[df["_d"] == asof]
            if not latest.empty:
                df = latest
        except Exception:
            asof = asof_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    else:
        asof = asof_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    effective = _next_business_day(asof)
    specs = list_alpha_specs()
    signals: List[Dict[str, Any]] = []

    # Limit universe size for seed
    if "ticker_symbol" in df.columns:
        tickers = list(df["ticker_symbol"].astype(str).unique())[:20]
        df = df[df["ticker_symbol"].isin(tickers)]
    else:
        tickers = DEFAULT_TICKERS

    for spec in specs:
        alpha_id = f"{spec.alpha_id}_daily"
        cfg = spec.registry_config()
        entry_z = float(cfg.get("entry_z", DEFAULT_ENTRY_Z))

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            ticker = str(row_dict.get("ticker_symbol", "UNK"))
            z, conf = score_row_features(spec.strategy, row_dict, cfg)
            if z >= entry_z:
                side = "BUY"
            elif z <= -entry_z:
                side = "SELL"
            else:
                side = "FLAT"
            # Skip FLAT rows to keep table lean; serving treats missing as flat
            if side == "FLAT":
                continue
            signals.append(
                {
                    "signal_id": f"sig_{alpha_id}_{effective}_{ticker}",
                    "alpha_id": alpha_id,
                    "version": ALPHA_VERSION,
                    "ticker_symbol": ticker,
                    "asof_date": asof,
                    "effective_date": effective,
                    "target_side": side,
                    "target_weight": round(min(1.0, abs(z) / 3.0), 4),
                    "confidence": conf,
                    "raw_score": round(z, 4),
                    "regime_detected": "NORMAL",
                    **{k: v for k, v in provenance.items() if v},
                }
            )

    inserted = db.record_daily_signals(signals)
    return {
        "feature_source": source,
        "synthetic_features": source.startswith("synthetic"),
        "model_version": provenance.get("model_version"),
        "dataset_id": provenance.get("dataset_id"),
        "asof_date": asof,
        "effective_date": effective,
        "signals_prepared": len(signals),
        "signals_inserted": inserted,
        "tickers": tickers if isinstance(tickers, list) else list(tickers),
    }


def run_seed(
    source_dir: str = "/tmp/lakehouse_source",
    sqlite_path: Optional[str] = None,
    promote: bool = False,
    asof_date: Optional[str] = None,
) -> Dict[str, Any]:
    db = PaperDatabase(sqlite_path=sqlite_path) if sqlite_path else PaperDatabase()
    db.init_db(reset=False)
    loaded = _load_feature_rows(source_dir)  # integration|full: raises if Gold features are missing
    provenance = _seed_provenance(loaded[1])
    reg = seed_alpha_registry(db, promote=promote, provenance=provenance)
    daily = seed_daily_signals(db, source_dir=source_dir, asof_date=asof_date, loaded=loaded)
    synthetic = bool(daily.get("synthetic_features"))
    result = {
        # SUCCESS only when seeded from real Gold features; synthetic panel => SIMULATED
        "status": "PASSED_SIMULATED" if synthetic else "SUCCESS",
        "execution_mode": "SIMULATED" if synthetic else ("LIVE" if is_integration_mode() else "LOCAL_GOLD"),
        "model_version": provenance.get("model_version"),
        "dataset_id": provenance.get("dataset_id"),
        "feature_source": provenance.get("feature_source") or loaded[1],
        "registry": reg,
        "daily_signals": daily,
        "db_backend": "postgres" if db.is_postgres else "sqlite",
    }
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Seed 10 INTRADAY + 10 DAILY alphas into registry")
    parser.add_argument("--source-dir", default=os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    parser.add_argument("--sqlite-path", default=None, help="Optional SQLite path override")
    parser.add_argument("--promote", action="store_true", help="Seed as PROMOTED_ACTIVE instead of PAPER_TRIAL")
    parser.add_argument("--asof-date", default=None, help="Override asof date YYYY-MM-DD for daily signals")
    parser.add_argument(
        "--out-file",
        default="evidence/phase6/alpha_seed_metrics.json",
        help="Write seed metrics JSON",
    )
    args = parser.parse_args()

    result = run_seed(
        source_dir=args.source_dir,
        sqlite_path=args.sqlite_path,
        promote=args.promote,
        asof_date=args.asof_date,
    )
    out = Path(args.out_file)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    logger.info("Alpha seed complete -> %s", out)


if __name__ == "__main__":
    main()
