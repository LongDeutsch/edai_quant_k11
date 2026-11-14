"""Lightweight historical metrics for seeded alphas (Page 4 / ensemble picker).

Simulates long/flat (or long/short) per strategy over the last ~2 years of
gold/feature data (or full available history), then upserts metrics_snapshot
onto alpha_registry rows.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from quant_edai.common.dataset_paths import resolve_dataset_root
from quant_edai.common.execution_status import assert_integration_ready, is_integration_mode
from quant_edai.common.ml_provenance import (
    dataset_id_from_path,
    default_alpha_model_version,
    is_real_provenance_value,
    resolve_provenance,
)
from quant_edai.quant.alpha_catalog import (
    ALPHA_VERSION,
    DEFAULT_ENTRY_Z,
    DEFAULT_EXIT_Z,
    DEFAULT_SL_PCT,
    score_row_features,
)
from quant_edai.quant.metrics import (
    SHARPE_INSUFFICIENT,
    calculate_max_drawdown,
    calculate_sharpe,
    finite_float,
)
from quant_edai.quant.paper_db import PaperDatabase

logger = logging.getLogger(__name__)

DEFAULT_BOOKSIZE = 100_000.0
DEFAULT_WINDOW_YEARS = 2
METRICS_KEYS = (
    "sharpe",
    "max_drawdown",
    "win_rate",
    "pnl",
    "booksize",
    "n_trades",
    "window_start",
    "window_end",
    "horizon",
    "window_used",
)


def _load_feature_panel(source_dir: str) -> Tuple[pd.DataFrame, str]:
    """Load Gold feature parquet (dataset-scoped first); synthesize only in smoke.

    MODE=integration|full: missing Gold features raise (no evidence/raw/synthetic fallbacks).
    """
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
    candidates.append(roots[0] / "feat_ticker_technical_indicators")
    for path in candidates:
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path)
            if df.empty:
                continue
            logger.info("Loaded feature panel from %s (%s rows)", path, len(df))
            return df, str(path)
        except Exception as e:
            logger.warning("Failed reading %s: %s", path, e)

    assert_integration_ready(
        condition=False,
        message=f"Gold features (feat_ticker_technical_indicators) not found under {source_dir}; "
        "run `make features` first (raw-OHLCV derivation and synthetic panel are smoke-only).",
    )

    # Fallback (smoke): derive features from raw OHLCV if present
    ohlcv_path = roots[0] / "raw_market_ohlcv"
    if ohlcv_path.exists():
        try:
            from quant_edai.processing.features.feature_calculator import (
                compute_technical_indicators,
            )

            raw = pd.read_parquet(ohlcv_path)
            feats = compute_technical_indicators(raw)
            logger.info("Derived features from raw_market_ohlcv (%s rows)", len(feats))
            return feats, f"{ohlcv_path}#derived"
        except Exception as e:
            logger.warning("OHLCV feature derivation failed: %s", e)

    # Minimal synthetic panel (~2y business days x few tickers) — smoke only, SIMULATED
    tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "PG", "SPY"]
    end = pd.Timestamp.now(tz="UTC").normalize()
    start = end - pd.DateOffset(years=DEFAULT_WINDOW_YEARS)
    dates = pd.bdate_range(start=start, end=end)
    rng = np.random.default_rng(42)
    rows: List[Dict[str, Any]] = []
    for t_i, ticker in enumerate(tickers):
        price = 100.0 + t_i * 10.0
        for d in dates:
            ret = float(rng.normal(0.0004, 0.015))
            price = max(1.0, price * (1.0 + ret))
            rows.append(
                {
                    "ticker_symbol": ticker,
                    "event_timestamp": d.strftime("%Y-%m-%d 16:00:00"),
                    "close_price": round(price, 4),
                    "volume": int(1_000_000 + t_i * 50_000),
                    "rsi_14d": float(np.clip(50 + rng.normal(0, 12), 5, 95)),
                    "macd_histogram": float(rng.normal(0, 0.3)),
                    "sma_ratio_20_50": float(1.0 + rng.normal(0, 0.02)),
                    "intraday_return_1d": ret,
                    "rolling_volatility_30d": float(0.15 + 0.05 * abs(rng.normal())),
                    "p_ml": float(np.clip(0.5 + ret * 8.0, 0.05, 0.95)),
                }
            )
    return pd.DataFrame(rows), "synthetic_panel_2y"


def _normalize_panel(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "event_timestamp" not in out.columns and "trade_date" in out.columns:
        out["event_timestamp"] = pd.to_datetime(out["trade_date"]).dt.strftime("%Y-%m-%d 16:00:00")
    out["event_timestamp"] = pd.to_datetime(out["event_timestamp"], utc=False, errors="coerce")
    out = out.dropna(subset=["event_timestamp"])
    if "close_price" not in out.columns and "close" in out.columns:
        out["close_price"] = out["close"]
    if "ticker_symbol" not in out.columns and "ticker" in out.columns:
        out["ticker_symbol"] = out["ticker"]
    if "p_ml" not in out.columns:
        ret = out.get("intraday_return_1d", pd.Series(0.0, index=out.index)).fillna(0.0)
        out["p_ml"] = (0.5 + 8.0 * ret).clip(0.05, 0.95)
    out["date"] = out["event_timestamp"].dt.strftime("%Y-%m-%d")
    out.sort_values(["ticker_symbol", "event_timestamp"], inplace=True)
    return out


def _clip_window(df: pd.DataFrame, years: int = DEFAULT_WINDOW_YEARS) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Prefer last `years` years; if shorter history, use full span and label window_used."""
    if df.empty:
        return df, {
            "window_start": "",
            "window_end": "",
            "window_used": "empty",
        }
    max_ts = df["event_timestamp"].max()
    min_ts = df["event_timestamp"].min()
    target_start = max_ts - pd.DateOffset(years=years)
    if min_ts >= target_start:
        clipped = df
        window_used = "full_available"
    else:
        clipped = df[df["event_timestamp"] >= target_start]
        window_used = f"last_{years}y"
    return clipped, {
        "window_start": str(pd.to_datetime(clipped["event_timestamp"].min()).date()),
        "window_end": str(pd.to_datetime(clipped["event_timestamp"].max()).date()),
        "window_used": window_used,
    }


def _simulate_alpha(
    panel: pd.DataFrame,
    strategy: str,
    config: Dict[str, Any],
    booksize: float,
    horizon: str,
) -> Dict[str, Any]:
    """Cross-sectional equal-weight long/short sim with SL; returns metrics dict."""
    entry_z = float(config.get("entry_z", DEFAULT_ENTRY_Z))
    exit_z = float(config.get("exit_z", DEFAULT_EXIT_Z))
    sl_pct = float(config.get("sl_pct", DEFAULT_SL_PCT))

    dates = sorted(panel["date"].unique())
    if len(dates) < 5:
        empty = {k: 0.0 for k in ("max_drawdown", "win_rate", "pnl", "n_trades")}
        empty.update({
            "sharpe": None,
            "sharpe_reason": SHARPE_INSUFFICIENT,
            "booksize": booksize,
            "horizon": horizon,
        })
        return empty

    scores: Dict[Tuple[str, str], float] = {}
    for _, row in panel.iterrows():
        row_dict = row.to_dict()
        z, _ = score_row_features(strategy, row_dict, config)
        scores[(str(row_dict["ticker_symbol"]), str(row_dict["date"]))] = float(z)

    cash = float(booksize)
    positions: Dict[str, Dict[str, float]] = {}
    equity_curve: List[float] = []
    daily_returns: List[float] = []
    trade_pnls: List[float] = []
    n_trades = 0
    prev_equity = float(booksize)
    max_pos = 5
    notional = booksize / max_pos

    def _mark_equity(marks: Dict[str, float]) -> float:
        eq = cash
        for t, p in positions.items():
            px = marks.get(t, p["entry"])
            # Reserved capital at entry + unrealized PnL by side
            eq += p["entry"] * p["qty"] + (px - p["entry"]) * p["qty"] * p["side"]
        return float(eq)

    for d in dates:
        day = panel[panel["date"] == d]
        marks = {str(r["ticker_symbol"]): float(r["close_price"]) for _, r in day.iterrows()}

        for ticker in list(positions.keys()):
            if ticker not in marks:
                continue
            px = marks[ticker]
            pos = positions[ticker]
            entry = pos["entry"]
            side = pos["side"]
            hit_sl = (side > 0 and px <= entry * (1.0 - sl_pct)) or (
                side < 0 and px >= entry * (1.0 + sl_pct)
            )
            z = scores.get((ticker, d), 0.0)
            exit_signal = (
                abs(z) <= exit_z
                or (side > 0 and z <= -entry_z)
                or (side < 0 and z >= entry_z)
            )
            if hit_sl or exit_signal:
                pnl = (px - entry) * pos["qty"] * side
                cash += entry * pos["qty"] + pnl
                trade_pnls.append(pnl)
                n_trades += 1
                del positions[ticker]

        candidates: List[Tuple[str, float, float]] = []
        for ticker, px in marks.items():
            if ticker in positions:
                continue
            z = scores.get((ticker, d), 0.0)
            if z >= entry_z:
                candidates.append((ticker, abs(z), 1.0))
            elif z <= -entry_z:
                candidates.append((ticker, abs(z), -1.0))
        candidates.sort(key=lambda x: x[1], reverse=True)

        for ticker, _, side in candidates:
            if len(positions) >= max_pos or cash < notional * 0.5:
                break
            px = marks[ticker]
            if px <= 0:
                continue
            qty = notional / px
            cash -= notional
            positions[ticker] = {"qty": qty, "entry": px, "side": side}
            n_trades += 1

        equity = _mark_equity(marks)
        equity_curve.append(equity)
        daily_returns.append((equity - prev_equity) / prev_equity if prev_equity > 0 else 0.0)
        prev_equity = equity

    sharpe = calculate_sharpe(pd.Series(daily_returns), periods_per_year=252)
    mdd, _ = calculate_max_drawdown(pd.Series(equity_curve))
    wins = sum(1 for p in trade_pnls if p > 0)
    win_rate = (wins / len(trade_pnls)) if trade_pnls else 0.0
    pnl = float(equity_curve[-1] - booksize)

    return {
        "sharpe": sharpe.rounded(4),
        "sharpe_reason": sharpe.reason,
        "max_drawdown": round(float(mdd), 4),
        "win_rate": round(float(win_rate), 4),
        "pnl": round(pnl, 2),
        "booksize": float(booksize),
        "n_trades": int(n_trades),
        "horizon": horizon,
    }


def _parse_config(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw) or {}
        except Exception:
            return {}
    return {}


def _parse_metrics(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            return json.loads(raw) or {}
        except Exception:
            return {}
    return {}


def run_alpha_metrics(
    db: Optional[PaperDatabase] = None,
    source_dir: str = "/tmp/lakehouse_source",
    booksize: float = DEFAULT_BOOKSIZE,
    window_years: int = DEFAULT_WINDOW_YEARS,
    horizon: Optional[str] = None,
) -> Dict[str, Any]:
    """Compute and upsert metrics_snapshot for registry alphas."""
    db = db or PaperDatabase()
    db.init_db(reset=False)

    panel_raw, source = _load_feature_panel(source_dir)
    synthetic = source.startswith("synthetic") or source.endswith("#derived")
    base_prov = resolve_provenance(
        model_version=default_alpha_model_version(ALPHA_VERSION),
        dataset_id=dataset_id_from_path(source),
        feature_source=None if source.startswith("synthetic") else source,
    )
    panel = _normalize_panel(panel_raw)

    has_splits = "split_id" in panel.columns and "IS" in panel["split_id"].values
    if has_splits:
        panel_is = panel[panel["split_id"] == "IS"].copy()
        panel_oos = panel[panel["split_id"] == "OOS"].copy()
        window_meta = {
            "window_start": str(pd.to_datetime(panel_is["event_timestamp"].min()).date()),
            "window_end": str(pd.to_datetime(panel_is["event_timestamp"].max()).date()),
            "window_used": "full_available",
        }
    else:
        panel, window_meta = _clip_window(panel, years=window_years)
        panel_is = panel
        panel_oos = pd.DataFrame()

    alphas = db.list_alphas()
    if horizon:
        alphas = [a for a in alphas if str(a.get("alpha_type", "")).upper() == horizon.upper()]

    updated = 0
    results: List[Dict[str, Any]] = []

    for alpha in alphas:
        cfg = _parse_config(alpha.get("config_json"))
        strategy = str(cfg.get("strategy") or "composite_eq")
        alpha_horizon = str(alpha.get("alpha_type") or cfg.get("horizon") or "INTRADAY").upper()

        # 1. In-Sample Evaluation (IS)
        metrics = _simulate_alpha(
            panel=panel_is,
            strategy=strategy,
            config=cfg,
            booksize=booksize,
            horizon=alpha_horizon,
        )
        metrics.update(window_meta)
        metrics["strategy"] = strategy
        metrics["source"] = source
        metrics["split_id"] = "IS"
        metrics["computed_at"] = datetime.now(timezone.utc).isoformat()

        # Preserve prior non-overlapping keys (e.g. ic_mean from seed)
        prior = _parse_metrics(alpha.get("metrics_snapshot"))
        prior.update(metrics)

        # 2. Out-of-Sample Validation (OOS)
        metrics_oos: Dict[str, Any] = {}
        if not panel_oos.empty:
            metrics_oos = _simulate_alpha(
                panel=panel_oos,
                strategy=strategy,
                config=cfg,
                booksize=booksize,
                horizon=alpha_horizon,
            )
            metrics_oos["strategy"] = strategy
            metrics_oos["split_id"] = "OOS"
            is_sh = finite_float(metrics.get("sharpe", 0.0))
            oos_sh = finite_float(metrics_oos.get("sharpe", 0.0))
            if is_sh is None or oos_sh is None:
                metrics_oos["degradation_pct"] = None
            else:
                metrics_oos["degradation_pct"] = round(((is_sh - oos_sh) / max(0.1, is_sh)) * 100.0, 2)
            metrics_oos["computed_at"] = datetime.now(timezone.utc).isoformat()

        record = dict(alpha)
        record["config_json"] = cfg
        record["metrics_snapshot"] = prior
        if metrics_oos:
            record["out_of_sample_metrics"] = metrics_oos
        record["split_id"] = "IS"
        # Metrics were computed from *this* feature source: stamp lineage (keep the row's own model_version)
        for f in ("dataset_id", "feature_source"):
            if base_prov.get(f):
                record[f] = base_prov[f]
        if not is_real_provenance_value(record.get("model_version")):
            record["model_version"] = base_prov.get("model_version")
        db.upsert_alpha(record)
        updated += 1
        results.append(
            {
                "alpha_id": alpha["alpha_id"],
                "version": alpha.get("version"),
                "horizon": alpha_horizon,
                "sharpe": metrics["sharpe"],
                "pnl": metrics["pnl"],
                "window_used": metrics["window_used"],
            }
        )

    return {
        "status": "PASSED_SIMULATED" if source.startswith("synthetic") else "SUCCESS",
        "execution_mode": "SIMULATED" if synthetic else ("LIVE" if is_integration_mode() else "LOCAL_GOLD"),
        "dataset_id": base_prov.get("dataset_id"),
        "feature_source": base_prov.get("feature_source") or source,
        "source": source,
        "booksize": booksize,
        "window": window_meta,
        "alphas_updated": updated,
        "results": results,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Refresh alpha metrics_snapshot from feature history")
    parser.add_argument("--source-dir", default=os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    parser.add_argument("--sqlite-path", default=None)
    parser.add_argument("--booksize", type=float, default=DEFAULT_BOOKSIZE)
    parser.add_argument("--window-years", type=int, default=DEFAULT_WINDOW_YEARS)
    parser.add_argument("--horizon", default=None, help="Optional INTRADAY|DAILY filter")
    parser.add_argument(
        "--out-file",
        default="evidence/phase6/alpha_metrics_refresh.json",
    )
    args = parser.parse_args()

    db = PaperDatabase(sqlite_path=args.sqlite_path) if args.sqlite_path else PaperDatabase()
    result = run_alpha_metrics(
        db=db,
        source_dir=args.source_dir,
        booksize=args.booksize,
        window_years=args.window_years,
        horizon=args.horizon,
    )
    out = Path(args.out_file)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, default=str))
    logger.info("Alpha metrics refresh complete -> %s", out)


if __name__ == "__main__":
    main()
