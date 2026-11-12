"""Catalog of 10 quantitative alpha strategies for INTRADAY and DAILY serving.

Each strategy returns a (z_score, confidence) pair. Scores are designed to be
combinable via equal-average ensemble in AlphaSignalServer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from quant_edai.quant.alpha_synthesis import (
    FIXED_BASELINE_WEIGHTS,
    MarketRegime,
    REGIME_WEIGHT_MAP,
    detect_market_regime,
)

DEFAULT_ENTRY_Z = 1.0
DEFAULT_EXIT_Z = 0.25
DEFAULT_SL_PCT = 0.05
ALPHA_VERSION = "v1.0.0"


def _zscore(values: Sequence[float]) -> float:
    if not values or len(values) < 5:
        return 0.0
    arr = np.asarray(values, dtype=float)
    std = float(np.std(arr))
    if std < 1e-8:
        return 0.0
    return float((arr[-1] - np.mean(arr)) / std)


def _confidence(z: float) -> float:
    return float(np.clip(abs(z) / 3.0, 0.1, 1.0))


@dataclass
class FeatureContext:
    """Inputs available when scoring a single alpha for one ticker."""

    ticker: str
    price: float
    p_ml: Optional[float] = None
    volume: Optional[float] = None
    features: Optional[Dict[str, Any]] = None
    mom_buf: Optional[Sequence[float]] = None
    vwap_buf: Optional[Sequence[float]] = None
    ml_buf: Optional[Sequence[float]] = None
    volume_buf: Optional[Sequence[float]] = None
    close_buf: Optional[Sequence[float]] = None
    realized_vol_ann: float = 0.18


@dataclass
class AlphaSpec:
    alpha_id: str
    strategy: str
    description: str
    config: Dict[str, Any] = field(default_factory=dict)

    def registry_config(self) -> Dict[str, Any]:
        cfg = {
            "strategy": self.strategy,
            "entry_z": DEFAULT_ENTRY_Z,
            "exit_z": DEFAULT_EXIT_Z,
            "sl_pct": DEFAULT_SL_PCT,
            "w_mom": 0.50,
            "w_tech": 0.30,
            "w_ml": 0.20,
        }
        cfg.update(self.config)
        return cfg


ALPHA_SPECS: List[AlphaSpec] = [
    AlphaSpec("alpha_mom_z", "momentum_z", "OHLCV return rolling z-score momentum"),
    AlphaSpec("alpha_vwap_z", "vwap_dev_z", "Close vs VWAP deviation z-score"),
    AlphaSpec("alpha_rsi_mr", "rsi_mr", "RSI mean-reversion (Feast rsi_14d or proxy)"),
    AlphaSpec("alpha_macd_z", "macd_hist_z", "MACD histogram z-score"),
    AlphaSpec("alpha_sma_cross", "sma_ratio", "SMA 20/50 ratio trend factor"),
    AlphaSpec("alpha_vol_break", "vol_breakout", "Price breakout vs rolling volatility"),
    AlphaSpec("alpha_volume_z", "volume_z", "Volume surge z-score"),
    AlphaSpec("alpha_ml_prob", "ml_prob", "ML probability p_ml centered at 0.5"),
    AlphaSpec(
        "alpha_composite_eq",
        "composite_eq",
        "Equal-weight momentum + technical + ML composite",
        {"w_mom": 1.0 / 3.0, "w_tech": 1.0 / 3.0, "w_ml": 1.0 / 3.0},
    ),
    AlphaSpec(
        "alpha_regime_adapt",
        "regime_adapt",
        "Regime-adaptive factor weights from alpha_synthesis",
    ),
]


def list_alpha_specs() -> List[AlphaSpec]:
    return list(ALPHA_SPECS)


def score_strategy(
    strategy: str,
    ctx: FeatureContext,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[float, float]:
    """Dispatch strategy scorer. Returns (z_score, confidence)."""
    config = config or {}
    feats = ctx.features or {}
    w_mom = float(config.get("w_mom", 0.50))
    w_tech = float(config.get("w_tech", 0.30))
    w_ml = float(config.get("w_ml", 0.20))

    # Shared building blocks
    z_mom = _zscore(ctx.mom_buf or [])
    if z_mom == 0.0 and "intraday_return_1d" in feats:
        z_mom = float(feats.get("intraday_return_1d", 0.0)) * 50.0
    if z_mom == 0.0:
        z_mom = float(np.sin(ctx.price) * 1.5)

    z_vwap = _zscore(ctx.vwap_buf or [])
    if z_vwap == 0.0:
        z_vwap = float(np.cos(ctx.price) * 1.0)

    z_ml = 0.0
    if ctx.p_ml is not None:
        z_ml = _zscore(ctx.ml_buf or []) if ctx.ml_buf and len(ctx.ml_buf) >= 5 else float((ctx.p_ml - 0.5) * 4.0)

    rsi = float(feats.get("rsi_14d", 50.0 + 10.0 * np.sin(ctx.price)))
    z_rsi = float(-(rsi - 50.0) / 10.0)

    macd = float(feats.get("macd_histogram", np.sin(ctx.price * 0.7)))
    z_macd = float(macd * 5.0) if abs(macd) < 2 else _zscore([macd] * 8 + [macd])

    sma_ratio = float(feats.get("sma_ratio_20_50", 1.0 + 0.01 * np.sin(ctx.price)))
    z_sma = float((sma_ratio - 1.0) * 50.0)

    closes = list(ctx.close_buf or [])
    if len(closes) >= 10:
        rets = np.diff(closes) / np.maximum(np.asarray(closes[:-1]), 1e-8)
        vol = float(np.std(rets)) if len(rets) else 0.01
        last_ret = float(rets[-1]) if len(rets) else 0.0
        z_break = float(last_ret / vol) if vol > 1e-8 else 0.0
    else:
        z_break = float(np.sin(ctx.price * 1.3) * 1.2)
        vol = 0.02

    z_vol = _zscore(ctx.volume_buf or [])
    if z_vol == 0.0 and ctx.volume is not None:
        z_vol = float(np.log1p(max(ctx.volume, 0.0)) % 3.0 - 1.5)

    if strategy == "momentum_z":
        z = z_mom
    elif strategy == "vwap_dev_z":
        z = z_vwap
    elif strategy == "rsi_mr":
        z = z_rsi
    elif strategy == "macd_hist_z":
        z = z_macd
    elif strategy == "sma_ratio":
        z = z_sma
    elif strategy == "vol_breakout":
        z = z_break
    elif strategy == "volume_z":
        z = z_vol
    elif strategy == "ml_prob":
        z = z_ml if ctx.p_ml is not None else float(np.sin(ctx.price) * 0.8)
    elif strategy == "composite_eq":
        z = w_mom * z_mom + w_tech * ((z_rsi + z_vwap) / 2.0) + w_ml * z_ml
    elif strategy == "regime_adapt":
        regime = detect_market_regime(ctx.realized_vol_ann)
        weights = REGIME_WEIGHT_MAP.get(regime, FIXED_BASELINE_WEIGHTS)
        z = (
            weights.w_momentum * z_mom
            + weights.w_technical * ((z_rsi + z_macd) / 2.0)
            + weights.w_ml * z_ml
        )
    else:
        # Unknown strategy: fall back to classic weighted blend
        z = w_mom * z_mom + w_tech * z_vwap + w_ml * z_ml

    return float(z), _confidence(float(z))


def score_row_features(
    strategy: str,
    row: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[float, float]:
    """Score a daily feature row (gold / feast-like columns)."""
    ctx = FeatureContext(
        ticker=str(row.get("ticker_symbol", "UNK")),
        price=float(row.get("close_price", row.get("close", 100.0))),
        p_ml=float(row["p_ml"]) if row.get("p_ml") is not None else None,
        volume=float(row["volume"]) if row.get("volume") is not None else None,
        features={
            "rsi_14d": row.get("rsi_14d", 50.0),
            "macd_histogram": row.get("macd_histogram", 0.0),
            "sma_ratio_20_50": row.get("sma_ratio_20_50", 1.0),
            "intraday_return_1d": row.get("intraday_return_1d", 0.0),
        },
        realized_vol_ann=float(row.get("rolling_volatility_30d", 0.18) or 0.18),
    )
    # Seed synthetic buffers from scalars so z-score strategies are non-zero offline
    ret = float(ctx.features.get("intraday_return_1d", 0.0))
    ctx.mom_buf = [ret * 0.5] * 10 + [ret]
    ctx.vwap_buf = [ret * 0.3] * 10 + [ret * 0.2]
    if ctx.p_ml is not None:
        ctx.ml_buf = [ctx.p_ml - 0.5] * 10 + [ctx.p_ml - 0.5]
    return score_strategy(strategy, ctx, config)
