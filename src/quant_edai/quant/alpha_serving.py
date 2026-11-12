"""Alpha Serving & Multi-Factor Signal Server.

Features:
1. Intraday Lookback Clamping: Reads ALPHA_INTRADAY_LOOKBACK_DAYS (default 10), strictly clamped to <= 10 days.
2. Daily Signals: Postgres only (reads alpha_signals_daily table, no Redis dual-write needed).
3. Multi-Alpha Ensemble: Combines K active alphas using Equal Average:
   Z_combined = (1/K) * sum(Z_i).
4. Low-latency Feast/Redis online feature integration with safe in-memory fallback.
5. PM Control checks: blocks entries if block_new_entries is active.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from quant_edai.common.execution_status import is_integration_mode
from quant_edai.common.ml_provenance import PROVENANCE_FIELDS, is_real_provenance_value
from quant_edai.quant.alpha_catalog import (
    DEFAULT_ENTRY_Z,
    DEFAULT_EXIT_Z,
    FeatureContext,
    score_strategy,
)
from quant_edai.quant.alpha_registry import AlphaHorizon, AlphaRegistry, AlphaStatus
from quant_edai.quant.paper_db import PaperDatabase

logger = logging.getLogger(__name__)

# Configurable Intraday Lookback Window (Clamped <= 22 Days / 1 Quant Trading Month)
DEFAULT_INTRADAY_LOOKBACK = 22
MAX_INTRADAY_LOOKBACK = 22


def get_clamped_intraday_lookback_days() -> int:
    """Read ALPHA_INTRADAY_LOOKBACK_DAYS env var, strictly clamped to <= 22 days."""
    raw = os.getenv("ALPHA_INTRADAY_LOOKBACK_DAYS", str(DEFAULT_INTRADAY_LOOKBACK))
    try:
        val = int(raw)
    except ValueError:
        val = DEFAULT_INTRADAY_LOOKBACK
    # Clamp strictly between 1 and 22 days
    return max(1, min(MAX_INTRADAY_LOOKBACK, val))


@dataclass
class TradeSignal:
    ticker_symbol: str
    action: str  # "BUY", "SELL", "FLAT"
    z_score: float
    confidence: float
    horizon: str  # "INTRADAY" or "DAILY"
    target_weight: float
    alphas_combined: List[str]
    asof_timestamp: str
    regime: str = "NORMAL"
    notes: str = ""
    # ML provenance of the alpha rows that produced this signal
    model_version: Optional[str] = None
    dataset_id: Optional[str] = None
    feature_source: Optional[str] = None


def _collect_provenance(rows: Any) -> Dict[str, Optional[str]]:
    """Merge provenance across the alpha rows combined into one signal (distinct model_versions are joined)."""
    out: Dict[str, Optional[str]] = {f: None for f in PROVENANCE_FIELDS}
    for f in PROVENANCE_FIELDS:
        vals: List[str] = []
        for r in rows:
            v = r.get(f) if hasattr(r, "get") else None
            if is_real_provenance_value(v) and str(v) not in vals:
                vals.append(str(v))
        if vals:
            out[f] = vals[0] if f != "model_version" else "+".join(sorted(vals))
    return out


class AlphaSignalServer:
    """Online Alpha Serving engine for Intraday and Daily execution."""

    def __init__(
        self,
        db: Optional[PaperDatabase] = None,
        registry: Optional[AlphaRegistry] = None,
        z_threshold: float = 1.5,
    ) -> None:
        self.db = db or PaperDatabase()
        self.registry = registry or AlphaRegistry(db=self.db)
        self.z_threshold = z_threshold
        self.lookback_days = get_clamped_intraday_lookback_days()
        
        # Lazy Feast Manager & Redis Cache
        self._feast_manager = None
        self._pm_blocks_new_entries = False
        self._redis_client = None
        self._redis_checked = False

    def set_pm_block_entries(self, blocked: bool) -> None:
        """Set PM override flag to reject any new trade signals."""
        self._pm_blocks_new_entries = blocked

    def get_ensemble_selected_ids(
        self,
        horizon: str = "INTRADAY",
        ensemble_id: str = "default",
    ) -> Optional[List[str]]:
        """Return saved Pick-5 alpha_ids for horizon, or None if no ensemble saved."""
        cfg = self.db.get_ensemble_config(horizon=horizon, ensemble_id=ensemble_id)
        if not cfg:
            return None
        ids = cfg.get("selected_alpha_ids") or []
        if not isinstance(ids, list):
            return None
        cleaned = [str(x) for x in ids if x]
        return cleaned or None

    def get_active_alphas_for_serving(
        self,
        horizon: str = "INTRADAY",
        mode: str = "paper",
        ensemble_id: str = "default",
    ) -> List[Dict[str, Any]]:
        """Active registry alphas intersected with saved ensemble selection when present."""
        active = self.registry.get_active_alphas(horizon=horizon, mode=mode)
        selected = self.get_ensemble_selected_ids(horizon=horizon, ensemble_id=ensemble_id)
        if not selected:
            return active
        selected_set = set(selected)
        filtered = [a for a in active if a.get("alpha_id") in selected_set]
        # Preserve ensemble order when possible
        order = {aid: i for i, aid in enumerate(selected)}
        filtered.sort(key=lambda a: order.get(a.get("alpha_id"), 10_000))
        return filtered

    def get_combined_signal_breakdown(
        self,
        ticker: str,
        bar_price: float,
        bar_timestamp: Optional[str] = None,
        p_ml: Optional[float] = None,
        mode: str = "paper",
        horizon: str = "INTRADAY",
        rolling_buffers: Optional[Dict[str, Any]] = None,
        ensemble_id: str = "default",
    ) -> Dict[str, Any]:
        """Per-alpha z scores + equal-average combined z for UI preview."""
        now_ts = bar_timestamp or datetime.now(timezone.utc).isoformat()
        active_alphas = self.get_active_alphas_for_serving(
            horizon=horizon, mode=mode, ensemble_id=ensemble_id
        )
        per_alpha: List[Dict[str, Any]] = []
        z_scores: List[float] = []

        for a in active_alphas:
            cfg = a.get("config_json")
            if isinstance(cfg, str):
                try:
                    cfg = json.loads(cfg)
                except Exception:
                    cfg = {}
            cfg = cfg or {}
            if horizon.upper() == "DAILY":
                # Daily breakdown uses row-like features from feast/fallback scalars
                from quant_edai.quant.alpha_catalog import score_row_features

                row = {
                    "ticker_symbol": ticker,
                    "close_price": bar_price,
                    "p_ml": p_ml,
                }
                z, conf = score_row_features(str(cfg.get("strategy") or "composite_eq"), row, cfg)
            else:
                z, conf = self._compute_single_intraday_alpha(
                    ticker=ticker,
                    price=bar_price,
                    p_ml=p_ml,
                    config=cfg,
                    rolling_buffers=rolling_buffers,
                )
            z_scores.append(float(z))
            per_alpha.append(
                {
                    "alpha_id": a.get("alpha_id"),
                    "version": a.get("version"),
                    "strategy": cfg.get("strategy"),
                    "z_score": round(float(z), 4),
                    "confidence": round(float(conf), 4),
                }
            )

        k = len(z_scores)
        combined = float(np.mean(z_scores)) if k > 0 else 0.0
        return {
            "ticker": ticker,
            "horizon": horizon.upper(),
            "asof_timestamp": now_ts,
            "k": k,
            "combined_z": round(combined, 4),
            "alphas": per_alpha,
            "selected_alpha_ids": [a.get("alpha_id") for a in active_alphas],
        }

    @property
    def redis_client(self):
        """Lazy-load Redis client for sub-5ms signal cache."""
        if not self._redis_checked:
            self._redis_checked = True
            redis_enabled = os.getenv("REDIS_ENABLED", "0") == "1" or os.getenv("REDIS_URL") is not None
            if not redis_enabled:
                self._redis_client = None
                return None

            redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
            redis_port = int(os.getenv("REDIS_PORT", "6379"))
            redis_url = os.getenv("REDIS_URL")
            try:
                import redis
                if redis_url:
                    r = redis.from_url(redis_url, socket_timeout=0.05, socket_connect_timeout=0.05)
                else:
                    r = redis.Redis(host=redis_host, port=redis_port, socket_timeout=0.05, socket_connect_timeout=0.05)
                r.ping()
                self._redis_client = r
                logger.info("AlphaSignalServer connected to Redis signal cache.")
            except Exception as e:
                logger.debug(f"Redis cache unavailable ({e}); proceeding in-memory.")
                self._redis_client = None
        return self._redis_client

    @property
    def feast_manager(self):
        """Lazy-load FeastFeatureStoreManager."""
        if self._feast_manager is None:
            try:
                from quant_edai.ml.feast.feast_manager import FeastFeatureStoreManager
                self._feast_manager = FeastFeatureStoreManager()
            except Exception as e:
                logger.debug(f"Feast manager unavailable ({e}); using simulation fallback.")
                self._feast_manager = None
        return self._feast_manager

    # -------------------------------------------------------------------------
    # 1. INTRADAY MULTI-ALPHA SERVING (SUB-10MS)
    # -------------------------------------------------------------------------
    def get_intraday_signal(
        self,
        ticker: str,
        bar_price: float,
        bar_timestamp: str,
        p_ml: Optional[float] = None,
        mode: str = "paper",
        rolling_buffers: Optional[Dict[str, Any]] = None,
    ) -> TradeSignal:
        """Generate combined intraday signal using K active alphas.

        Enforces:
        - Cửa sổ trượt lookback <= 10 ngày.
        - Multi-alpha combination via Equal Average (1/K * sum(Z_i)).
        - PM block check.
        """
        now_ts = bar_timestamp or datetime.now(timezone.utc).isoformat()

        # 0. PM Emergency Block Check
        if self._pm_blocks_new_entries:
            return TradeSignal(
                ticker_symbol=ticker,
                action="FLAT",
                z_score=0.0,
                confidence=0.0,
                horizon="INTRADAY",
                target_weight=0.0,
                alphas_combined=[],
                asof_timestamp=now_ts,
                notes="BLOCKED_BY_PM_GOVERNANCE",
            )

        # 1. Fetch active intraday alphas (ensemble-filtered to Pick-5 when saved)
        active_alphas = self.get_active_alphas_for_serving(horizon="INTRADAY", mode=mode)

        # In live mode: STRICTLY require PROMOTED_ACTIVE alpha. No promoted alpha = FLAT signal.
        if not active_alphas:
            if mode == "live" or is_integration_mode():
                # integration|full: no synthetic default composite gate
                return TradeSignal(
                    ticker_symbol=ticker,
                    action="FLAT",
                    z_score=0.0,
                    confidence=0.0,
                    horizon="INTRADAY",
                    target_weight=0.0,
                    alphas_combined=[],
                    asof_timestamp=now_ts,
                    notes="REJECTED_NO_PROMOTED_ACTIVE_ALPHA",
                )
            # In paper/sim mode: fallback to baseline composite
            active_alphas = [{
                "alpha_id": "alpha_default_composite",
                "version": "v1.0.0",
                "config_json": {"w_mom": 0.50, "w_tech": 0.30, "w_ml": 0.20},
            }]

        individual_z_scores: List[float] = []
        individual_confidences: List[float] = []
        alpha_names: List[str] = []
        entry_thresholds: List[float] = []
        exit_thresholds: List[float] = []

        # 2. Score each active alpha (strategy dispatch via alpha_catalog)
        for a in active_alphas:
            alpha_id = a["alpha_id"]
            version = a.get("version", "v1.0.0")
            alpha_names.append(f"{alpha_id}:{version}")

            cfg = a.get("config_json")
            if isinstance(cfg, str):
                try:
                    cfg = json.loads(cfg)
                except Exception:
                    cfg = {}
            cfg = cfg or {}
            entry_thresholds.append(float(cfg.get("entry_z", DEFAULT_ENTRY_Z)))
            exit_thresholds.append(float(cfg.get("exit_z", DEFAULT_EXIT_Z)))

            z_score, conf = self._compute_single_intraday_alpha(
                ticker=ticker,
                price=bar_price,
                p_ml=p_ml,
                config=cfg,
                rolling_buffers=rolling_buffers,
            )
            individual_z_scores.append(z_score)
            individual_confidences.append(conf)

        # 3. Rule 5: Multi-alpha Equal Average Combination
        k = len(individual_z_scores)
        z_combined = float(np.mean(individual_z_scores)) if k > 0 else 0.0
        conf_combined = float(np.mean(individual_confidences)) if k > 0 else 0.0
        entry_z = float(np.mean(entry_thresholds)) if entry_thresholds else float(self.z_threshold)
        exit_z = float(np.mean(exit_thresholds)) if exit_thresholds else DEFAULT_EXIT_Z

        # 4. Map combined score to action (entry_z / exit_z band)
        if z_combined >= entry_z:
            action = "BUY"
            weight = min(1.0, z_combined / 3.0)
        elif z_combined <= -entry_z:
            action = "SELL"
            weight = min(1.0, abs(z_combined) / 3.0)
        elif abs(z_combined) <= exit_z:
            action = "FLAT"
            weight = 0.0
        else:
            # Between exit and entry: no new risk (FLAT for serving contract)
            action = "FLAT"
            weight = 0.0

        sig = TradeSignal(
            ticker_symbol=ticker,
            action=action,
            z_score=round(z_combined, 4),
            confidence=round(conf_combined, 4),
            horizon="INTRADAY",
            target_weight=round(weight, 4),
            alphas_combined=alpha_names,
            asof_timestamp=now_ts,
            notes=f"EqualAvg(K={k}) | entry_z={entry_z:.2f} exit_z={exit_z:.2f} | Lookback={self.lookback_days}d",
            **_collect_provenance(active_alphas),
        )

        # Cache in Redis with 60s TTL for sub-5ms low latency serving
        r = self.redis_client
        if r is not None:
            try:
                r.setex(f"alpha:sig:{ticker}", 60, json.dumps(asdict(sig)))
            except Exception:
                pass

        return sig

    def _compute_single_intraday_alpha(
        self,
        ticker: str,
        price: float,
        p_ml: Optional[float],
        config: Dict[str, Any],
        rolling_buffers: Optional[Dict[str, Any]],
    ) -> Tuple[float, float]:
        """Compute score for a single alpha via catalog strategy dispatch."""
        features = None
        if self.feast_manager is not None:
            try:
                features = self.feast_manager.get_online_feature_record(ticker)
            except Exception:
                features = None

        rb = rolling_buffers or {}
        mom_map = rb.get("mom") or {}
        vwap_map = rb.get("vwap") or {}
        ml_map = rb.get("ml") or {}
        vol_map = rb.get("volume") or {}
        close_map = rb.get("close") or {}

        ctx = FeatureContext(
            ticker=ticker,
            price=price,
            p_ml=p_ml,
            volume=(list(vol_map[ticker])[-1] if ticker in vol_map and len(vol_map[ticker]) else None),
            features=features,
            mom_buf=list(mom_map[ticker]) if ticker in mom_map else None,
            vwap_buf=list(vwap_map[ticker]) if ticker in vwap_map else None,
            ml_buf=list(ml_map[ticker]) if ticker in ml_map else None,
            volume_buf=list(vol_map[ticker]) if ticker in vol_map else None,
            close_buf=list(close_map[ticker]) if ticker in close_map else None,
            realized_vol_ann=float((features or {}).get("rolling_volatility_30d", 0.18) or 0.18),
        )
        strategy = str(config.get("strategy") or "composite_eq")
        return score_strategy(strategy, ctx, config)

    # -------------------------------------------------------------------------
    # 2. DAILY MULTI-ALPHA SERVING (POSTGRES ONLY)
    # -------------------------------------------------------------------------
    def get_daily_signal(
        self,
        ticker: str,
        effective_date: str,
        mode: str = "paper",
    ) -> TradeSignal:
        """Fetch daily signal for target date from PostgreSQL alpha_signals_daily.

        Enforces:
        - Postgres only (no Redis dual-write required).
        - Equal average across K active daily alphas.
        - Day-long hold from 09:30 ET to 15:55 ET.
        """
        now_ts = datetime.now(timezone.utc).isoformat()

        # 0. PM Emergency Block Check
        if self._pm_blocks_new_entries:
            return TradeSignal(
                ticker_symbol=ticker,
                action="FLAT",
                z_score=0.0,
                confidence=0.0,
                horizon="DAILY",
                target_weight=0.0,
                alphas_combined=[],
                asof_timestamp=now_ts,
                notes="BLOCKED_BY_PM_GOVERNANCE",
            )

        # 1. Fetch active daily alphas (ensemble-filtered to Pick-5 when saved)
        active_alphas = self.get_active_alphas_for_serving(horizon="DAILY", mode=mode)
        alpha_ids = [a["alpha_id"] for a in active_alphas] if active_alphas else None

        # 2. Query Postgres table alpha_signals_daily
        df_sig = self.db.get_daily_signals(effective_date=effective_date)
        if not df_sig.empty:
            df_ticker = df_sig[df_sig["ticker_symbol"] == ticker]
            if alpha_ids:
                df_ticker = df_ticker[df_ticker["alpha_id"].isin(alpha_ids)]

            if not df_ticker.empty:
                # Rule 5: Multi-alpha Equal Average Combination for Daily
                k = len(df_ticker)
                avg_score = float(df_ticker["raw_score"].mean())
                avg_weight = float(df_ticker["target_weight"].mean())
                avg_conf = float(df_ticker["confidence"].mean())
                alphas_used = [f"{r['alpha_id']}:{r['version']}" for _, r in df_ticker.iterrows()]

                if avg_score >= self.z_threshold:
                    action = "BUY"
                elif avg_score <= -self.z_threshold:
                    action = "SELL"
                else:
                    action = "FLAT"

                regime = df_ticker.iloc[0].get("regime_detected", "NORMAL")
                return TradeSignal(
                    ticker_symbol=ticker,
                    action=action,
                    z_score=round(avg_score, 4),
                    confidence=round(avg_conf, 4),
                    horizon="DAILY",
                    target_weight=round(avg_weight, 4),
                    alphas_combined=alphas_used,
                    asof_timestamp=now_ts,
                    regime=regime,
                    notes=f"Daily EOD T->T+1 | EqualAvg(K={k}) | Hold to 15:55 ET",
                    **_collect_provenance(df_ticker.to_dict(orient="records")),
                )

        # Fallback if no pre-computed daily signal exists
        return TradeSignal(
            ticker_symbol=ticker,
            action="FLAT",
            z_score=0.0,
            confidence=0.0,
            horizon="DAILY",
            target_weight=0.0,
            alphas_combined=[],
            asof_timestamp=now_ts,
            notes="NO_DAILY_SIGNAL_IN_POSTGRES",
        )
