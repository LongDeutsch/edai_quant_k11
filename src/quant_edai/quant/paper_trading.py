"""Real-time Event-Driven Paper Trading Engine (Rubric M1-38)."""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
import os
import time
from typing import Any, Deque, Dict, List, Optional, Tuple
import uuid

import numpy as np
import pandas as pd

from quant_edai.common.market_time import (
    is_eod_flat_time,
    is_market_open_window,
    parse_bar_timestamp,
)
from quant_edai.common.ml_provenance import (
    PROVENANCE_FIELDS,
    is_real_provenance_value,
    resolve_provenance,
)
from quant_edai.quant.alpha_serving import AlphaSignalServer
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.pm_controls import PMControlService

logger = logging.getLogger(__name__)


@dataclass
class MarketBar:
    ticker_symbol: str
    timestamp: str  # ISO 8601 string
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    vwap: Optional[float] = None
    p_ml: Optional[float] = None
    # ML provenance carried by the bar/signal payload (optional; engine-level values fill gaps)
    model_version: Optional[str] = None
    dataset_id: Optional[str] = None
    feature_source: Optional[str] = None
    source_layer: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> MarketBar:
        return cls(
            ticker_symbol=str(d.get("ticker_symbol") or d.get("ticker")),
            timestamp=str(d.get("timestamp") or d.get("event_timestamp") or d.get("bar_time")),
            open_price=float(d.get("open_price") or d.get("open", 0.0)),
            high_price=float(d.get("high_price") or d.get("high", 0.0)),
            low_price=float(d.get("low_price") or d.get("low", 0.0)),
            close_price=float(d.get("close_price") or d.get("close", 0.0)),
            volume=float(d.get("volume", 0.0)),
            vwap=float(d["vwap"]) if d.get("vwap") is not None else None,
            p_ml=float(d["p_ml"]) if d.get("p_ml") is not None else None,
            model_version=d.get("model_version"),
            dataset_id=d.get("dataset_id"),
            feature_source=d.get("feature_source"),
            source_layer=d.get("source_layer"),
        )


@dataclass
class Position:
    ticker_symbol: str
    session_id: str
    side: str  # 'LONG', 'SHORT'
    quantity: float
    average_entry_price: float
    current_mark_price: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    stop_loss_price: float
    take_profit_price: float
    entry_time: str
    updated_at: str


class PaperTradingEngine:
    """Institutional Event-Driven Intraday Scalping Engine."""

    def __init__(
        self,
        session_id: Optional[str] = None,
        db: Optional[PaperDatabase] = None,
        starting_cash: float = 100_000.0,
        trade_notional: float = 10_000.0,
        max_positions: int = 5,
        max_daily_loss_pct: float = -0.02,  # -2.0% daily circuit breaker
        slippage_bps: float = 5.0,         # 5 bps institutional spread
        commission_bps: float = 1.0,       # 1 bps institutional commission
        tp_bps: float = 20.0,              # +20 bps Take-Profit target
        sl_bps: float = 12.0,              # -12 bps Stop-Loss target
        sl_pct: Optional[float] = None,    # If set (e.g. 0.05), overrides sl_bps (= pct * 10000)
        profile: str = "intraday",         # 'intraday' (bps), 'daily' (EOD hold), or 'swing' (%)
        alpha_source: str = "store",       # 'store' (pure AlphaSignalServer) or 'inline' (local buffer CI)
        rolling_window: int = 45,
        z_entry_threshold: float = 0.80,
        signal_server: Optional[AlphaSignalServer] = None,
        pm_controls: Optional[PMControlService] = None,
        model_version: Optional[str] = None,
        dataset_id: Optional[str] = None,
        feature_source: Optional[str] = None,
    ):
        self.session_id = session_id or f"sim_{uuid.uuid4().hex[:8]}"
        self.db = db or PaperDatabase()
        self.profile = profile
        self.alpha_source = alpha_source.lower()
        # Engine-level ML provenance (explicit > env MODEL_VERSION/DATASET_ID/FEATURE_SOURCE > pointer)
        self.provenance: Dict[str, Optional[str]] = resolve_provenance(
            model_version=model_version, dataset_id=dataset_id, feature_source=feature_source
        )
        self._bar_provenance: Dict[str, Optional[str]] = {}
        self._signal_provenance: Dict[str, Dict[str, Optional[str]]] = {}

        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.trade_notional = trade_notional
        self.max_positions = max_positions
        self.max_daily_loss_pct = max_daily_loss_pct

        # Profile presets
        if profile == "swing":
            self.tp_bps = 150.0   # +1.5%
            self.sl_bps = 75.0    # -0.75%
        elif profile == "daily":
            self.tp_bps = 300.0   # +3.0% daily guard
            self.sl_bps = 150.0   # -1.5% daily guard
        else:
            self.tp_bps = tp_bps  # +20 bps default
            self.sl_bps = sl_bps  # -12 bps default

        # Explicit stop-loss percent (e.g. 5%) wins over profile bps
        if sl_pct is not None and sl_pct > 0:
            self.sl_bps = float(sl_pct) * 10000.0
            # Keep TP from blocking the SL-focused multi-alpha policy
            self.tp_bps = max(self.tp_bps, self.sl_bps * 2.0)

        self.slippage_bps = slippage_bps
        self.commission_bps = commission_bps
        self.rolling_window = rolling_window
        self.z_entry_threshold = z_entry_threshold

        # Serving & PM Control components
        self.signal_server = signal_server or AlphaSignalServer(db=self.db, z_threshold=self.z_entry_threshold)
        self.pm_controls = pm_controls or PMControlService(db=self.db)

        # Engine state
        self.positions: Dict[str, Position] = {}
        self.realized_pnl_cumulative: float = 0.0
        self.daily_start_equity: float = starting_cash
        self.daily_halted: bool = False
        self.peak_equity: float = starting_cash

        # Rolling feature buffers per ticker
        self.mom_buffers: Dict[str, Deque[float]] = {}
        self.vwap_dev_buffers: Dict[str, Deque[float]] = {}
        self.ml_buffers: Dict[str, Deque[float]] = {}
        self.volume_buffers: Dict[str, Deque[float]] = {}
        self.close_buffers: Dict[str, Deque[float]] = {}
        self.cum_vol_price: Dict[str, float] = {}
        self.cum_vol: Dict[str, float] = {}

        # Cross-sectional combined-z map for top-K ticker gate (ensemble top_k_tickers)
        self.cross_section_scores: Dict[str, float] = {}
        self.top_k_tickers: int = self._load_top_k_tickers()
        self.enforce_top_k_gate: bool = True

        # Latency tracking
        self.latencies_ms: List[float] = []

    def _load_top_k_tickers(self) -> int:
        """Read ensemble top_k_tickers (default 5) for current profile horizon."""
        horizon = "DAILY" if self.profile == "daily" else "INTRADAY"
        try:
            cfg = self.db.get_ensemble_config(horizon=horizon, ensemble_id="default")
            if cfg and cfg.get("top_k_tickers"):
                return max(1, int(cfg["top_k_tickers"]))
        except Exception:
            pass
        return max(1, int(self.max_positions))

    def _update_cross_section_score(self, ticker: str, combined_z: float) -> None:
        self.cross_section_scores[ticker] = float(combined_z)

    def _is_in_top_k(self, ticker: str, side: str) -> bool:
        """Gate entries to current top-K by combined z (longs: highest; shorts: lowest)."""
        if not self.enforce_top_k_gate:
            return True
        if not self.cross_section_scores:
            return True
        ranked = sorted(
            self.cross_section_scores.items(),
            key=lambda kv: kv[1],
            reverse=(side == "LONG"),
        )
        top = {t for t, _ in ranked[: self.top_k_tickers]}
        return ticker in top

    def get_top_tickers(self, k: Optional[int] = None) -> List[Tuple[str, float]]:
        """Return current cross-section leaders by combined z (descending)."""
        n = k if k is not None else self.top_k_tickers
        ranked = sorted(self.cross_section_scores.items(), key=lambda kv: kv[1], reverse=True)
        return ranked[: max(1, n)]

    def _remember_signal_provenance(self, ticker: str, sig: Any) -> None:
        prov = {f: getattr(sig, f, None) for f in PROVENANCE_FIELDS}
        if any(is_real_provenance_value(v) for v in prov.values()):
            self._signal_provenance[ticker] = prov

    def _provenance_for(self, ticker: str) -> Dict[str, Optional[str]]:
        """signal provenance > bar payload provenance > engine-level (env/explicit)."""
        merged: Dict[str, Optional[str]] = {}
        sources = (self._signal_provenance.get(ticker, {}), self._bar_provenance, self.provenance)
        for f in PROVENANCE_FIELDS:
            merged[f] = next((src.get(f) for src in sources if is_real_provenance_value(src.get(f))), None)
        return merged

    def on_bar(self, bar: MarketBar) -> Dict[str, Any]:
        """Unified Event-Driven Entrypoint for every 1-minute market bar."""
        start_time = time.perf_counter()

        ticker = bar.ticker_symbol
        ts = bar.timestamp
        close = bar.close_price
        opn = bar.open_price if bar.open_price > 0 else close

        # Calculate or update intraday VWAP
        if bar.vwap is not None:
            vwap = bar.vwap
        else:
            self.cum_vol_price[ticker] = self.cum_vol_price.get(ticker, 0.0) + (close * bar.volume)
            self.cum_vol[ticker] = self.cum_vol.get(ticker, 0.0) + bar.volume
            vwap = (self.cum_vol_price[ticker] / self.cum_vol[ticker]) if self.cum_vol.get(ticker, 0) > 0 else close

        self._bar_provenance = {f: getattr(bar, f, None) for f in PROVENANCE_FIELDS}

        # EOD flatten at 15:55 America/New_York (see common.market_time for the Z/naive policy)
        dt_et = parse_bar_timestamp(ts)
        is_eod = bool(dt_et is not None and is_eod_flat_time(dt_et))

        events_fired: List[str] = []
        position_closed_this_bar = False

        # 1. Update mark price & check stops on existing position
        if ticker in self.positions:
            pos = self.positions[ticker]
            pos.current_mark_price = close
            pos.updated_at = ts

            if pos.side == "LONG":
                pos.unrealized_pnl = (close - pos.average_entry_price) * pos.quantity
                pos.unrealized_pnl_pct = (close - pos.average_entry_price) / pos.average_entry_price
                # Check Stop Loss / Take Profit
                if close <= pos.stop_loss_price:
                    self._close_position(ticker, close, ts, reason="STOP_LOSS")
                    events_fired.append(f"SL_LONG_{ticker}")
                    position_closed_this_bar = True
                elif close >= pos.take_profit_price:
                    self._close_position(ticker, close, ts, reason="TAKE_PROFIT")
                    events_fired.append(f"TP_LONG_{ticker}")
                    position_closed_this_bar = True
            else:  # SHORT
                pos.unrealized_pnl = (pos.average_entry_price - close) * pos.quantity
                pos.unrealized_pnl_pct = (pos.average_entry_price - close) / pos.average_entry_price
                if close >= pos.stop_loss_price:
                    self._close_position(ticker, close, ts, reason="STOP_LOSS")
                    events_fired.append(f"SL_SHORT_{ticker}")
                    position_closed_this_bar = True
                elif close <= pos.take_profit_price:
                    self._close_position(ticker, close, ts, reason="TAKE_PROFIT")
                    events_fired.append(f"TP_SHORT_{ticker}")
                    position_closed_this_bar = True

            # Persist updated position if still open
            if ticker in self.positions:
                self.db.upsert_position(asdict(self.positions[ticker]))

        # 2. Check EOD Flatting at 15:55
        if is_eod and len(self.positions) > 0:
            open_tickers = list(self.positions.keys())
            for t in open_tickers:
                t_price = close if t == ticker else self.positions[t].current_mark_price
                self._close_position(t, t_price, ts, reason="EOD_FLAT")
                events_fired.append(f"EOD_FLAT_{t}")

        # 3. Compute Portfolio Equity & Check Circuit Breaker
        total_unrealized = sum(p.unrealized_pnl for p in self.positions.values())
        invested_equity = sum(p.current_mark_price * p.quantity for p in self.positions.values())
        total_equity = self.cash + invested_equity

        if total_equity > self.peak_equity:
            self.peak_equity = total_equity

        drawdown_pct = (total_equity - self.peak_equity) / self.peak_equity if self.peak_equity > 0 else 0.0

        daily_pnl_pct = (total_equity - self.daily_start_equity) / self.daily_start_equity
        if daily_pnl_pct <= self.max_daily_loss_pct and not self.daily_halted:
            logger.warning(f"CIRCUIT BREAKER TRIGGERED: Daily loss {daily_pnl_pct*100:.2f}% <= {self.max_daily_loss_pct*100:.2f}%. Liquidating all positions.")
            self.daily_halted = True
            for t in list(self.positions.keys()):
                t_price = close if t == ticker else self.positions[t].current_mark_price
                self._close_position(t, t_price, ts, reason="CIRCUIT_BREAKER")
                events_fired.append(f"CIRCUIT_BREAKER_{t}")

        # 4. Generate Signal and Potential Entry
        signal_val = 0.0
        pm_blocked = self.pm_controls.is_entry_blocked()
        if pm_blocked:
            events_fired.append(f"PM_BLOCK_ACTIVE_{ticker}")
        elif not self.daily_halted and not is_eod:
            if self.profile == "daily":
                # Daily signal policy (Rubric M1-38/M1-39):
                # 1. Check market open window (09:30 - 09:35 ET)
                is_market_open = False
                effective_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if dt_et is not None:
                    is_market_open = is_market_open_window(dt_et)
                    effective_date = dt_et.strftime("%Y-%m-%d")
                else:
                    is_market_open = True

                daily_sig = self.signal_server.get_daily_signal(ticker=ticker, effective_date=effective_date)
                signal_val = daily_sig.z_score
                self._remember_signal_provenance(ticker, daily_sig)
                self._update_cross_section_score(ticker, signal_val)

                # Signal exit for open daily positions (weak/FLAT or flip)
                if not position_closed_this_bar and ticker in self.positions:
                    pos = self.positions[ticker]
                    if daily_sig.action == "FLAT" or (
                        pos.side == "LONG" and daily_sig.action == "SELL"
                    ) or (
                        pos.side == "SHORT" and daily_sig.action == "BUY"
                    ):
                        self._close_position(ticker, close, ts, reason="SIGNAL_EXIT")
                        events_fired.append(f"SIGNAL_EXIT_{pos.side}_{ticker}")
                        position_closed_this_bar = True

                if (
                    is_market_open
                    and not position_closed_this_bar
                    and ticker not in self.positions
                    and len(self.positions) < self.max_positions
                    and self.cash >= self.trade_notional
                ):
                    if daily_sig.action == "BUY" and self._is_in_top_k(ticker, "LONG"):
                        self._open_position(ticker, close, ts, side="LONG")
                        events_fired.append(f"ENTRY_LONG_{ticker}")
                    elif daily_sig.action == "SELL" and self._is_in_top_k(ticker, "SHORT"):
                        self._open_position(ticker, close, ts, side="SHORT")
                        events_fired.append(f"ENTRY_SHORT_{ticker}")
                    elif daily_sig.action in ("BUY", "SELL"):
                        events_fired.append(f"TOPK_GATE_BLOCK_{ticker}")
            else:
                # Always refresh rolling buffers for catalog/Feast fallbacks
                signal_val = self._update_signal(bar, vwap)

                sig = self.signal_server.get_intraday_signal(
                    ticker=ticker,
                    bar_price=close,
                    bar_timestamp=ts,
                    p_ml=bar.p_ml,
                    rolling_buffers={
                        "mom": self.mom_buffers,
                        "vwap": self.vwap_dev_buffers,
                        "ml": self.ml_buffers,
                        "volume": self.volume_buffers,
                        "close": self.close_buffers,
                    },
                )
                if self.alpha_source == "store":
                    signal_val = sig.z_score
                self._remember_signal_provenance(ticker, sig)
                self._update_cross_section_score(ticker, float(signal_val))

                # Exit when store signal weakens / flips (before considering new entries)
                if (
                    self.alpha_source == "store"
                    and not position_closed_this_bar
                    and ticker in self.positions
                ):
                    pos = self.positions[ticker]
                    should_exit = False
                    if sig.action == "FLAT":
                        should_exit = True
                    elif pos.side == "LONG" and sig.action == "SELL":
                        should_exit = True
                    elif pos.side == "SHORT" and sig.action == "BUY":
                        should_exit = True
                    if should_exit:
                        self._close_position(ticker, close, ts, reason="SIGNAL_EXIT")
                        events_fired.append(f"SIGNAL_EXIT_{pos.side}_{ticker}")
                        position_closed_this_bar = True

                # Check capacity and entry conditions
                if (
                    not position_closed_this_bar
                    and ticker not in self.positions
                    and len(self.positions) < self.max_positions
                    and self.cash >= self.trade_notional
                ):
                    if self.alpha_source == "store":
                        # Pure store mode: ONLY open when Alpha Serving produces BUY or SELL
                        if sig.action == "BUY" and self._is_in_top_k(ticker, "LONG"):
                            self._open_position(ticker, close, ts, side="LONG")
                            events_fired.append(f"ENTRY_LONG_{ticker}")
                        elif sig.action == "SELL" and self._is_in_top_k(ticker, "SHORT"):
                            self._open_position(ticker, close, ts, side="SHORT")
                            events_fired.append(f"ENTRY_SHORT_{ticker}")
                        elif sig.action in ("BUY", "SELL"):
                            events_fired.append(f"TOPK_GATE_BLOCK_{ticker}")
                    else:
                        # Inline / CI mode: use local rolling buffer threshold
                        if signal_val >= self.z_entry_threshold and self._is_in_top_k(ticker, "LONG"):
                            self._open_position(ticker, close, ts, side="LONG")
                            events_fired.append(f"ENTRY_LONG_{ticker}")
                        elif signal_val <= -self.z_entry_threshold and self._is_in_top_k(ticker, "SHORT"):
                            self._open_position(ticker, close, ts, side="SHORT")
                            events_fired.append(f"ENTRY_SHORT_{ticker}")
                        elif abs(signal_val) >= self.z_entry_threshold:
                            events_fired.append(f"TOPK_GATE_BLOCK_{ticker}")

        # Record market bar into session store
        try:
            bar_record = {
                "session_id": self.session_id,
                "ticker_symbol": ticker,
                "event_timestamp": ts,
                "open_price": bar.open_price,
                "high_price": bar.high_price,
                "low_price": bar.low_price,
                "close_price": bar.close_price,
                "volume": bar.volume,
                "vwap": vwap,
                "p_ml": bar.p_ml,
            }
            if hasattr(self.db, "record_bar"):
                self.db.record_bar(bar_record)
        except Exception:
            pass

        # 5. Measure Latency & Record Snapshot
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        self.latencies_ms.append(elapsed_ms)

        # Snapshot state
        snapshot = {
            "session_id": self.session_id,
            "cash_balance": self.cash,
            "invested_equity": invested_equity,
            "total_portfolio_value": total_equity,
            "realized_pnl_cumulative": self.realized_pnl_cumulative,
            "unrealized_pnl_current": total_unrealized,
            "rolling_sharpe_ratio": 0.0,
            "max_drawdown_pct": drawdown_pct,
            "active_positions_count": len(self.positions),
            "latency_ms": elapsed_ms,
            "recorded_at": ts,
        }
        self.db.record_snapshot(snapshot)

        return {
            "ticker": ticker,
            "timestamp": ts,
            "events": events_fired,
            "signal": signal_val,
            "equity": total_equity,
            "cash": self.cash,
            "positions_count": len(self.positions),
            "latency_ms": elapsed_ms,
        }

    def _update_signal(self, bar: MarketBar, vwap: float) -> float:
        """Update rolling buffers and compute composite intraday Z-score."""
        ticker = bar.ticker_symbol
        close = bar.close_price
        opn = bar.open_price if bar.open_price > 0 else close

        # Initialize buffers
        if ticker not in self.mom_buffers:
            self.mom_buffers[ticker] = deque(maxlen=self.rolling_window)
            self.vwap_dev_buffers[ticker] = deque(maxlen=self.rolling_window)
            self.ml_buffers[ticker] = deque(maxlen=self.rolling_window)
            self.volume_buffers[ticker] = deque(maxlen=self.rolling_window)
            self.close_buffers[ticker] = deque(maxlen=self.rolling_window)

        # Compute factor components
        mom_1m = (close - opn) / opn if opn > 0 else 0.0
        vwap_dev = (close - vwap) / vwap if vwap > 0 else 0.0
        ml_centered = (bar.p_ml - 0.50) if bar.p_ml is not None else 0.0

        self.mom_buffers[ticker].append(mom_1m)
        self.vwap_dev_buffers[ticker].append(vwap_dev)
        self.ml_buffers[ticker].append(ml_centered)
        self.volume_buffers[ticker].append(float(bar.volume))
        self.close_buffers[ticker].append(close)

        if len(self.mom_buffers[ticker]) < 15:
            return 0.0

        def _zscore(buf: Deque[float]) -> float:
            arr = np.array(buf)
            std = np.std(arr)
            return float((arr[-1] - np.mean(arr)) / std) if std > 1e-8 else 0.0

        z_mom = _zscore(self.mom_buffers[ticker])
        z_vwap = _zscore(self.vwap_dev_buffers[ticker])
        z_ml = _zscore(self.ml_buffers[ticker]) if bar.p_ml is not None else 0.0

        # Weighted combination: 0.50*Mom + 0.30*VWAP + 0.20*ML
        z_composite = (0.50 * z_mom) + (0.30 * z_vwap) + (0.20 * z_ml)
        return float(z_composite)

    def _open_position(self, ticker: str, price: float, ts: str, side: str) -> None:
        """Execute entry order with slippage and commission."""
        slip_mult = (1.0 + self.slippage_bps / 10000.0) if side == "LONG" else (1.0 - self.slippage_bps / 10000.0)
        fill_price = price * slip_mult
        quantity = round(self.trade_notional / fill_price, 4)
        commission = (self.trade_notional * self.commission_bps) / 10000.0

        total_cost = (fill_price * quantity) + commission
        if total_cost > self.cash:
            return

        order_id = f"ord_{uuid.uuid4().hex[:12]}"
        trade_id = f"trd_{uuid.uuid4().hex[:12]}"

        # Stop-Loss & Take-Profit targets in bps
        if side == "LONG":
            sl_price = round(fill_price * (1.0 - self.sl_bps / 10000.0), 4)
            tp_price = round(fill_price * (1.0 + self.tp_bps / 10000.0), 4)
        else:
            sl_price = round(fill_price * (1.0 + self.sl_bps / 10000.0), 4)
            tp_price = round(fill_price * (1.0 - self.tp_bps / 10000.0), 4)

        order_record = {
            "order_id": order_id,
            "session_id": self.session_id,
            "ticker_symbol": ticker,
            "order_type": "MARKET",
            "direction": "BUY" if side == "LONG" else "SELL",
            "quantity": quantity,
            "requested_price": price,
            "status": "FILLED",
            "bar_timestamp": ts,
            "created_at": ts,
            **self._provenance_for(ticker),
        }

        # Idempotency check
        if not self.db.record_order(order_record):
            logger.info(f"Idempotent duplicate order ignored for {ticker} at {ts}")
            return

        trade_record = {
            "trade_id": trade_id,
            "order_id": order_id,
            "session_id": self.session_id,
            "ticker_symbol": ticker,
            "direction": "BUY" if side == "LONG" else "SELL",
            "fill_price": fill_price,
            "fill_quantity": quantity,
            "slippage_bps": self.slippage_bps,
            "commission": commission,
            "realized_pnl": 0.0,
            "executed_at": ts,
            **self._provenance_for(ticker),
        }
        self.db.record_trade(trade_record)

        self.cash -= total_cost
        pos = Position(
            ticker_symbol=ticker,
            session_id=self.session_id,
            side=side,
            quantity=quantity,
            average_entry_price=fill_price,
            current_mark_price=price,
            unrealized_pnl=0.0,
            unrealized_pnl_pct=0.0,
            stop_loss_price=sl_price,
            take_profit_price=tp_price,
            entry_time=ts,
            updated_at=ts,
        )
        self.positions[ticker] = pos
        self.db.upsert_position(asdict(pos))

    def _close_position(self, ticker: str, price: float, ts: str, reason: str) -> None:
        """Close an existing position with slippage, commission and PnL accounting."""
        if ticker not in self.positions:
            return

        pos = self.positions[ticker]
        side = pos.side
        exit_dir = "SELL" if side == "LONG" else "BUY"

        slip_mult = (1.0 - self.slippage_bps / 10000.0) if side == "LONG" else (1.0 + self.slippage_bps / 10000.0)
        fill_price = price * slip_mult
        commission = (fill_price * pos.quantity * self.commission_bps) / 10000.0

        if side == "LONG":
            gross_pnl = (fill_price - pos.average_entry_price) * pos.quantity
            returned_capital = (fill_price * pos.quantity) - commission
        else:
            gross_pnl = (pos.average_entry_price - fill_price) * pos.quantity
            returned_capital = (pos.average_entry_price * pos.quantity) + gross_pnl - commission

        net_pnl = gross_pnl - commission

        order_id = f"ord_{uuid.uuid4().hex[:12]}"
        trade_id = f"trd_{uuid.uuid4().hex[:12]}"

        order_record = {
            "order_id": order_id,
            "session_id": self.session_id,
            "ticker_symbol": ticker,
            "order_type": reason,
            "direction": exit_dir,
            "quantity": pos.quantity,
            "requested_price": price,
            "status": "FILLED",
            "bar_timestamp": ts,
            "created_at": ts,
            **self._provenance_for(ticker),
        }
        self.db.record_order(order_record)

        trade_record = {
            "trade_id": trade_id,
            "order_id": order_id,
            "session_id": self.session_id,
            "ticker_symbol": ticker,
            "direction": exit_dir,
            "fill_price": fill_price,
            "fill_quantity": pos.quantity,
            "slippage_bps": self.slippage_bps,
            "commission": commission,
            "realized_pnl": net_pnl,
            "executed_at": ts,
            **self._provenance_for(ticker),
        }
        self.db.record_trade(trade_record)

        self.cash += returned_capital
        self.realized_pnl_cumulative += net_pnl
        del self.positions[ticker]
        self.db.remove_position(ticker, session_id=self.session_id)
