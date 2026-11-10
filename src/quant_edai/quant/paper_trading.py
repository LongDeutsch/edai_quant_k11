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

from quant_edai.quant.paper_db import PaperDatabase

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
        profile: str = "intraday",         # 'intraday' (bps) or 'swing' (%)
        rolling_window: int = 45,
        z_entry_threshold: float = 0.80,
    ):
        self.session_id = session_id or f"sim_{uuid.uuid4().hex[:8]}"
        self.db = db or PaperDatabase()

        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.trade_notional = trade_notional
        self.max_positions = max_positions
        self.max_daily_loss_pct = max_daily_loss_pct

        # Profile presets
        if profile == "swing":
            self.tp_bps = 150.0   # +1.5%
            self.sl_bps = 75.0    # -0.75%
        else:
            self.tp_bps = tp_bps  # +20 bps default
            self.sl_bps = sl_bps  # -12 bps default

        self.slippage_bps = slippage_bps
        self.commission_bps = commission_bps
        self.rolling_window = rolling_window
        self.z_entry_threshold = z_entry_threshold

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
        self.cum_vol_price: Dict[str, float] = {}
        self.cum_vol: Dict[str, float] = {}

        # Latency tracking
        self.latencies_ms: List[float] = []

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

        # Parse hour/minute for EOD Flattening (15:55 EST check)
        is_eod = False
        try:
            # Handle ISO string e.g. "2024-03-15T15:55:00" or "2024-03-15 15:55:00"
            clean_ts = ts.replace("T", " ")
            dt = datetime.fromisoformat(clean_ts.split("+")[0].split("Z")[0])
            if dt.hour == 15 and dt.minute >= 55:
                is_eod = True
            elif dt.hour >= 16:
                is_eod = True
        except Exception:
            pass

        events_fired: List[str] = []

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
                elif close >= pos.take_profit_price:
                    self._close_position(ticker, close, ts, reason="TAKE_PROFIT")
                    events_fired.append(f"TP_LONG_{ticker}")
            else:  # SHORT
                pos.unrealized_pnl = (pos.average_entry_price - close) * pos.quantity
                pos.unrealized_pnl_pct = (pos.average_entry_price - close) / pos.average_entry_price
                if close >= pos.stop_loss_price:
                    self._close_position(ticker, close, ts, reason="STOP_LOSS")
                    events_fired.append(f"SL_SHORT_{ticker}")
                elif close <= pos.take_profit_price:
                    self._close_position(ticker, close, ts, reason="TAKE_PROFIT")
                    events_fired.append(f"TP_SHORT_{ticker}")

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
        if not self.daily_halted and not is_eod:
            signal_val = self._update_signal(bar, vwap)

            # Check capacity and entry conditions
            if (
                ticker not in self.positions
                and len(self.positions) < self.max_positions
                and self.cash >= self.trade_notional
            ):
                if signal_val >= self.z_entry_threshold:
                    self._open_position(ticker, close, ts, side="LONG")
                    events_fired.append(f"ENTRY_LONG_{ticker}")
                elif signal_val <= -self.z_entry_threshold:
                    self._open_position(ticker, close, ts, side="SHORT")
                    events_fired.append(f"ENTRY_SHORT_{ticker}")

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

        # Compute factor components
        mom_1m = (close - opn) / opn
        vwap_dev = (close - vwap) / vwap if vwap > 0 else 0.0
        ml_centered = (bar.p_ml - 0.50) if bar.p_ml is not None else 0.0

        self.mom_buffers[ticker].append(mom_1m)
        self.vwap_dev_buffers[ticker].append(vwap_dev)
        self.ml_buffers[ticker].append(ml_centered)

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
        }
        self.db.record_trade(trade_record)

        self.cash += returned_capital
        self.realized_pnl_cumulative += net_pnl
        del self.positions[ticker]
        self.db.remove_position(ticker, session_id=self.session_id)
