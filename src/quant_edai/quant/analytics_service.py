"""Analytics service for Quant Alpha Quality & Performance Attribution.

Decouples metric computation, validation, and data extraction from presentation.
Supports both Intraday Live/Paper Execution (Phase 6) and Daily Walk-Forward Backtesting (Phase 5).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from quant_edai.quant.paper_db import PaperDatabase

logger = logging.getLogger(__name__)


@dataclass
class IntradayPerformanceSummary:
    """Summary metrics computed strictly on completed closed roundtrips."""
    total_trades_count: int = 0
    closed_trades_count: int = 0
    open_trades_count: int = 0
    winning_trades_count: int = 0
    losing_trades_count: int = 0
    breakeven_trades_count: int = 0
    win_rate_pct: float = 0.0
    profit_factor: float = 0.0
    payoff_ratio: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    gross_pnl: float = 0.0
    total_commission: float = 0.0
    net_realized_pnl: float = 0.0
    total_slippage_est: float = 0.0
    max_drawdown_pct: float = 0.0
    circuit_breaker_threshold_pct: float = -2.0
    circuit_breaker_triggered: bool = False


class AlphaAnalyticsService:
    """Centralized service for alpha quality, execution attribution, and backtest scorecard."""

    def __init__(
        self,
        paper_db: Optional[PaperDatabase] = None,
        phase5_summary_path: str = "evidence/phase5/backtest_summary.json",
        attribution_report_path: str = "evidence/phase5/novel_idea_attribution_report.md",
    ) -> None:
        self.db = paper_db or PaperDatabase()
        self.phase5_path = Path(phase5_summary_path)
        self.attribution_path = Path(attribution_report_path)

    # -------------------------------------------------------------------------
    # INTRADAY EXECUTION ANALYTICS (PHASE 6)
    # -------------------------------------------------------------------------
    def compute_intraday_performance(
        self,
        session_id: Optional[str] = None,
    ) -> Tuple[IntradayPerformanceSummary, pd.DataFrame, pd.DataFrame]:
        """Compute performance metrics strictly from closed trades in paper trading database.

        Returns
        -------
        summary : IntradayPerformanceSummary
        df_closed : pd.DataFrame
            DataFrame of closed round-trips with correct PnL and friction accounting.
        df_snapshots : pd.DataFrame
            Portfolio snapshots for drawdown and equity progression.
        """
        all_trades = self.db.get_trades_with_orders(session_id=session_id, limit=2000)
        snapshots_df = self.db.get_snapshots(limit=2000)

        summary = IntradayPerformanceSummary()
        summary.total_trades_count = len(all_trades)

        if all_trades.empty:
            return summary, pd.DataFrame(), snapshots_df

        # Filter strictly for completed closed positions:
        # A trade is a closing trade if realized_pnl is non-zero OR order_type indicates an exit
        is_exit_mask = (all_trades["realized_pnl"].abs() > 1e-4) | (
            all_trades["order_type"].isin(["STOP_LOSS", "TAKE_PROFIT", "EOD_FLAT", "CIRCUIT_BREAKER"])
        )
        df_closed = all_trades[is_exit_mask].copy()
        df_open = all_trades[~is_exit_mask].copy()

        summary.closed_trades_count = len(df_closed)
        summary.open_trades_count = len(df_open)

        if df_closed.empty:
            return summary, df_closed, snapshots_df

        df_closed["realized_pnl"] = df_closed["realized_pnl"].astype(float)
        df_closed["commission"] = df_closed["commission"].astype(float)
        df_closed["fill_price"] = df_closed["fill_price"].astype(float)
        df_closed["fill_quantity"] = df_closed["fill_quantity"].astype(float)
        df_closed["slippage_bps"] = df_closed.get("slippage_bps", 5.0).astype(float)

        # Estimate gross PnL before exit commission
        df_closed["gross_pnl"] = df_closed["realized_pnl"] + df_closed["commission"]
        df_closed["slippage_cost"] = (
            df_closed["fill_price"] * df_closed["fill_quantity"] * (df_closed["slippage_bps"] / 10000.0)
        )

        winning = df_closed[df_closed["realized_pnl"] > 1e-4]
        losing = df_closed[df_closed["realized_pnl"] < -1e-4]
        breakeven = df_closed[df_closed["realized_pnl"].abs() <= 1e-4]

        summary.winning_trades_count = len(winning)
        summary.losing_trades_count = len(losing)
        summary.breakeven_trades_count = len(breakeven)

        # Win Rate on completed closed roundtrips
        summary.win_rate_pct = (len(winning) / len(df_closed)) * 100.0 if len(df_closed) > 0 else 0.0

        gross_profit = float(winning["realized_pnl"].sum())
        gross_loss = float(abs(losing["realized_pnl"].sum()))
        summary.gross_profit = gross_profit
        summary.gross_loss = gross_loss
        summary.profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)

        avg_win = float(winning["realized_pnl"].mean()) if not winning.empty else 0.0
        avg_loss = float(abs(losing["realized_pnl"].mean())) if not losing.empty else 0.0
        summary.avg_win = avg_win
        summary.avg_loss = avg_loss
        summary.payoff_ratio = (avg_win / avg_loss) if avg_loss > 0 else (avg_win if avg_win > 0 else 0.0)

        summary.net_realized_pnl = float(df_closed["realized_pnl"].sum())
        summary.total_commission = float(all_trades["commission"].sum())
        summary.total_slippage_est = float(df_closed["slippage_cost"].sum())
        summary.gross_pnl = float(df_closed["gross_pnl"].sum())

        # Max drawdown from snapshots
        if not snapshots_df.empty and "max_drawdown_pct" in snapshots_df.columns:
            min_dd = float(snapshots_df["max_drawdown_pct"].min())
            summary.max_drawdown_pct = min_dd * 100.0
            summary.circuit_breaker_triggered = min_dd <= (summary.circuit_breaker_threshold_pct / 100.0)

        return summary, df_closed, snapshots_df

    # -------------------------------------------------------------------------
    # DAILY WALK-FORWARD ANALYTICS (PHASE 5)
    # -------------------------------------------------------------------------
    def load_phase5_backtest_data(self) -> Optional[Dict[str, Any]]:
        """Load and parse Phase 5 backtest summary contract."""
        if not self.phase5_path.exists():
            return None

        try:
            with open(self.phase5_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.error(f"Error reading Phase 5 backtest summary: {e}")
            return None

    def load_attribution_report(self) -> str:
        """Load Markdown text of Novel Idea 2 Attribution Report."""
        if not self.attribution_path.exists():
            return "Attribution report not found."
        try:
            with open(self.attribution_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            return f"Error loading attribution report: {e}"

    def get_regime_weight_matrix(self) -> pd.DataFrame:
        """Return standardized factor weight matrix across market regimes."""
        return pd.DataFrame([
            {"Market Regime": "Low Volatility", "Momentum (Trend)": "50%", "Mean Reversion (Tech)": "25%", "ML Classifier": "25%", "Rationale": "Strong trend continuation; low whipsaw risk"},
            {"Market Regime": "Normal", "Momentum (Trend)": "35%", "Mean Reversion (Tech)": "35%", "ML Classifier": "30%", "Rationale": "Balanced multi-factor allocation"},
            {"Market Regime": "High Volatility", "Momentum (Trend)": "15%", "Mean Reversion (Tech)": "45%", "ML Classifier": "40%", "Rationale": "Defensive shift away from trend following"},
            {"Market Regime": "Crisis / Shock", "Momentum (Trend)": "10%", "Mean Reversion (Tech)": "50%", "ML Classifier": "40%", "Rationale": "Capital preservation; mean-reversion focus"},
        ])

    def get_ablation_comparison_matrix(self) -> pd.DataFrame:
        """Return factor ablation and position sizing comparisons from Phase 5."""
        return pd.DataFrame([
            {
                "Configuration": "S&P 500 ETF (SPY Benchmark)",
                "Weighting Model": "Market-Cap Weighted",
                "Position Sizing": "Passive Buy & Hold",
                "Cumulative Return": "+10.08%",
                "Annualized Sharpe": "0.51",
                "Sortino Ratio": "0.69",
                "Max Drawdown": "-16.02%",
                "Calmar Ratio": "0.74",
            },
            {
                "Configuration": "Fixed-Weight Baseline (33/33/34)",
                "Weighting Model": "Static Equal Factor Weights",
                "Position Sizing": "Fixed $10k Notional",
                "Cumulative Return": "+270.57%",
                "Annualized Sharpe": "8.78",
                "Sortino Ratio": "20.70",
                "Max Drawdown": "-9.72%",
                "Calmar Ratio": "37.15",
            },
            {
                "Configuration": "Equal-Weighted Constituents (1/N)",
                "Weighting Model": "Regime-Adaptive Weights",
                "Position Sizing": "Equal Capital per Asset",
                "Cumulative Return": "+54.96%",
                "Annualized Sharpe": "2.76",
                "Sortino Ratio": "4.38",
                "Max Drawdown": "-9.25%",
                "Calmar Ratio": "7.21",
            },
            {
                "Configuration": "Regime-Adaptive + Risk Parity (Final)",
                "Weighting Model": "Dynamic Regime-Adaptive",
                "Position Sizing": "Inverse Volatility (1/σ)",
                "Cumulative Return": "+49.42%",
                "Annualized Sharpe": "2.65",
                "Sortino Ratio": "4.10",
                "Max Drawdown": "-11.47%",
                "Calmar Ratio": "5.18",
            },
        ])
