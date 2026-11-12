"""Portfolio Manager (PM) Governance & Emergency Controls.

Provides operational overrides and model promotion gates:
1. Promote / Demote Alpha versions (Rule 4: max 1 active version per alpha_id).
2. Emergency Force Flatten: Immediately closes all open positions across tickers.
3. Block New Entries: Kill-switch allowing risk exits but blocking new risk exposure.
4. Audit Trail: Immutable logging of all PM interventions into PostgreSQL pm_governance_audit.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from typing import Any, Dict, List, Optional

from quant_edai.common.ml_provenance import PROVENANCE_FIELDS, is_real_provenance_value, resolve_provenance
from quant_edai.quant.alpha_registry import AlphaRegistry, AlphaStatus
from quant_edai.quant.paper_db import PaperDatabase

logger = logging.getLogger(__name__)

# File-based flag for multi-process PM block state persistence
PM_STATE_FILE = os.getenv("PM_STATE_FILE", "/tmp/pm_trading_controls.json")


class PMControlService:
    """Manages PM governance actions, model promotions, and emergency kill-switches."""

    def __init__(
        self,
        db: Optional[PaperDatabase] = None,
        registry: Optional[AlphaRegistry] = None,
        state_file: Optional[str] = None,
    ) -> None:
        self.db = db or PaperDatabase()
        self.registry = registry or AlphaRegistry(db=self.db)
        self.state_file = state_file or os.getenv("PM_STATE_FILE", PM_STATE_FILE)

    # -------------------------------------------------------------------------
    # 1. ALPHA MODEL LIFECYCLE GATES
    # -------------------------------------------------------------------------
    def promote_alpha_to_paper(
        self,
        alpha_id: str,
        version: str,
        pm_user: str = "PM",
        reason: str = "Approved for Paper Trial soak",
    ) -> bool:
        """Promote candidate alpha to paper trading trial."""
        return self.registry.promote_to_paper(alpha_id, version, pm_user=pm_user, reason=reason)

    def promote_alpha_to_live(
        self,
        alpha_id: str,
        version: str,
        pm_user: str = "PM",
        reason: str = "Approved for Production Live Execution",
    ) -> bool:
        """Promote paper trial alpha to production live execution."""
        return self.registry.promote_to_live(alpha_id, version, pm_user=pm_user, reason=reason)

    def demote_alpha(
        self,
        alpha_id: str,
        version: str,
        pm_user: str = "PM",
        reason: str = "Performance decay / PM demotion",
    ) -> bool:
        """Demote an active alpha to retired."""
        return self.registry.demote_or_retire(alpha_id, version, pm_user=pm_user, reason=reason)

    # -------------------------------------------------------------------------
    # 2. EMERGENCY KILL-SWITCHES & OVERRIDES
    # -------------------------------------------------------------------------
    def _position_provenance(self, session_id: str, ticker: str) -> Dict[str, Optional[str]]:
        """Provenance for a forced exit: inherit from the position's latest fill, else env."""
        prov = resolve_provenance()
        try:
            for tr in reversed(self.db.get_recent_trades(session_id=session_id, limit=500) or []):
                if tr.get("ticker_symbol") == ticker and all(
                    is_real_provenance_value(tr.get(f)) for f in PROVENANCE_FIELDS
                ):
                    return {f: tr[f] for f in PROVENANCE_FIELDS}
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("Could not inherit provenance for PM flatten of %s: %s", ticker, e)
        return prov

    def force_flatten_all_positions(
        self,
        session_id: Optional[str] = None,
        pm_user: str = "PM",
        reason: str = "EMERGENCY_PM_FLATTEN",
    ) -> int:
        """Liquidate all open positions immediately and record audit action."""
        positions = self.db.get_positions()
        if session_id:
            positions = [p for p in positions if p.get("session_id") == session_id]

        closed_count = 0
        now_ts = datetime.now(timezone.utc).isoformat()

        for pos in positions:
            ticker = pos["ticker_symbol"]
            mark_p = float(pos["current_mark_price"])
            qty = float(pos["quantity"])
            side = pos["side"]
            exit_dir = "SELL" if side == "LONG" else "BUY"

            # Compute realized PnL
            if side == "LONG":
                gross_pnl = (mark_p - float(pos["average_entry_price"])) * qty
            else:
                gross_pnl = (float(pos["average_entry_price"]) - mark_p) * qty
            comm = 1.00
            net_pnl = gross_pnl - comm

            # Use session bar timestamp if available, else current wall-clock
            pos_ts = pos.get("updated_at") or pos.get("entry_time")
            trade_ts = pos_ts if pos_ts else now_ts

            prov = self._position_provenance(pos["session_id"], ticker)
            order_id = f"ord_pm_flat_{ticker}_{datetime.now().strftime('%H%M%S')}"
            trade_id = f"trd_pm_flat_{ticker}_{datetime.now().strftime('%H%M%S')}"

            # Record emergency order & trade
            self.db.record_order({
                "order_id": order_id,
                "session_id": pos["session_id"],
                "ticker_symbol": ticker,
                "order_type": "PM_FORCE_FLAT",
                "direction": exit_dir,
                "quantity": qty,
                "requested_price": mark_p,
                "status": "FILLED",
                "bar_timestamp": trade_ts,
                "created_at": trade_ts,
                **prov,
            })
            self.db.record_trade({
                "trade_id": trade_id,
                "order_id": order_id,
                "session_id": pos["session_id"],
                "ticker_symbol": ticker,
                "direction": exit_dir,
                "fill_price": mark_p,
                "fill_quantity": qty,
                "slippage_bps": 5.0,
                "commission": comm,
                "realized_pnl": net_pnl,
                "executed_at": trade_ts,
                **prov,
            })
            self.db.remove_position(ticker, session_id=pos["session_id"])
            closed_count += 1

        # Audit log
        self.db.record_pm_audit({
            "action_type": "FORCE_FLATTEN",
            "alpha_id": "ALL",
            "version": "ALL",
            "executed_by": pm_user,
            "reason": reason,
            "details": {"positions_closed": closed_count, "session_id": session_id},
        })
        logger.warning(f"PM FORCE FLATTEN executed by {pm_user}: Closed {closed_count} positions.")
        return closed_count

    def set_block_new_entries(
        self,
        blocked: bool,
        pm_user: str = "PM",
        reason: str = "Risk containment",
    ) -> bool:
        """Toggle new entry blocking (persisted across processes via state file)."""
        state = {"blocked": blocked, "updated_by": pm_user, "updated_at": datetime.now(timezone.utc).isoformat(), "reason": reason}
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not write PM state file: {e}")

        # Audit log
        action = "BLOCK_NEW_ENTRIES" if blocked else "RESUME_ENTRIES"
        self.db.record_pm_audit({
            "action_type": action,
            "executed_by": pm_user,
            "reason": reason,
            "details": state,
        })
        logger.info(f"PM {action} applied by {pm_user}.")
        return blocked

    def is_entry_blocked(self) -> bool:
        """Check if PM has blocked new order entries."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return bool(data.get("blocked", False))
            except Exception:
                pass
        return False

    def get_audit_trail(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch PM audit trail records."""
        return self.db.get_pm_audits(limit=limit)
