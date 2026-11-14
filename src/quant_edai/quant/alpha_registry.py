"""Alpha Model Registry & Lifecycle Governance Engine.

Implements MLOps-style model registry for quantitative alphas:
RESEARCH -> CANDIDATE -> PAPER_TRIAL -> PROMOTED_ACTIVE -> RETIRED

Key Business Rules:
- Max 1 active version per alpha_id (promoting a new version auto-retires previous active version).
- Multi-alpha support: returns all distinct active alpha_ids for ensemble serving.
- Full audit trail logging into pm_governance_audit.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from quant_edai.common.ml_provenance import PROVENANCE_FIELDS, resolve_provenance
from quant_edai.quant.paper_db import PaperDatabase

logger = logging.getLogger(__name__)


class AlphaStatus(str, Enum):
    RESEARCH = "RESEARCH"
    CANDIDATE = "CANDIDATE"
    PAPER_TRIAL = "PAPER_TRIAL"
    PROMOTED_ACTIVE = "PROMOTED_ACTIVE"
    APPROVED_FROZEN = "APPROVED_FROZEN"
    RETIRED = "RETIRED"


class AlphaHorizon(str, Enum):
    INTRADAY = "INTRADAY"
    DAILY = "DAILY"


@dataclass
class QualityGateResult:
    passed: bool
    score: float
    reasons: List[str]


class AlphaRegistry:
    """Manages alpha registration, quality gates, lifecycle promotions, and active ensembles."""

    # Standard quantitative gate thresholds for candidate promotion
    MIN_IC_MEAN: float = 0.05
    MIN_SHARPE: float = 1.50
    MAX_DRAWDOWN: float = -0.20

    def __init__(self, db: Optional[PaperDatabase] = None) -> None:
        self.db = db or PaperDatabase()
        self.db.init_db(reset=False)

    def evaluate_quality_gate(self, metrics: Any, version: Optional[str] = None) -> QualityGateResult:
        """Evaluate if backtest metrics pass institutional quality gate.
        Accepts either a metrics dictionary, or (alpha_id, version).
        """
        if isinstance(metrics, str):
            alpha_id = metrics
            rec = self.get_alpha(alpha_id, version)
            if not rec:
                return QualityGateResult(passed=False, score=0.0, reasons=[f"Alpha {alpha_id}:{version} not found in registry"])
            metrics = rec.get("out_of_sample_metrics") or rec.get("metrics_snapshot") or {}
            if isinstance(metrics, str):
                try:
                    metrics = json.loads(metrics)
                except Exception:
                    metrics = {}

        metrics = metrics or {}
        reasons = []
        ic = float(metrics.get("ic_mean", metrics.get("ic", 0.0)))
        raw_sharpe = metrics.get("sharpe", 0.0)
        sharpe: Optional[float]
        if raw_sharpe is None:
            sharpe = None
        else:
            try:
                sharpe = float(raw_sharpe)
            except (TypeError, ValueError):
                sharpe = None
            else:
                if sharpe != sharpe or sharpe in (float("inf"), float("-inf")):
                    sharpe = None
        mdd = float(metrics.get("max_drawdown", metrics.get("mdd", -1.0)))

        passed = True
        if ic < self.MIN_IC_MEAN:
            passed = False
            reasons.append(f"IC Mean {ic:+.4f} < {self.MIN_IC_MEAN:+.4f} threshold")
        if sharpe is None:
            passed = False
            reasons.append("Sharpe undefined")
        elif sharpe < self.MIN_SHARPE:
            passed = False
            reasons.append(f"Sharpe {sharpe:.2f} < {self.MIN_SHARPE:.2f} threshold")
        if mdd < self.MAX_DRAWDOWN:
            passed = False
            reasons.append(f"Max Drawdown {mdd*100:.1f}% exceeds limit {self.MAX_DRAWDOWN*100:.1f}%")

        if passed:
            reasons.append("All OOS Quality Gates Passed (IC, Sharpe, Drawdown)")

        composite_score = ic * 10.0
        if sharpe is not None:
            composite_score += sharpe * 0.5
        return QualityGateResult(passed=passed, score=round(composite_score, 3), reasons=reasons)

    def register_candidate(
        self,
        alpha_id: Any,
        version: Optional[str] = None,
        alpha_type: str = "INTRADAY",
        description: str = "",
        config: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        auto_promote_if_passed: bool = False,
        provenance: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Register a new alpha version as CANDIDATE (or RESEARCH if gates fail)."""
        explicit_prov: Dict[str, Any] = {k: v for k, v in (provenance or {}).items() if k in PROVENANCE_FIELDS}
        if isinstance(alpha_id, dict):
            d = alpha_id
            for f in PROVENANCE_FIELDS:
                if d.get(f) and f not in explicit_prov:
                    explicit_prov[f] = d[f]
            alpha_id = str(d.get("alpha_id"))
            version = str(d.get("version", "v1.0.0"))
            alpha_type = str(d.get("horizon") or d.get("alpha_type", "INTRADAY"))
            description = str(d.get("description", ""))
            config = d.get("config_json") or d.get("config", {})
            metrics = d.get("out_of_sample_metrics") or d.get("metrics") or d.get("in_sample_metrics", {})

        version = version or "v1.0.0"
        metrics = metrics or {}
        config = config or {}
        gate = self.evaluate_quality_gate(metrics)

        initial_status = AlphaStatus.CANDIDATE.value if gate.passed else AlphaStatus.RESEARCH.value

        record = {
            "alpha_id": alpha_id,
            "version": version,
            "alpha_type": alpha_type.upper(),
            "status": initial_status,
            "description": description or f"Alpha {alpha_id} {version}",
            "config_json": config,
            "metrics_snapshot": metrics,
            "out_of_sample_metrics": metrics,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        record.update({k: v for k, v in resolve_provenance(**explicit_prov).items() if v})
        self.db.upsert_alpha(record)
        logger.info(f"Registered Alpha [{alpha_id}:{version}] with status {initial_status} (Gate: {gate.passed})")

        if auto_promote_if_passed and gate.passed:
            self.promote_to_paper(alpha_id, version, pm_user="SYSTEM_AUTO", reason="Auto-promoted on quality gate pass")

        return record

    def get_alpha(self, alpha_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Retrieve alpha record from registry database."""
        return self.db.get_alpha(alpha_id, version)

    def list_all_alphas(self) -> List[Dict[str, Any]]:
        """List all registered alphas."""
        return self.db.list_alphas()

    def promote_to_paper(
        self,
        alpha_id: str,
        version: str,
        pm_user: str = "PM",
        reason: str = "Promoted to Paper Trial soak",
    ) -> bool:
        """Promote alpha version to PAPER_TRIAL.
        
        Enforces Rule 4: At most 1 version of this alpha_id can be active in trial/live.
        Retires previous active version of the same alpha_id.
        """
        # 1. Demote/retire previous active version of this alpha_id
        active_versions = [
            a for a in self.db.list_alphas()
            if a["alpha_id"] == alpha_id and a["status"] in [AlphaStatus.PAPER_TRIAL.value, AlphaStatus.PROMOTED_ACTIVE.value]
        ]
        for prev in active_versions:
            if prev["version"] != version:
                self.db.update_alpha_status(alpha_id, prev["version"], AlphaStatus.RETIRED.value)
                self.db.record_pm_audit({
                    "action_type": "AUTO_RETIRE_SUPERSEDED",
                    "alpha_id": alpha_id,
                    "version": prev["version"],
                    "executed_by": pm_user,
                    "reason": f"Superseded by new version {version} promotion to PAPER_TRIAL",
                })
                logger.info(f"Retired superseded alpha version {alpha_id}:{prev['version']}")

        # 2. Promote target version
        self.db.update_alpha_status(alpha_id, version, AlphaStatus.PAPER_TRIAL.value, promoted_by=pm_user)
        self.db.record_pm_audit({
            "action_type": "PROMOTE_TO_PAPER",
            "alpha_id": alpha_id,
            "version": version,
            "executed_by": pm_user,
            "reason": reason,
        })
        logger.info(f"Promoted [{alpha_id}:{version}] to PAPER_TRIAL by {pm_user}")
        return True

    def promote_to_live(
        self,
        alpha_id: str,
        version: str,
        pm_user: str = "PM",
        reason: str = "Approved for Production Live Trading",
    ) -> bool:
        """Promote alpha version to PROMOTED_ACTIVE (Live).
        
        Enforces:
        - Quality gate check (IC >= 0.05, Sharpe >= 1.5, MDD >= -20%).
        - Rule 4: At most 1 version of this alpha_id can be PROMOTED_ACTIVE.
        """
        # Quality Gate Check
        gate = self.evaluate_quality_gate(alpha_id, version)
        if not gate.passed:
            logger.warning(f"Quality gate rejected promotion of {alpha_id}:{version} to LIVE: {gate.reasons}")
            return False

        active_versions = [
            a for a in self.db.list_alphas()
            if a["alpha_id"] == alpha_id and a["status"] == AlphaStatus.PROMOTED_ACTIVE.value
        ]
        for prev in active_versions:
            if prev["version"] != version:
                self.db.update_alpha_status(alpha_id, prev["version"], AlphaStatus.RETIRED.value)
                self.db.record_pm_audit({
                    "action_type": "AUTO_RETIRE_SUPERSEDED",
                    "alpha_id": alpha_id,
                    "version": prev["version"],
                    "executed_by": pm_user,
                    "reason": f"Superseded by live version {version}",
                })

        self.db.update_alpha_status(alpha_id, version, AlphaStatus.PROMOTED_ACTIVE.value, promoted_by=pm_user)
        self.db.record_pm_audit({
            "action_type": "PROMOTE_TO_LIVE",
            "alpha_id": alpha_id,
            "version": version,
            "executed_by": pm_user,
            "reason": reason,
        })
        logger.info(f"Promoted [{alpha_id}:{version}] to PROMOTED_ACTIVE by {pm_user}")
        return True

    def demote_or_retire(
        self,
        alpha_id: str,
        version: str,
        pm_user: str = "PM",
        reason: str = "Demoted by PM",
    ) -> bool:
        """Demote an active alpha version to RETIRED."""
        self.db.update_alpha_status(alpha_id, version, AlphaStatus.RETIRED.value)
        self.db.record_pm_audit({
            "action_type": "DEMOTE_OR_RETIRE",
            "alpha_id": alpha_id,
            "version": version,
            "executed_by": pm_user,
            "reason": reason,
        })
        logger.info(f"Demoted/Retired [{alpha_id}:{version}] by {pm_user}")
        return True

    def get_active_alphas(
        self,
        horizon: str = "INTRADAY",
        mode: str = "paper",
    ) -> List[Dict[str, Any]]:
        """Return all distinct active alphas ready to be combined into trading signals.
        
        Rule 3 & 4:
        - For paper mode: returns alphas in PAPER_TRIAL or PROMOTED_ACTIVE.
        - For live mode: returns alphas in PROMOTED_ACTIVE only.
        - Exactly max 1 version per alpha_id is returned.
        """
        all_alphas = self.db.list_alphas(alpha_type=horizon.upper())
        allowed_statuses = [
            AlphaStatus.PAPER_TRIAL.value,
            AlphaStatus.PROMOTED_ACTIVE.value,
            AlphaStatus.APPROVED_FROZEN.value,
        ] if mode == "paper" else [
            AlphaStatus.PROMOTED_ACTIVE.value,
            AlphaStatus.APPROVED_FROZEN.value,
        ]

        active_map: Dict[str, Dict[str, Any]] = {}
        for a in all_alphas:
            if a["status"] in allowed_statuses:
                alpha_id = a["alpha_id"]
                # If multiple exist somehow, prefer APPROVED_FROZEN > PROMOTED_ACTIVE > PAPER_TRIAL
                if alpha_id not in active_map:
                    active_map[alpha_id] = a
                else:
                    curr = active_map[alpha_id]
                    if a["status"] == AlphaStatus.APPROVED_FROZEN.value:
                        active_map[alpha_id] = a
                    elif a["status"] == AlphaStatus.PROMOTED_ACTIVE.value and curr["status"] != AlphaStatus.APPROVED_FROZEN.value:
                        active_map[alpha_id] = a

        return list(active_map.values())

    def approve_and_freeze_ensemble(
        self,
        alpha_ids: List[str],
        version: str = "v1.0.0",
        horizon: str = "INTRADAY",
        dataset_id: Optional[str] = None,
        model_version: Optional[str] = None,
        pm_user: str = "ADMIN",
        reason: str = "Admin OOS validation approval",
    ) -> bool:
        """Approve and freeze Top-5 ensemble based on Out-of-Sample validation."""
        for alpha_id in alpha_ids:
            self.db.update_alpha_status(alpha_id, version, AlphaStatus.APPROVED_FROZEN.value, promoted_by=pm_user)
            self.db.record_pm_audit({
                "action_type": "APPROVE_AND_FREEZE",
                "alpha_id": alpha_id,
                "version": version,
                "executed_by": pm_user,
                "reason": reason,
                "details": json.dumps({"dataset_id": dataset_id, "model_version": model_version}),
            })
        weights = [1.0 / len(alpha_ids)] * len(alpha_ids) if alpha_ids else []
        self.db.upsert_ensemble_config({
            "ensemble_id": "default",
            "horizon": horizon.upper(),
            "selected_alpha_ids": alpha_ids,
            "weights": weights,
            "dataset_id": dataset_id,
            "model_version": model_version,
            "approval_status": AlphaStatus.APPROVED_FROZEN.value,
            "approved_by": pm_user,
            "updated_by": pm_user,
        })
        logger.info(f"Approved and frozen ensemble {alpha_ids} by {pm_user}")
        return True
