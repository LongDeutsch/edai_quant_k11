"""ML provenance contract: model_version + dataset_id + feature_source on every ML-derived row.

MODE=integration|full
    Missing, empty or synthetic/simulated provenance raises ``ProvenanceError``.
MODE=smoke (default)
    Missing fields are stamped with the explicit ``SIMULATED`` sentinel so smoke
    rows can never be mistaken for lakehouse-backed evidence.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from quant_edai.common.dataset_paths import read_current_dataset_id
from quant_edai.common.execution_status import is_integration_mode, normalize_mode

PROVENANCE_FIELDS = ("model_version", "dataset_id", "feature_source")
SIMULATED = "SIMULATED"

# Values that are acceptable in smoke but never count as real lineage in integration|full.
_PLACEHOLDER_VALUES = frozenset(
    {"", "none", "null", "nan", "unknown", "n/a", "simulated", "synthetic", "synthetic_panel", "synthetic_panel_2y"}
)
_PLACEHOLDER_PREFIXES = ("synthetic", "simulated")

MODEL_CARD_SUFFIX = ".meta.json"


class ProvenanceError(RuntimeError):
    """Raised when integration|full data lacks model_version / dataset_id / feature_source."""


def is_real_provenance_value(value: Any) -> bool:
    """True when ``value`` looks like real lineage (not empty / synthetic / simulated)."""
    if value is None:
        return False
    text = str(value).strip()
    low = text.lower()
    if low in _PLACEHOLDER_VALUES:
        return False
    return not low.startswith(_PLACEHOLDER_PREFIXES)


def missing_provenance(record: Mapping[str, Any], fields: Iterable[str] = PROVENANCE_FIELDS) -> list[str]:
    return [f for f in fields if not is_real_provenance_value(record.get(f))]


def provenance_from_env() -> Dict[str, Optional[str]]:
    """Provenance hints supplied by the orchestrator (Make / Airflow)."""
    return {
        "model_version": os.environ.get("MODEL_VERSION") or None,
        "dataset_id": os.environ.get("DATASET_ID") or None,
        "feature_source": os.environ.get("FEATURE_SOURCE") or None,
    }


def resolve_dataset_id(out_dir: Optional[str] = None) -> Optional[str]:
    """DATASET_ID env wins, then the CURRENT_DATASET pointer under OUT_DIR."""
    ds = os.environ.get("DATASET_ID")
    if ds:
        return ds
    root = out_dir or os.environ.get("OUT_DIR", "/tmp/lakehouse_source")
    try:
        return read_current_dataset_id(root)
    except Exception:
        return None


def dataset_id_from_path(path: str | Path | None) -> Optional[str]:
    """Extract ``{dataset_id}`` from ``.../runs/{dataset_id}/...`` (None for legacy flat layouts)."""
    if not path:
        return None
    parts = Path(str(path).split("#", 1)[0]).parts
    for i, part in enumerate(parts[:-1]):
        if part == "runs":
            return parts[i + 1]
    return None


def default_alpha_model_version(alpha_version: str) -> str:
    """Rule-based catalog alphas version themselves; MODEL_VERSION env wins when set."""
    return os.environ.get("MODEL_VERSION") or f"alpha_catalog-{alpha_version}"


def model_card_path(model_path: str | Path) -> Path:
    p = Path(model_path)
    return p.with_name(p.stem + MODEL_CARD_SUFFIX)


def write_model_card(
    model_path: str | Path,
    *,
    model_version: str,
    dataset_id: Optional[str],
    feature_source: Optional[str],
    extra: Optional[Dict[str, Any]] = None,
) -> Path:
    """Persist provenance next to a trained artifact (consumed by backtester/serving)."""
    card = {
        "model_version": model_version,
        "dataset_id": dataset_id,
        "feature_source": feature_source,
    }
    if extra:
        card.update(extra)
    path = model_card_path(model_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(card, indent=2, default=str), encoding="utf-8")
    return path


def read_model_card(model_path: str | Path) -> Dict[str, Any]:
    path = model_card_path(model_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def resolve_provenance(
    *,
    model_version: Optional[str] = None,
    dataset_id: Optional[str] = None,
    feature_source: Optional[str] = None,
    model_path: Optional[str | Path] = None,
    out_dir: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Merge explicit args > model card > env > CURRENT_DATASET pointer."""
    env = provenance_from_env()
    card = read_model_card(model_path) if model_path else {}
    return {
        "model_version": model_version or card.get("model_version") or env["model_version"],
        "dataset_id": dataset_id
        or env["dataset_id"]
        or card.get("dataset_id")
        # CURRENT_DATASET pointer is lineage only for integration|full (or an explicit out_dir);
        # smoke rows stay SIMULATED rather than inheriting a stray pointer.
        or (resolve_dataset_id(out_dir) if (out_dir or is_integration_mode()) else None),
        "feature_source": feature_source or env["feature_source"] or card.get("feature_source"),
    }


def require_provenance(
    record: Mapping[str, Any] | Dict[str, Any],
    *,
    mode: Optional[str] = None,
    kind: str = "record",
    fields: Iterable[str] = PROVENANCE_FIELDS,
    defaults: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a copy of ``record`` carrying all provenance fields.

    ``defaults`` (e.g. engine-level provenance) fill gaps before validation.
    integration|full: any field still missing/placeholder -> ProvenanceError.
    smoke: gaps are stamped ``SIMULATED`` and ``provenance_status=SIMULATED`` is set.
    """
    out: Dict[str, Any] = dict(record)
    fields = tuple(fields)
    if defaults:
        for f in fields:
            if not is_real_provenance_value(out.get(f)) and is_real_provenance_value(defaults.get(f)):
                out[f] = defaults[f]

    missing = missing_provenance(out, fields)
    if not missing:
        out["provenance_status"] = "OK"
        return out

    if is_integration_mode(mode):
        ident = out.get("signal_id") or out.get("order_id") or out.get("trade_id") or out.get("alpha_id") or ""
        raise ProvenanceError(
            f"[MODE={normalize_mode(mode)}] {kind} {ident} missing ML provenance: {', '.join(missing)} "
            f"(require {', '.join(fields)})"
        )

    for f in missing:
        out[f] = SIMULATED
    out["provenance_status"] = SIMULATED
    return out


def provenance_columns(record: Mapping[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """(model_version, dataset_id, feature_source) as plain strings for DB inserts."""
    return tuple(  # type: ignore[return-value]
        (str(record[f]) if record.get(f) is not None else None) for f in PROVENANCE_FIELDS
    )
