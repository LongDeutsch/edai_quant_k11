"""Immutable Bronze layout keyed by dataset_id / run_id."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

CURRENT_POINTER_NAME = "CURRENT_DATASET"


def generate_dataset_id(
    seed: int = 42,
    mode: str = "smoke",
    scale: float = 0.1,
    explicit: Optional[str] = None,
) -> str:
    """Build a unique dataset_id. Never reuse an existing id for a new write."""
    if explicit:
        safe = re.sub(r"[^a-zA-Z0-9_.-]+", "_", explicit.strip())
        return safe or f"ds_{uuid4().hex[:12]}"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    scale_tag = str(scale).replace(".", "p")
    return f"{ts}_{mode}_s{scale_tag}_seed{seed}_{uuid4().hex[:8]}"


def resolve_dataset_root(
    out_dir: str | Path,
    dataset_id: Optional[str] = None,
    *,
    create: bool = False,
    allow_legacy: bool = True,
) -> Path:
    """Return OUT_DIR/runs/{dataset_id}. Resolves CURRENT pointer when dataset_id omitted.

    Legacy fallback: if no pointer but OUT_DIR/raw_market_ticks exists, return OUT_DIR.
    """
    root = Path(out_dir)
    # If root is already pointing to the dataset directory
    if dataset_id and root.name == dataset_id:
        return root

    ds = dataset_id or os.environ.get("DATASET_ID") or read_current_dataset_id(root)
    if ds:
        if root.name == ds:
            return root
        path = root / "runs" / ds
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path
    if allow_legacy and (root / "raw_market_ticks").exists():
        return root
    if create:
        # Caller asked to create but no id — mint one
        ds = generate_dataset_id()
        path = root / "runs" / ds
        path.mkdir(parents=True, exist_ok=True)
        write_current_pointer(root, ds)
        return path
    raise FileNotFoundError(
        f"No DATASET_ID / {CURRENT_POINTER_NAME} under {root}. Run generator first."
    )


def lakehouse_uri(dataset_id: str, zone: str = "bronze") -> str:
    """S3A URI for immutable Bronze/Silver/Gold namespaces under lakehouse."""
    return f"s3a://lakehouse/{zone}/runs/{dataset_id}"


def write_current_pointer(out_dir: str | Path, dataset_id: str) -> Path:
    """Point CURRENT_DATASET at the active immutable run (does not delete prior runs)."""
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    pointer = root / CURRENT_POINTER_NAME
    payload = {
        "dataset_id": dataset_id,
        "run_root": str(root / "runs" / dataset_id),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    pointer.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    # Also keep a plain-text id for shell convenience
    (root / "CURRENT_DATASET_ID").write_text(dataset_id + "\n", encoding="utf-8")
    return pointer


def read_current_dataset_id(out_dir: str | Path) -> Optional[str]:
    root = Path(out_dir)
    plain = root / "CURRENT_DATASET_ID"
    if plain.exists():
        text = plain.read_text(encoding="utf-8").strip()
        if text:
            return text
    pointer = root / CURRENT_POINTER_NAME
    if pointer.exists():
        try:
            data = json.loads(pointer.read_text(encoding="utf-8"))
            return data.get("dataset_id")
        except Exception:
            return None
    return None
