"""P0.1: immutable Bronze / dataset_id isolation, SHA-256 manifest & America/New_York hours."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import pandas as pd
import pytest

from quant_edai.common.dataset_paths import read_current_dataset_id, resolve_dataset_root
from quant_edai.generator.data_generator import QuantDataGenerator


@pytest.mark.p0
def test_dataset_isolation_no_cross_run_mix(tmp_path: Path) -> None:
    out = tmp_path / "lakehouse"
    gen1 = QuantDataGenerator(
        mode="smoke",
        scale=0.001,
        seed=1,
        out_dir=str(out),
        start_date="2024-01-02",
        end_date="2024-01-05",
    )
    s1 = gen1.run()
    id1 = s1["dataset_id"]
    run1 = out / "runs" / id1
    assert run1.exists()
    assert (run1 / "raw_market_ticks").exists()
    assert (run1 / "manifest.json").exists()

    gen2 = QuantDataGenerator(
        mode="smoke",
        scale=0.001,
        seed=2,
        out_dir=str(out),
        start_date="2022-11-01",
        end_date="2022-11-05",
    )
    s2 = gen2.run()
    id2 = s2["dataset_id"]
    assert id1 != id2
    run2 = out / "runs" / id2
    assert run1.exists(), "prior Bronze run must remain immutable"
    assert run2.exists()
    assert read_current_dataset_id(out) == id2
    current = resolve_dataset_root(out)
    assert current == run2
    # Active snapshot must not mix smoke short window with prior seed path
    assert current != run1
    stats = json.loads((current / "generation_stats.json").read_text(encoding="utf-8"))
    assert stats["dataset_id"] == id2
    assert stats["seed"] == 2


@pytest.mark.p0
def test_bronze_immutable_fail_on_overwrite(tmp_path: Path) -> None:
    out = tmp_path / "lakehouse"
    ds_fixed = "ds_fixed_immutability_test"
    gen = QuantDataGenerator(
        mode="integration",
        scale=0.001,
        seed=42,
        out_dir=str(out),
        dataset_id=ds_fixed,
        start_date="2024-01-02",
        end_date="2024-01-03",
    )
    gen.run()
    run_dir = out / "runs" / ds_fixed
    assert run_dir.exists()
    assert (run_dir / "manifest.json").exists()

    # Rerunning with the exact same dataset_id in integration mode must raise FileExistsError
    gen_duplicate = QuantDataGenerator(
        mode="integration",
        scale=0.001,
        seed=42,
        out_dir=str(out),
        dataset_id=ds_fixed,
        start_date="2024-01-02",
        end_date="2024-01-03",
    )
    with pytest.raises(FileExistsError, match="Bronze is strictly immutable"):
        gen_duplicate.run()


@pytest.mark.p0
def test_bronze_manifest_and_sha256(tmp_path: Path) -> None:
    out = tmp_path / "lakehouse"
    gen = QuantDataGenerator(
        mode="smoke",
        scale=0.001,
        seed=10,
        out_dir=str(out),
        start_date="2024-01-02",
        end_date="2024-01-03",
    )
    res = gen.run()
    run_dir = out / "runs" / res["dataset_id"]
    manifest_p = run_dir / "manifest.json"
    assert manifest_p.exists()
    manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
    assert manifest["dataset_id"] == res["dataset_id"]
    assert len(manifest["files"]) > 0

    # Verify sha256 checksums match real file bytes
    for entry in manifest["files"]:
        fpath = run_dir / entry["path"]
        assert fpath.exists()
        hasher = hashlib.sha256()
        with open(fpath, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        assert hasher.hexdigest() == entry["sha256"]
        assert fpath.stat().st_size == entry["size_bytes"]


@pytest.mark.p0
def test_america_new_york_market_hours(tmp_path: Path) -> None:
    out = tmp_path / "lakehouse"
    gen = QuantDataGenerator(
        mode="smoke",
        scale=0.001,
        seed=77,
        out_dir=str(out),
        start_date="2024-01-02",
        end_date="2024-01-03",
    )
    res = gen.run()
    run_dir = out / "runs" / res["dataset_id"]
    df_ticks = pd.read_parquet(run_dir / "raw_market_ticks")
    assert "dataset_id" in df_ticks.columns
    assert (df_ticks["dataset_id"] == res["dataset_id"]).all()

    # Convert event_timestamp to America/New_York
    ts_series = pd.to_datetime(df_ticks["event_timestamp"], utc=True).dt.tz_convert("America/New_York")
    # All ticks must execute between 09:30:00 and 16:00:00 ET
    hours = ts_series.dt.hour + ts_series.dt.minute / 60.0
    assert (hours >= 9.5).all(), "Tick executed before market open 09:30 ET"
    assert (hours <= 16.0).all(), "Tick executed after market close 16:00 ET"
