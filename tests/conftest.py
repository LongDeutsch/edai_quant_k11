"""Root test configuration and fixtures for Quant AI Platform."""

import os
import pytest


@pytest.fixture(autouse=True)
def clean_pm_state_for_tests(tmp_path, monkeypatch):
    """Ensure all unit tests run in an isolated environment without interference from manual UI PM overrides."""
    state_file = str(tmp_path / "pm_controls_test.json")
    monkeypatch.setenv("PM_STATE_FILE", state_file)
    try:
        import quant_edai.quant.pm_controls as pm_mod
        monkeypatch.setattr(pm_mod, "PM_STATE_FILE", state_file)
    except Exception:
        pass
    yield state_file


@pytest.fixture
def smoke_env(monkeypatch, tmp_path):
    """MODE=smoke, SQLite-only, no ambient provenance/dataset env (P1 tests)."""
    monkeypatch.setenv("USE_SQLITE_ONLY", "1")
    monkeypatch.setenv("MODE", "smoke")
    monkeypatch.setenv("OUT_DIR", str(tmp_path / "lakehouse"))
    for var in ("MODEL_VERSION", "DATASET_ID", "FEATURE_SOURCE", "BAR_TIMESTAMP_TZ", "KAFKA_BOOTSTRAP", "KAFKA_BOOTSTRAP_SERVERS"):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.fixture
def integration_env(smoke_env):
    """Same as ``smoke_env`` but MODE=integration (real data / provenance required)."""
    smoke_env.setenv("MODE", "integration")
    return smoke_env
