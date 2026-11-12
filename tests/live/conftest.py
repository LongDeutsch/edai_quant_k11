"""Pre-flight live infrastructure fixtures for tests/live.

Enforces:
1. Zero simulation / fallback in tests/live.
2. All required Docker containers must be online and responsive on their exposed ports.
3. Fail-hard immediately if any service is down.
"""

from __future__ import annotations

import os
import socket
import urllib.request
import pytest

LIVE_PORTS = {
    "MinIO (S3A)": ("127.0.0.1", 9000),
    "Iceberg REST Catalog": ("127.0.0.1", 8181),
    "Kafka Broker": ("127.0.0.1", 9092),
    "Schema Registry": ("127.0.0.1", 8085),
    "Flink JobManager": ("127.0.0.1", 8081),
    "ClickHouse HTTP": ("127.0.0.1", 8123),
    "PostgreSQL": ("127.0.0.1", 5432),
    "Airflow Webserver": ("127.0.0.1", 8080),
}


def is_port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_live_infra():
    """Session-level guard ensuring all 8 core services are reachable."""
    offline_services = []
    for name, (host, port) in LIVE_PORTS.items():
        if not is_port_open(host, port):
            offline_services.append(f"{name} ({host}:{port})")

    if offline_services:
        missing_list = "\n  - ".join(offline_services)
        raise RuntimeError(
            f"\n[FATAL] Real Integration requires live Docker infrastructure. The following services are OFFLINE:\n"
            f"  - {missing_list}\n"
            f"Start services via: make up (or docker compose up -d)\n"
            f"Bypass/Mock/Simulation is strictly forbidden in tests/live/."
        )

    # Force execution mode to integration
    os.environ["MODE"] = "integration"
    os.environ["EXECUTION_MODE"] = "LIVE"
    os.environ["SPARK_ENGINE"] = "spark"
    os.environ["FLINK_ENGINE"] = "flink"
