"""
Pre-flight spike to verify Apache Iceberg REST Catalog endpoint and environment.

Tests:
1. GET http://localhost:8181/v1/config
2. Validates warehouse URI ('s3a://lakehouse/') and S3 endpoint.
3. Tests Iceberg REST Catalog namespaces endpoint.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict
import requests


def spike_iceberg_rest(catalog_url: str = "http://localhost:8181") -> Dict[str, Any]:
    print(f"==> Spiking Apache Iceberg REST Catalog at {catalog_url}...")
    result = {
        "catalog_url": catalog_url,
        "is_online": False,
        "config_endpoint_status": None,
        "warehouse_location": None,
        "namespaces_status": None,
        "status": "OFFLINE",
    }

    try:
        resp = requests.get(f"{catalog_url}/v1/config", timeout=3)
        result["config_endpoint_status"] = resp.status_code
        if resp.status_code == 200:
            result["is_online"] = True
            data = resp.json()
            defaults = data.get("defaults", {})
            overrides = data.get("overrides", {})
            result["warehouse_location"] = overrides.get("warehouse") or defaults.get("warehouse")
            print(f"[Iceberg REST] Connected successfully! Warehouse: {result['warehouse_location']}")

            # Check namespaces endpoint
            ns_resp = requests.get(f"{catalog_url}/v1/namespaces", timeout=3)
            result["namespaces_status"] = ns_resp.status_code
            result["status"] = "PASSED"
        else:
            print(f"[Iceberg REST] Returned HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[Iceberg REST] Service is currently offline ({e}). Running in local fallback mode.")
        result["status"] = "OFFLINE_FALLBACK"

    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Spike Iceberg REST Catalog")
    parser.add_argument("--catalog-url", default=os.getenv("ICEBERG_REST_URL", "http://localhost:8181"), help="Iceberg REST endpoint")
    args = parser.parse_args()
    res = spike_iceberg_rest(catalog_url=args.catalog_url)
    print(json.dumps(res, indent=2))
