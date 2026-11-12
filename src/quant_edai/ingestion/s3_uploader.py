"""
S3 Source Uploader Module.

Stages generated datasets into MinIO bucket `external-source`.
MODE=integration|full → fail hard if MinIO/SDK unavailable.
MODE=smoke → STAGED_LOCAL + execution_mode=SIMULATED.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict
import urllib.request

from quant_edai.common.dataset_paths import lakehouse_uri, read_current_dataset_id, resolve_dataset_root
from quant_edai.common.execution_status import build_status, is_integration_mode, normalize_mode


class S3SourceUploader:
    """Uploads staged Parquet/JSON datasets to MinIO S3 bucket `external-source`."""

    def __init__(
        self,
        endpoint_url: str = "http://localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket_name: str = "external-source",
        source_dir: str = "/tmp/lakehouse_source",
        dataset_id: str | None = None,
        mode: str | None = None,
    ):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.base_out_dir = Path(source_dir)
        self.mode = normalize_mode(mode)
        self.dataset_id = dataset_id or os.environ.get("DATASET_ID") or read_current_dataset_id(
            self.base_out_dir
        )
        try:
            self.source_dir = resolve_dataset_root(
                self.base_out_dir, self.dataset_id, allow_legacy=True
            )
        except FileNotFoundError:
            self.source_dir = self.base_out_dir

    def is_minio_reachable(self) -> bool:
        """Checks if MinIO service is accessible."""
        try:
            req = urllib.request.Request(f"{self.endpoint_url}/minio/health/live")
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status == 200
        except Exception:
            return False

    def upload_dataset(self) -> Dict[str, Any]:
        """Discover files in dataset run root and stage to MinIO (or SIMULATED local)."""
        if not self.source_dir.exists():
            result = build_status(
                ok=False,
                mode=self.mode,
                engine="minio",
                degraded_reason=f"Source directory {self.source_dir} does not exist. Run generator first.",
                uploaded_files=0,
            ).as_dict()
            result["message"] = result.get("detail")
            return result

        all_files = [p for p in self.source_dir.glob("**/*") if p.is_file()]
        # Skip pointer-only noise from walking into unrelated dirs
        total_bytes = sum(p.stat().st_size for p in all_files)
        reachable = self.is_minio_reachable()
        ds = self.dataset_id or "unknown"
        bronze_prefix = f"bronze/runs/{ds}"

        if reachable:
            try:
                import minio  # type: ignore

                client = minio.Minio(
                    self.endpoint_url.replace("http://", "").replace("https://", ""),
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=False,
                )
                if not client.bucket_exists(self.bucket_name):
                    client.make_bucket(self.bucket_name)
                # Also ensure lakehouse bucket for Spark S3A warehouse
                lake_bucket = "lakehouse"
                if not client.bucket_exists(lake_bucket):
                    client.make_bucket(lake_bucket)

                uploaded = []
                for fpath in all_files:
                    rel_key = str(fpath.relative_to(self.source_dir))
                    # external-source mirror
                    client.fput_object(self.bucket_name, f"{bronze_prefix}/{rel_key}", str(fpath))
                    # lakehouse bronze immutable namespace
                    client.fput_object(lake_bucket, f"bronze/runs/{ds}/{rel_key}", str(fpath))
                    uploaded.append(rel_key)

                status = build_status(
                    ok=True,
                    mode=self.mode,
                    engine="minio",
                    dataset_id=ds,
                    run_id=ds,
                    mode_transport="MINIO_S3",
                    bucket=self.bucket_name,
                    endpoint=self.endpoint_url,
                    uploaded_files_count=len(uploaded),
                    total_bytes=total_bytes,
                    bronze_uri=lakehouse_uri(ds, "bronze"),
                    external_prefix=f"s3://{self.bucket_name}/{bronze_prefix}",
                ).as_dict()
                # Prefer SUCCESS label for live MinIO in smoke too when actually uploaded
                if status["execution_mode"] == "SIMULATED" and self.mode in {"smoke", "test"}:
                    status["status"] = "SUCCESS"
                    status["execution_mode"] = "LIVE"
                status["mode"] = "MINIO_S3"
                return status
            except ImportError as e:
                if is_integration_mode(self.mode):
                    return build_status(
                        ok=False,
                        mode=self.mode,
                        engine="minio",
                        degraded_reason=f"minio SDK required in MODE={self.mode}: {e}",
                        dataset_id=ds,
                    ).as_dict()
                return build_status(
                    ok=True,
                    mode=self.mode,
                    engine="sim",
                    forced_simulated=True,
                    degraded_reason=f"minio SDK missing: {e}",
                    status_override="STAGED_LOCAL",
                    staged_files_count=len(all_files),
                    total_bytes=total_bytes,
                    local_path=str(self.source_dir),
                    dataset_id=ds,
                ).as_dict() | {
                    "status": "STAGED_LOCAL",
                    "mode": "LOCAL_STORAGE",
                    "execution_mode": "SIMULATED",
                }
            except Exception as e:
                if is_integration_mode(self.mode):
                    return build_status(
                        ok=False,
                        mode=self.mode,
                        engine="minio",
                        degraded_reason=f"MinIO upload failed: {e}",
                        dataset_id=ds,
                    ).as_dict()
                return {
                    "status": "STAGED_LOCAL",
                    "execution_mode": "SIMULATED",
                    "mode": "LOCAL_STORAGE",
                    "reason": str(e),
                    "staged_files_count": len(all_files),
                    "total_bytes": total_bytes,
                    "local_path": str(self.source_dir),
                    "dataset_id": ds,
                }

        # MinIO offline
        if is_integration_mode(self.mode):
            return build_status(
                ok=False,
                mode=self.mode,
                engine="minio",
                degraded_reason="MinIO unreachable; MODE=integration|full forbids local silent success",
                dataset_id=ds,
                staged_files_count=len(all_files),
                local_path=str(self.source_dir),
            ).as_dict()

        return {
            "status": "STAGED_LOCAL",
            "execution_mode": "SIMULATED",
            "mode": "LOCAL_STORAGE",
            "message": "MinIO container is offline; datasets verified in local staging path.",
            "staged_files_count": len(all_files),
            "total_bytes": total_bytes,
            "local_path": str(self.source_dir),
            "dataset_id": ds,
        }


def sync_lakehouse_data(
    source_dir: str = "/tmp/lakehouse_source",
    endpoint: str = "http://localhost:9000",
    bucket: str = "external-source",
) -> Dict[str, Any]:
    """Helper to stage/sync lakehouse external data to MinIO or local fallback."""
    uploader = S3SourceUploader(
        source_dir=source_dir,
        endpoint_url=endpoint,
        bucket_name=bucket,
        mode=os.environ.get("MODE", "smoke"),
    )
    return uploader.upload_dataset()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload staged data to MinIO S3")
    parser.add_argument("--source-dir", type=str, default=os.getenv("OUT_DIR", "/tmp/lakehouse_source"))
    parser.add_argument("--endpoint", type=str, default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--bucket", type=str, default="external-source")
    parser.add_argument("--dataset-id", type=str, default=None)
    parser.add_argument("--mode", type=str, default=os.getenv("MODE", "smoke"))

    args = parser.parse_args()
    uploader = S3SourceUploader(
        source_dir=args.source_dir,
        endpoint_url=args.endpoint,
        bucket_name=args.bucket,
        dataset_id=args.dataset_id,
        mode=args.mode,
    )
    result = uploader.upload_dataset()
    print(f"[S3Uploader] Result: {json.dumps(result, indent=2)}")
    if result.get("status") in {"FAILED", "ERROR"} or result.get("execution_mode") == "LIVE_REQUIRED":
        sys.exit(1)


if __name__ == "__main__":
    main()
