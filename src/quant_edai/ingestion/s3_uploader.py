"""
S3 Source Uploader Module.

Stages generated datasets into MinIO bucket `external-source` (simulating external department data).
Gracefully falls back to local file staging when MinIO container is not running.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error


class S3SourceUploader:
    """Uploads staged Parquet/JSON datasets to MinIO S3 bucket `external-source`."""

    def __init__(
        self,
        endpoint_url: str = "http://localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket_name: str = "external-source",
        source_dir: str = "/tmp/lakehouse_source",
    ):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.source_dir = Path(source_dir)

    def is_minio_reachable(self) -> bool:
        """Checks if MinIO service is accessible."""
        try:
            req = urllib.request.Request(f"{self.endpoint_url}/minio/health/live")
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status == 200
        except Exception:
            return False

    def upload_dataset(self) -> Dict[str, Any]:
        """
        Discovers all files in source_dir and stages them to MinIO or verifies local staging.
        """
        if not self.source_dir.exists():
            return {
                "status": "ERROR",
                "message": f"Source directory {self.source_dir} does not exist. Run generator first.",
                "uploaded_files": 0,
            }

        all_files = [p for p in self.source_dir.glob("**/*") if p.is_file()]
        total_bytes = sum(p.stat().st_size for p in all_files)

        reachable = self.is_minio_reachable()

        if reachable:
            # MinIO is running - attempt upload
            uploaded = []
            try:
                # Try using boto3 or minio if available
                import minio  # type: ignore
                client = minio.Minio(
                    self.endpoint_url.replace("http://", "").replace("https://", ""),
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=False,
                )
                if not client.bucket_exists(self.bucket_name):
                    client.make_bucket(self.bucket_name)

                for fpath in all_files:
                    rel_key = str(fpath.relative_to(self.source_dir))
                    client.fput_object(self.bucket_name, rel_key, str(fpath))
                    uploaded.append(rel_key)

                return {
                    "status": "SUCCESS",
                    "mode": "MINIO_S3",
                    "bucket": self.bucket_name,
                    "endpoint": self.endpoint_url,
                    "uploaded_files_count": len(uploaded),
                    "total_bytes": total_bytes,
                }
            except Exception as e:
                # If MinIO Python SDK fails, report status
                return {
                    "status": "FALLBACK_LOCAL_STAGED",
                    "mode": "LOCAL_STORAGE",
                    "reason": str(e),
                    "staged_files_count": len(all_files),
                    "total_bytes": total_bytes,
                    "local_path": str(self.source_dir),
                }

        # MinIO is offline -> local file staging verified
        return {
            "status": "STAGED_LOCAL",
            "mode": "LOCAL_STORAGE",
            "message": "MinIO container is offline; datasets verified in local staging path.",
            "staged_files_count": len(all_files),
            "total_bytes": total_bytes,
            "local_path": str(self.source_dir),
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
    )
    return uploader.upload_dataset()


def main():
    parser = argparse.ArgumentParser(description="Upload staged data to MinIO S3")
    parser.add_argument("--source-dir", type=str, default="/tmp/lakehouse_source")
    parser.add_argument("--endpoint", type=str, default="http://localhost:9000")
    parser.add_argument("--bucket", type=str, default="external-source")

    args = parser.parse_args()
    uploader = S3SourceUploader(
        source_dir=args.source_dir, endpoint_url=args.endpoint, bucket_name=args.bucket
    )
    result = uploader.upload_dataset()
    print(f"[S3Uploader] Result: {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
