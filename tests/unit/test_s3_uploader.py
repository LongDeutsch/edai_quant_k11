"""Unit tests for S3SourceUploader."""

import json
from pathlib import Path
import pytest

from quant_edai.ingestion.s3_uploader import S3SourceUploader


def test_uploader_local_fallback(tmp_path):
    """Verifies that when MinIO is offline, local staging is correctly validated."""
    # Create fake staged files
    mock_source = tmp_path / "lakehouse_source"
    mock_source.mkdir()
    (mock_source / "test_file.parquet").write_bytes(b"dummy_parquet_data")

    # Use a dummy non-routable port to ensure offline detection
    uploader = S3SourceUploader(
        endpoint_url="http://127.0.0.1:59999",
        source_dir=str(mock_source),
        bucket_name="external-source",
    )

    result = uploader.upload_dataset()
    assert result["status"] == "STAGED_LOCAL"
    assert result["mode"] == "LOCAL_STORAGE"
    assert result["staged_files_count"] == 1
    assert result["total_bytes"] == len(b"dummy_parquet_data")


def test_uploader_nonexistent_directory(tmp_path):
    """Verifies error handling on non-existent directory."""
    non_existent = tmp_path / "missing_dir"
    uploader = S3SourceUploader(source_dir=str(non_existent))
    result = uploader.upload_dataset()
    assert result["status"] in {"ERROR", "FAILED", "DEGRADED"}
    assert result["uploaded_files"] == 0
