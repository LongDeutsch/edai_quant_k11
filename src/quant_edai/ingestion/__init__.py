"""Ingestion utilities for staging raw data to S3 / MinIO."""

__all__ = ["S3SourceUploader"]

def __getattr__(name: str):
    if name == "S3SourceUploader":
        from quant_edai.ingestion.s3_uploader import S3SourceUploader
        return S3SourceUploader
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
