"""
PySpark Session Factory for Quant EDAI Medallion Lakehouse.

Configures SparkSession with:
1. Adaptive Query Execution (AQE) and Skew Join handling (Rubric M1-10)
2. Hadoop S3A MinIO credentials and endpoint
3. Apache Iceberg REST Catalog (port 8181) with pinned runtime jars
4. Spark History Server event logging (shared ./.data/spark-events volume, port 18080)
"""

from __future__ import annotations

import os
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from ruamel.yaml import YAML
    _yaml = YAML(typ="safe")
except ImportError:
    try:
        import yaml
        _yaml = None
    except ImportError:
        _yaml = None


def load_spark_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Loads Spark engine configuration from YAML."""
    default_path = Path("configs/processing/spark.yaml")
    cfg_file = Path(config_path) if config_path else default_path
    if cfg_file.exists():
        with open(cfg_file, "r") as f:
            if _yaml is not None:
                data = _yaml.load(f)
            else:
                import yaml
                data = yaml.safe_load(f)
            return data.get("spark", {}) if data else {}
    return {}


def is_service_reachable(url: str, timeout: float = 1.0) -> bool:
    """Checks if a REST endpoint is reachable."""
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout):
            return True
    except Exception:
        return False


# Pinned packages for S3A + Iceberg (Spark 3.5 / Scala 2.12)
ICEBERG_SPARK_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_BUNDLE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
DEFAULT_SPARK_PACKAGES = f"{ICEBERG_SPARK_RUNTIME},{HADOOP_AWS},{AWS_SDK_BUNDLE}"


def get_spark_session(
    app_name: str = "quant_edai_lakehouse",
    master: str = "local[*]",
    config_path: Optional[str] = None,
    enable_ui: bool = True,
    enable_s3a: bool = True,
    enable_rest_catalog: bool = True,
    require_s3a: Optional[bool] = None,
) -> Any:
    """
    Initializes and returns a configured PySpark SparkSession.
    Configures shared History Server event log volume and Iceberg REST Catalog.
    When ENGINE=spark and MODE=integration|full, S3A/catalog health is required.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise ImportError(
            "PySpark is not installed in the active environment. "
            "Install it via: uv pip install 'pyspark>=3.5.0,<4.0.0' or use ENGINE=sim for simulation."
        ) from e

    from quant_edai.common.execution_status import is_integration_mode

    # Keep driver/worker on the same interpreter (uv/.venv vs system python mismatch).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    cfg = load_spark_config(config_path)
    spark_master = master or cfg.get("master", "local[*]")
    app_title = app_name or cfg.get("app_name", "quant_edai_spark")
    engine = os.environ.get("ENGINE", "sim").lower()
    must_s3a = require_s3a if require_s3a is not None else (
        engine == "spark" and is_integration_mode()
    )

    packages = os.environ.get("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES)
    builder = (
        SparkSession.builder.master(spark_master)
        .appName(app_title)
        .config("spark.jars.packages", packages)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    # Core performance & shuffle tuning
    builder = (
        builder
        .config("spark.driver.memory", cfg.get("driver_memory", "2g"))
        .config("spark.executor.memory", cfg.get("executor_memory", "2g"))
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
    )

    # Adaptive Query Execution (AQE) & Skew Join (Rubric M1-10)
    aqe_cfg = cfg.get("aqe", {})
    builder = (
        builder
        .config("spark.sql.adaptive.enabled", str(aqe_cfg.get("enabled", True)).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(aqe_cfg.get("skew_join_enabled", True)).lower())
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", str(aqe_cfg.get("skew_join_skewed_partition_factor", 5)))
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(aqe_cfg.get("skew_join_skewed_partition_threshold_in_bytes", 67108864)))
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(aqe_cfg.get("coalesce_partitions_enabled", True)).lower())
    )

    # S3A MinIO Object Store Integration
    default_s3 = "http://minio:9000" if is_service_reachable("http://minio:9000/minio/health/live") else "http://localhost:9000"
    s3_endpoint = os.environ.get("MINIO_ENDPOINT") or cfg.get("s3a", {}).get("endpoint") or default_s3

    if enable_s3a:
        s3_cfg = cfg.get("s3a", {})
        s3_access = s3_cfg.get("access_key", "minioadmin")
        s3_secret = s3_cfg.get("secret_key", "minioadmin")
        s3_impl = s3_cfg.get("impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", s3_impl)
            .config("spark.hadoop.fs.s3.impl", s3_impl)
        )

    # Spark Web UI & Event Logging to Shared History Server Volume
    # Must match docker-compose spark-history mount: <repo>/.data/spark-events -> /tmp/spark-events
    events_dir = Path(".data/spark-events").resolve()
    events_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(events_dir, 0o755)
    except OSError:
        pass
    builder = (
        builder
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{events_dir}")
    )

    ui_cfg = cfg.get("ui", {})
    if enable_ui and ui_cfg.get("enabled", True):
        builder = (
            builder
            .config("spark.ui.enabled", "true")
            .config("spark.ui.port", str(ui_cfg.get("port", 4040)))
        )
    else:
        builder = builder.config("spark.ui.enabled", "false")

    # Iceberg REST Catalog
    default_rest = "http://iceberg-rest:8181" if is_service_reachable("http://iceberg-rest:8181/v1/config") else "http://localhost:8181"
    rest_catalog_url = os.environ.get("ICEBERG_REST_URL") or default_rest
    catalog_ok = enable_rest_catalog and is_service_reachable(f"{rest_catalog_url.rstrip('/')}/v1/config")
    minio_ok = is_service_reachable(f"{s3_endpoint.rstrip('/')}/minio/health/live")
    if must_s3a and not minio_ok:
        raise RuntimeError(
            f"ENGINE=spark under MODE=integration|full requires reachable MinIO/S3A endpoint at {s3_endpoint}."
        )
    if must_s3a and enable_rest_catalog and not catalog_ok:
        raise RuntimeError(
            f"ENGINE=spark under MODE=integration|full requires Iceberg REST catalog at {rest_catalog_url}."
        )

    if catalog_ok:
        print(f"[PySpark] Iceberg REST Catalog detected at {rest_catalog_url}. Enabling REST catalog...")
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", rest_catalog_url)
            .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
            .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint)
            .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        )
    else:
        # Local warehouse fallback (smoke only)
        builder = builder.config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("quant.dataset_id", os.environ.get("DATASET_ID", ""))
    return spark
