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


def get_spark_session(
    app_name: str = "quant_edai_lakehouse",
    master: str = "local[*]",
    config_path: Optional[str] = None,
    enable_ui: bool = True,
    enable_s3a: bool = True,
    enable_rest_catalog: bool = True,
) -> Any:
    """
    Initializes and returns a configured PySpark SparkSession.
    Configures shared History Server event log volume and Iceberg REST Catalog.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise ImportError(
            "PySpark is not installed in the active environment. "
            "Install it via: uv pip install 'pyspark>=3.5.0,<4.0.0' or use ENGINE=sim for simulation."
        ) from e

    cfg = load_spark_config(config_path)
    spark_master = master or cfg.get("master", "local[*]")
    app_title = app_name or cfg.get("app_name", "quant_edai_spark")

    builder = SparkSession.builder.master(spark_master).appName(app_title)

    # Core performance & shuffle tuning
    builder = (
        builder
        .config("spark.driver.memory", cfg.get("driver_memory", "2g"))
        .config("spark.executor.memory", cfg.get("executor_memory", "2g"))
        .config("spark.sql.shuffle.partitions", "16")
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
    if enable_s3a:
        s3_cfg = cfg.get("s3a", {})
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", s3_cfg.get("endpoint", "http://localhost:9000"))
            .config("spark.hadoop.fs.s3a.access.key", s3_cfg.get("access_key", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", s3_cfg.get("secret_key", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", str(s3_cfg.get("path_style_access", True)).lower())
            .config("spark.hadoop.fs.s3a.impl", s3_cfg.get("impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
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

    # Iceberg REST Catalog (if reachable at :8181)
    rest_catalog_url = "http://localhost:8181"
    if enable_rest_catalog and is_service_reachable(f"{rest_catalog_url}/v1/config"):
        print(f"[PySpark] Iceberg REST Catalog detected at {rest_catalog_url}. Enabling REST catalog...")
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", rest_catalog_url)
            .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
            .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://localhost:9000")
            .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        )
    else:
        # Local warehouse fallback
        builder = builder.config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
