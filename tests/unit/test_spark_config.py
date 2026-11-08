"""
Unit tests for Spark configuration and session factory.
"""

from pathlib import Path
from quant_edai.processing.spark.spark_session import load_spark_config


def test_load_spark_config():
    cfg = load_spark_config("configs/processing/spark.yaml")
    assert cfg is not None
    assert "aqe" in cfg
    assert cfg["aqe"]["skew_join_enabled"] is True
    assert cfg["salting"]["salt_factor"] == 16
    assert "AAPL" in cfg["salting"]["mega_caps"]
    assert cfg["iceberg"]["catalog_name"] == "lakehouse"
    assert cfg["s3a"]["endpoint"] == "http://localhost:9000"
