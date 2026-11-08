"""Unit tests for Spark Baseline vs Skew-Optimized processing."""

from pathlib import Path
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.processing.spark.baseline_job import BaselineSparkJob
from quant_edai.processing.spark.skew_optimized import SkewOptimizedSparkJob


@pytest.fixture(scope="module")
def staged_data(tmp_path_factory):
    """Generates synthetic dataset for batch processing tests."""
    out_dir = tmp_path_factory.mktemp("spark_test_source")
    gen = QuantDataGenerator(seed=42, scale=0.05, out_dir=str(out_dir))
    gen.run()
    return out_dir


def test_spark_baseline_stragglers(staged_data):
    job = BaselineSparkJob(source_dir=str(staged_data), num_partitions=16)
    stats = job.run()

    assert stats["total_rows_processed"] > 0
    assert stats["joined_rows"] > 0
    assert stats["aggregated_groups"] > 0
    # Skew causes max task duration to be significantly higher than median
    assert stats["max_task_duration_seconds"] > stats["median_task_duration_seconds"]
    assert stats["task_duration_variance"] > 0


def test_spark_skew_optimized_balancing(staged_data):
    baseline_job = BaselineSparkJob(source_dir=str(staged_data), num_partitions=16)
    baseline_stats = baseline_job.run()

    opt_job = SkewOptimizedSparkJob(source_dir=str(staged_data), num_partitions=16, salt_factor=16)
    opt_stats = opt_job.run()

    # Optimized job must process same number of rows
    assert opt_stats["total_rows_processed"] == baseline_stats["total_rows_processed"]

    # Variance across task durations must be strictly lower under salting
    assert opt_stats["task_duration_variance"] < baseline_stats["task_duration_variance"]
    # Max task duration must be lower
    assert opt_stats["max_task_duration_seconds"] < baseline_stats["max_task_duration_seconds"]
    # Effective latency must show speedup
    assert opt_stats["effective_stage_latency_seconds"] < baseline_stats["effective_stage_latency_seconds"]
