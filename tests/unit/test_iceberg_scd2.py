"""Unit tests for Iceberg Schema Evolution, Deduplication, SCD2, and Compaction."""

from pathlib import Path
import pandas as pd
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
from quant_edai.processing.spark.iceberg_compaction import IcebergCompactionJob
from quant_edai.processing.spark.scd2_builder import SCD2DimensionBuilder


@pytest.fixture(scope="module")
def staged_data(tmp_path_factory):
    out_dir = tmp_path_factory.mktemp("iceberg_test_source")
    gen = QuantDataGenerator(seed=42, scale=0.05, out_dir=str(out_dir))
    gen.run()
    return out_dir


def test_spark_dedup_and_schema_evolution(staged_data, tmp_path):
    target_silver = tmp_path / "silver_market"
    job = DeduplicationAndSchemaEvolutionJob(
        source_dir=str(staged_data),
        target_dir=str(target_silver),
    )
    stats = job.run()

    assert stats["duplicates_removed"] > 0
    assert stats["schema_evolution_verified"] is True
    assert stats["evolved_rows_with_data"] > 0

    # Read back silver dataset
    df_silver = pd.read_parquet(target_silver)
    assert "vwap" in df_silver.columns
    assert "trade_count" in df_silver.columns
    # Check that duplicates are gone (every ticker + trade_date is unique)
    dups = df_silver.duplicated(subset=["ticker_symbol", "trade_date"])
    assert dups.sum() == 0


def test_scd2_dimension_builder(staged_data, tmp_path):
    source_dim = staged_data / "dim_companies" / "dim_companies.parquet"
    target_gold = tmp_path / "gold_dim"

    builder = SCD2DimensionBuilder(
        source_file=str(source_dim),
        target_dir=str(target_gold),
    )
    df_dim = builder.build_or_update()

    assert {"valid_from_ts", "valid_to_ts", "is_current"}.issubset(set(df_dim.columns))
    assert True in df_dim["is_current"].values
    assert False in df_dim["is_current"].values

    # Test point in time lookup
    historical_snap = builder.point_in_time_lookup(df_dim, as_of_date="2022-01-01 00:00:00")
    assert not historical_snap.empty
    assert (historical_snap["valid_from_ts"] <= "2022-01-01 00:00:00").all()
    assert (historical_snap["valid_to_ts"] >= "2022-01-01 00:00:00").all()


def test_iceberg_compaction_procedure(staged_data, tmp_path):
    target_compacted = tmp_path / "compacted_market"
    job = IcebergCompactionJob(
        table_dir=str(staged_data / "raw_market_ohlcv"),
        compacted_dir=str(target_compacted),
        target_rows_per_file=1000,
    )
    stats = job.run()

    assert stats["compaction_status"] == "SUCCESS"
    assert stats["initial_file_count"] > stats["compacted_file_count"]
    # File count reduction is substantial
    assert stats["initial_file_count"] >= 10
    assert stats["compacted_file_count"] <= 5
