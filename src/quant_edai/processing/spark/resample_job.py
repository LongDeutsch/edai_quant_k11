"""
PySpark Batch Processing: Silver Ticks to Gold Bars Resampler Job.

Transforms Silver Zone raw ticks (stg_market_ticks) into:
1. gold_market_bars_1m: 1-minute tumbling window OHLCV + VWAP + trade_count.
2. gold_market_bars_daily: Daily session OHLCV + VWAP + trade_count.

Dual-mode support:
- ENGINE=sim: Fast pandas simulation for unit tests & CI sandboxes.
- ENGINE=spark: Real PySpark DataFrame & Spark SQL execution.
Part of Tick-First Medallion Architecture.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
import numpy as np
import pandas as pd


class SparkResampleJob:
    """Resamples Silver ticks into Gold 1-minute and Daily OHLCV bars."""

    def __init__(
        self,
        source_ticks_dir: str = "/tmp/lakehouse_source/stg_market_ticks",
        gold_1m_dir: str = "/tmp/lakehouse_source/gold_market_bars_1m",
        gold_daily_dir: str = "/tmp/lakehouse_source/gold_market_bars_daily",
        iceberg_1m_table: str = "lakehouse.gold.fact_market_bars_1m",
        iceberg_daily_table: str = "lakehouse.gold.fact_daily_ohlcv",
        dataset_id: Optional[str] = None,
    ):
        self.source_ticks_dir = Path(source_ticks_dir) if not str(source_ticks_dir).startswith("s3") else source_ticks_dir
        self.gold_1m_dir = Path(gold_1m_dir) if not str(gold_1m_dir).startswith("s3") else gold_1m_dir
        self.gold_daily_dir = Path(gold_daily_dir) if not str(gold_daily_dir).startswith("s3") else gold_daily_dir
        self.iceberg_1m_table = iceberg_1m_table
        self.iceberg_daily_table = iceberg_daily_table
        self.dataset_id = dataset_id or os.environ.get("DATASET_ID")

    def _resolve_source_path(self) -> Path:
        """Finds valid source ticks directory (stg_market_ticks or raw_market_ticks fallback)."""
        if self.source_ticks_dir.exists():
            return self.source_ticks_dir
        from quant_edai.common.dataset_paths import resolve_dataset_root
        try:
            resolved_root = resolve_dataset_root(self.source_ticks_dir.parent, self.dataset_id, allow_legacy=True)
            cand = resolved_root / self.source_ticks_dir.name
            if cand.exists():
                return cand
            cand2 = resolved_root / "raw_market_ticks"
            if cand2.exists():
                return cand2
        except Exception:
            pass
        fallback = self.source_ticks_dir.parent / "raw_market_ticks"
        if fallback.exists():
            return fallback
        raise FileNotFoundError(
            f"No tick data found at {self.source_ticks_dir} or {fallback}. Run generator or dedup first."
        )

    def run_sim(self) -> Dict[str, Any]:
        """Runs the fast pandas simulation of 1-minute and daily OHLCV bar aggregation."""
        source_path = self._resolve_source_path()
        print(f"[Spark Resample] Loading ticks from {source_path}...")
        df_ticks = pd.read_parquet(source_path)

        if len(df_ticks) == 0:
            raise ValueError(f"Source ticks directory {source_path} is empty.")

        # Ensure trade_date is string
        if "trade_date" in df_ticks.columns:
            df_ticks["trade_date"] = df_ticks["trade_date"].astype(str)

        # Parse event_timestamp to UTC datetime
        df_ticks["ts"] = pd.to_datetime(df_ticks["event_timestamp"], utc=True, format="ISO8601")
        # Ensure proper sorting by event_timestamp
        df_ticks = df_ticks.sort_values(by=["ticker_symbol", "ts"]).reset_index(drop=True)
        df_ticks["price_x_size"] = df_ticks["price"] * df_ticks["size"]

        # ------------------------------------------------------------------
        # 1. Resample to 1-Minute Tumbling Window (gold_market_bars_1m)
        # ------------------------------------------------------------------
        df_ticks["bar_1m_ts"] = df_ticks["ts"].dt.floor("1min")

        bars_1m_records = []
        for (ticker, bar_time), group in df_ticks.groupby(["ticker_symbol", "bar_1m_ts"], observed=True):
            grp = group.sort_values("ts")
            open_p = float(grp.iloc[0]["price"])
            high_p = float(grp["price"].max())
            low_p = float(grp["price"].min())
            close_p = float(grp.iloc[-1]["price"])
            vol = int(grp["size"].sum())
            px_size_sum = float(grp["price_x_size"].sum())
            vwap = round(px_size_sum / max(1, vol), 4)
            t_count = len(grp)
            t_date = str(grp.iloc[0]["trade_date"])
            exchange = str(grp.iloc[0]["exchange"])

            time_str = bar_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_time_str = (bar_time + pd.to_timedelta(1, unit="m")).strftime("%Y-%m-%dT%H:%M:%SZ")
            bar_id = f"bar_1m_{ticker}_{bar_time.strftime('%Y%m%d_%H%M%S')}"
            ds_id = str(grp.iloc[0].get("dataset_id", self.dataset_id or ""))
            split_id = str(grp.iloc[0].get("split_id", "IS"))

            bars_1m_records.append({
                "bar_id": bar_id,
                "ticker_symbol": ticker,
                "bar_timestamp": time_str,
                "window_start": time_str,
                "window_end": end_time_str,
                "event_timestamp": time_str,
                "trade_date": t_date,
                "open_price": round(open_p, 2),
                "high_price": round(high_p, 2),
                "low_price": round(low_p, 2),
                "close_price": round(close_p, 2),
                "volume": vol,
                "vwap": vwap,
                "trade_count": t_count,
                "exchange": exchange,
                "dataset_id": ds_id,
                "split_id": split_id,
            })

        df_1m = pd.DataFrame(bars_1m_records)
        df_1m.sort_values(by=["trade_date", "bar_timestamp", "ticker_symbol"], inplace=True)

        import shutil
        shutil.rmtree(self.gold_1m_dir, ignore_errors=True)
        self.gold_1m_dir.mkdir(parents=True, exist_ok=True)
        df_1m.to_parquet(self.gold_1m_dir, partition_cols=["trade_date"], index=False, max_partitions=4096)
        print(f"[Spark Resample] Wrote {len(df_1m):,} 1m bars to {self.gold_1m_dir}")

        # ------------------------------------------------------------------
        # 2. Resample to Daily Session Window (gold_market_bars_daily)
        # ------------------------------------------------------------------
        bars_daily_records = []
        for (ticker, t_date), group in df_ticks.groupby(["ticker_symbol", "trade_date"], observed=True):
            grp = group.sort_values("ts")
            open_p = float(grp.iloc[0]["price"])
            high_p = float(grp["price"].max())
            low_p = float(grp["price"].min())
            close_p = float(grp.iloc[-1]["price"])
            vol = int(grp["size"].sum())
            px_size_sum = float(grp["price_x_size"].sum())
            vwap = round(px_size_sum / max(1, vol), 4)
            t_count = len(grp)
            exchange = str(grp.iloc[0]["exchange"])
            regime = str(grp.iloc[0].get("market_regime", "BULL_LOW_VOL"))

            bar_id = f"bar_daily_{ticker}_{t_date.replace('-', '')}"
            ds_id = str(grp.iloc[0].get("dataset_id", self.dataset_id or ""))
            split_id = str(grp.iloc[0].get("split_id", "IS"))

            bars_daily_records.append({
                "trade_id": bar_id,
                "ticker_symbol": ticker,
                "trade_date": t_date,
                "open_price": round(open_p, 2),
                "high_price": round(high_p, 2),
                "low_price": round(low_p, 2),
                "close_price": round(close_p, 2),
                "volume": vol,
                "vwap": vwap,
                "trade_count": t_count,
                "exchange": exchange,
                "market_regime": regime,
                "dataset_id": ds_id,
                "split_id": split_id,
                "ingestion_timestamp": grp.iloc[-1].get("ingestion_timestamp", grp.iloc[-1]["event_timestamp"]),
            })

        df_daily = pd.DataFrame(bars_daily_records)
        df_daily.sort_values(by=["trade_date", "ticker_symbol"], inplace=True)

        shutil.rmtree(self.gold_daily_dir, ignore_errors=True)
        self.gold_daily_dir.mkdir(parents=True, exist_ok=True)
        df_daily.to_parquet(self.gold_daily_dir, partition_cols=["trade_date"], index=False, max_partitions=4096)
        print(f"[Spark Resample] Wrote {len(df_daily):,} daily bars to {self.gold_daily_dir}")

        stats = {
            "job_type": "SPARK_RESAMPLE_TICKS",
            "engine": "sim",
            "source_ticks_dir": str(source_path),
            "gold_1m_dir": str(self.gold_1m_dir),
            "gold_daily_dir": str(self.gold_daily_dir),
            "total_ticks_processed": len(df_ticks),
            "bars_1m_generated": len(df_1m),
            "bars_daily_generated": len(df_daily),
            "tickers_count": int(df_ticks["ticker_symbol"].nunique()),
            "trading_days": int(df_ticks["trade_date"].nunique()),
        }
        return stats

    def run_spark(self) -> Dict[str, Any]:
        """Runs the PySpark DataFrame aggregation using Window and Tumbling Window functions."""
        from quant_edai.processing.spark.spark_session import get_spark_session
        from pyspark.sql.window import Window
        import pyspark.sql.functions as F

        spark = get_spark_session(app_name="quant_edai_resample_spark", master="local[*]")

        # Check if Iceberg silver table is available in lakehouse catalog
        has_silver_table = False
        try:
            spark.sql("REFRESH TABLE lakehouse.silver.stg_market_ticks")
            df_silver = spark.table("lakehouse.silver.stg_market_ticks")
            if self.dataset_id:
                df_silver = df_silver.filter(F.col("dataset_id") == self.dataset_id)
            if df_silver.count() > 0:
                has_silver_table = True
                df_raw = df_silver
                print(f"[PySpark Resample] Sourcing {df_raw.count()} clean Silver ticks directly from lakehouse.silver.stg_market_ticks (dataset_id={self.dataset_id})")
        except Exception:
            has_silver_table = False

        if not has_silver_table:
            src_str = str(self.source_ticks_dir).rstrip("/")
            if "raw_market_ticks" in src_str:
                raise ValueError(
                    f"Architecture Violation: SparkResampleJob is forbidden from reading raw Bronze ticks directly ({src_str}). "
                    "Must read clean Silver ticks from lakehouse.silver.stg_market_ticks."
                )
            is_s3 = src_str.startswith("s3a://") or src_str.startswith("s3://")
            if is_s3:
                df_raw = spark.read.option("mergeSchema", "true").parquet(src_str)
            else:
                source_path = self._resolve_source_path()
                df_raw = spark.read.option("mergeSchema", "true").parquet(str(source_path))
        else:
            src_str = "lakehouse.silver.stg_market_ticks"
            is_s3 = False

        total_ticks = df_raw.count()

        ds_id = self.dataset_id or os.environ.get("DATASET_ID", "default")
        if "dataset_id" not in df_raw.columns:
            df_raw = df_raw.withColumn("dataset_id", F.lit(ds_id))
        else:
            df_raw = df_raw.withColumn("dataset_id", F.coalesce(F.col("dataset_id"), F.lit(ds_id)))

        # Parse event timestamp in UTC
        df_ticks = (
            df_raw
            .withColumn("ts", F.to_timestamp(F.col("event_timestamp")))
            .withColumn("price_x_size", F.col("price") * F.col("size"))
        )

        # 1. 1-Minute Tumbling Window
        df_with_1m = df_ticks.withColumn("bar_1m_ts", F.date_trunc("minute", F.col("ts")))

        # Tie-breaker: order by (ts, trade_id) for deterministic OHLCV
        win_1m_asc = Window.partitionBy("dataset_id", "ticker_symbol", "bar_1m_ts").orderBy(F.col("ts").asc(), F.col("trade_id").asc())
        win_1m_desc = Window.partitionBy("dataset_id", "ticker_symbol", "bar_1m_ts").orderBy(F.col("ts").desc(), F.col("trade_id").desc())

        df_1m_annotated = (
            df_with_1m
            .withColumn("open_p", F.first("price").over(win_1m_asc))
            .withColumn("close_p", F.first("price").over(win_1m_desc))
        )

        df_1m = (
            df_1m_annotated
            .groupBy("dataset_id", "ticker_symbol", "trade_date", "bar_1m_ts")
            .agg(
                F.first("open_p").alias("open_price"),
                F.max("price").alias("high_price"),
                F.min("price").alias("low_price"),
                F.first("close_p").alias("close_price"),
                F.sum("size").alias("volume"),
                F.round(F.sum("price_x_size") / F.greatest(F.lit(1), F.sum("size")), 4).alias("vwap"),
                F.count("*").alias("trade_count"),
                F.first("exchange").alias("exchange"),
            )
            .withColumn("bar_timestamp", F.date_format("bar_1m_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .withColumn("window_start", F.date_format("bar_1m_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .withColumn("window_end", F.date_format(F.col("bar_1m_ts") + F.expr("INTERVAL 1 MINUTE"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .withColumn(
                "bar_id",
                F.concat(
                    F.lit("bar_1m_"),
                    F.col("ticker_symbol"),
                    F.lit("_"),
                    F.date_format("bar_1m_ts", "yyyyMMdd_HHmmss"),
                ),
            )
            .withColumn("created_at", F.current_timestamp())
            .drop("bar_1m_ts")
        )

        # 2. Daily Window (Tie-breaker on trade_id)
        win_daily_asc = Window.partitionBy("dataset_id", "ticker_symbol", "trade_date").orderBy(F.col("ts").asc(), F.col("trade_id").asc())
        win_daily_desc = Window.partitionBy("dataset_id", "ticker_symbol", "trade_date").orderBy(F.col("ts").desc(), F.col("trade_id").desc())

        df_daily_annotated = (
            df_ticks
            .withColumn("open_p", F.first("price").over(win_daily_asc))
            .withColumn("close_p", F.first("price").over(win_daily_desc))
        )

        df_daily = (
            df_daily_annotated
            .groupBy("dataset_id", "ticker_symbol", "trade_date")
            .agg(
                F.first("open_p").alias("open_price"),
                F.max("price").alias("high_price"),
                F.min("price").alias("low_price"),
                F.first("close_p").alias("close_price"),
                F.sum("size").alias("volume"),
                F.round(F.sum("price_x_size") / F.greatest(F.lit(1), F.sum("size")), 4).alias("vwap"),
                F.count("*").alias("trade_count"),
                F.first("exchange").alias("exchange"),
            )
            .withColumn(
                "trade_id",
                F.concat(
                    F.lit("bar_daily_"),
                    F.col("ticker_symbol"),
                    F.lit("_"),
                    F.regexp_replace(F.col("trade_date"), "-", ""),
                ),
            )
            .withColumn("created_at", F.current_timestamp())
        )

        count_1m = df_1m.count()
        count_daily = df_daily.count()

        # Write to Iceberg tables if REST catalog is enabled
        snapshots_1m = []
        snapshots_daily = []
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
            # 1m Iceberg Table
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.iceberg_1m_table} (
                    bar_id STRING,
                    ticker_symbol STRING,
                    bar_timestamp STRING,
                    window_start STRING,
                    window_end STRING,
                    trade_date STRING,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    volume BIGINT,
                    vwap DOUBLE,
                    trade_count BIGINT,
                    exchange STRING,
                    dataset_id STRING,
                    created_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (dataset_id, trade_date)
                TBLPROPERTIES ('format-version'='2')
            """)
            df_1m.createOrReplaceTempView("gold_1m_batch")
            spark.sql(f"""
                MERGE INTO {self.iceberg_1m_table} t
                USING gold_1m_batch s
                ON t.dataset_id = s.dataset_id AND t.bar_id = s.bar_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            spark.sql(f"REFRESH TABLE {self.iceberg_1m_table}")
            snap_1m = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {self.iceberg_1m_table}.snapshots ORDER BY committed_at DESC LIMIT 5").collect()
            snapshots_1m = [{"snapshot_id": str(r["snapshot_id"]), "committed_at": str(r["committed_at"]), "operation": str(r["operation"])} for r in snap_1m]

            # Daily Iceberg Table
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.iceberg_daily_table} (
                    trade_id STRING,
                    ticker_symbol STRING,
                    trade_date STRING,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    volume BIGINT,
                    vwap DOUBLE,
                    trade_count BIGINT,
                    exchange STRING,
                    dataset_id STRING,
                    created_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (dataset_id, trade_date)
                TBLPROPERTIES ('format-version'='2')
            """)
            df_daily.createOrReplaceTempView("gold_daily_batch")
            spark.sql(f"""
                MERGE INTO {self.iceberg_daily_table} t
                USING gold_daily_batch s
                ON t.dataset_id = s.dataset_id AND t.trade_id = s.trade_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            spark.sql(f"REFRESH TABLE {self.iceberg_daily_table}")
            snap_daily = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {self.iceberg_daily_table}.snapshots ORDER BY committed_at DESC LIMIT 5").collect()
            snapshots_daily = [{"snapshot_id": str(r["snapshot_id"]), "committed_at": str(r["committed_at"]), "operation": str(r["operation"])} for r in snap_daily]
        except Exception as e:
            print(f"[PySpark Resample] Warning during Iceberg write: {e}")

        # Local/S3 Parquet persistence
        if not is_s3:
            Path(self.gold_1m_dir).mkdir(parents=True, exist_ok=True)
            df_1m.write.mode("overwrite").partitionBy("trade_date").parquet(str(self.gold_1m_dir))
            Path(self.gold_daily_dir).mkdir(parents=True, exist_ok=True)
            df_daily.write.mode("overwrite").partitionBy("trade_date").parquet(str(self.gold_daily_dir))

        # Mathematical volume conservation assertion
        sum_vol_1m = df_1m.agg(F.sum("volume")).collect()[0][0] or 0
        sum_vol_daily = df_daily.agg(F.sum("volume")).collect()[0][0] or 0

        stats = {
            "job_type": "SPARK_RESAMPLE_TICKS_PYSPARK",
            "engine": "pyspark",
            "source_ticks_dir": str(src_str),
            "gold_1m_dir": str(self.gold_1m_dir),
            "gold_daily_dir": str(self.gold_daily_dir),
            "iceberg_1m_table": self.iceberg_1m_table,
            "iceberg_daily_table": self.iceberg_daily_table,
            "iceberg_snapshots_1m": snapshots_1m,
            "iceberg_snapshots_daily": snapshots_daily,
            "total_ticks_processed": total_ticks,
            "bars_1m_generated": count_1m,
            "bars_daily_generated": count_daily,
            "sum_volume_1m": int(sum_vol_1m),
            "sum_volume_daily": int(sum_vol_daily),
            "volume_conserved": (sum_vol_1m == sum_vol_daily),
        }
        return stats

    def run(self, engine: Optional[str] = None) -> Dict[str, Any]:
        """Runs either simulation or real PySpark job based on ENGINE setting."""
        from quant_edai.common.engine_guard import run_with_engine

        return run_with_engine(
            engine=engine,
            run_sim=self.run_sim,
            run_live=self.run_spark,
            live_token="spark",
            job_name="Spark Resample",
        )


def main():
    parser = argparse.ArgumentParser(description="Spark Resample Ticks to 1m and Daily Bars")
    parser.add_argument("--source-dir", type=str, default="/tmp/lakehouse_source/stg_market_ticks")
    parser.add_argument("--gold-1m-dir", type=str, default="/tmp/lakehouse_source/gold_market_bars_1m")
    parser.add_argument("--gold-daily-dir", type=str, default="/tmp/lakehouse_source/gold_market_bars_daily")
    parser.add_argument("--engine", type=str, default=None, choices=["sim", "spark"])
    parser.add_argument("--out-file", type=str, default=None, help="Optional output JSON file")

    args = parser.parse_args()
    job = SparkResampleJob(
        source_ticks_dir=args.source_dir,
        gold_1m_dir=args.gold_1m_dir,
        gold_daily_dir=args.gold_daily_dir,
    )
    stats = job.run(engine=args.engine)

    if args.out_file:
        out_p = Path(args.out_file)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        with open(out_p, "w") as f:
            json.dump(stats, f, indent=2)
        print(f"[Spark Resample] Saved stats to {args.out_file}")
    else:
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
