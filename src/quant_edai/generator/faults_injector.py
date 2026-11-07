"""
Faults Injector Module.

Injects 6 controlled, deterministic real-world anomalies into financial datasets:
1. Volume & row skew (80% concentration in Mega-Caps)
2. Schema evolution (mid-stream vwap & trade_count fields)
3. Offline duplicates (~2.0% retry duplicates)
4. Streaming late arrivals (5-10 minute delayed ticks)
5. Streaming duplicates (~1.5% at-least-once retries)
6. Market regime drift (Bull to high-volatility Bear regime transition)
"""

from __future__ import annotations

import copy
from typing import List, Sequence
import numpy as np
import pandas as pd


class FaultsInjector:
    """Injects synthetic real-world data problems according to rubric requirements."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = np.random.default_rng(seed)

    def inject_volume_skew(
        self,
        df: pd.DataFrame,
        mega_caps: Sequence[str] = ("AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA"),
        target_volume_ratio: float = 0.80,
    ) -> pd.DataFrame:
        """
        Concentrates target_volume_ratio (e.g. 80%) of total market volume in Mega-Cap tickers.

        Ensures sum(volume[mega_caps]) / sum(volume[all]) ~= target_volume_ratio.
        """
        df = df.copy()
        if "volume" not in df.columns or "ticker_symbol" not in df.columns:
            return df

        is_mega = df["ticker_symbol"].isin(mega_caps)
        num_mega = is_mega.sum()
        num_others = (~is_mega).sum()

        if num_mega == 0 or num_others == 0:
            return df

        # Current total volumes
        total_current_volume = df["volume"].sum()
        if total_current_volume <= 0:
            return df

        # We calibrate volume multiplier such that Mega-Caps achieve target_volume_ratio
        current_other_volume = df.loc[~is_mega, "volume"].sum()
        # target: other_volume / total = (1 - target_volume_ratio)
        # target_mega_volume = current_other_volume * (target_volume_ratio / (1 - target_volume_ratio))
        target_mega_volume = current_other_volume * (target_volume_ratio / (1.0 - target_volume_ratio))
        current_mega_volume = df.loc[is_mega, "volume"].sum()

        if current_mega_volume > 0:
            scale_factor = target_mega_volume / current_mega_volume
            df.loc[is_mega, "volume"] = np.round(df.loc[is_mega, "volume"] * scale_factor).astype(np.int64)

        return df

    def inject_schema_evolution(
        self,
        df: pd.DataFrame,
        boundary_date: str = "2024-06-01",
        date_col: str = "trade_date",
    ) -> pd.DataFrame:
        """
        Simulates schema evolution mid-dataset:
        - Dates < boundary_date: vwap and trade_count are NaN (or None)
        - Dates >= boundary_date: vwap and trade_count are populated
        """
        df = df.copy()
        boundary_ts = pd.to_datetime(boundary_date)
        trade_dates = pd.to_datetime(df[date_col])

        post_boundary = trade_dates >= boundary_ts

        # Generate realistic VWAP as typical price + minor noise
        typical_price = (df["high_price"] + df["low_price"] + df["close_price"]) / 3.0
        vwap_series = np.round(typical_price + self.rng.normal(0, 0.05, size=len(df)), 2)

        # Generate trade_count proportional to volume
        trade_count_series = np.maximum(
            100, np.round(df["volume"] / self.rng.integers(150, 300, size=len(df))).astype(np.int32)
        )

        df["vwap"] = np.where(post_boundary, vwap_series, np.nan)
        df["trade_count"] = np.where(post_boundary, trade_count_series, np.nan)

        return df

    def inject_offline_duplicates(
        self,
        df: pd.DataFrame,
        duplicate_ratio: float = 0.02,
    ) -> pd.DataFrame:
        """
        Injects duplicate rows with identical (ticker_symbol, trade_date) and slightly drifted ingestion_timestamp.
        """
        if duplicate_ratio <= 0 or df.empty:
            return df

        df = df.copy()
        num_duplicates = max(1, int(len(df) * duplicate_ratio))
        dup_indices = self.rng.choice(len(df), size=num_duplicates, replace=False)

        dup_rows = df.iloc[dup_indices].copy()
        # Add slight timestamp drift to ingestion_timestamp if present
        if "ingestion_timestamp" in dup_rows.columns:
            drifted_ts = pd.to_datetime(dup_rows["ingestion_timestamp"]) + pd.to_timedelta(
                self.rng.integers(1, 60, size=num_duplicates), unit="s"
            )
            dup_rows["ingestion_timestamp"] = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in drifted_ts]

        # Concatenate and shuffle
        combined = pd.concat([df, dup_rows], ignore_index=True)
        return combined

    def inject_streaming_late_arrivals(
        self,
        df_ticks: pd.DataFrame,
        late_ratio: float = 0.05,
        min_delay_seconds: int = 300,
        max_delay_seconds: int = 600,
    ) -> pd.DataFrame:
        """
        Delays publishing timestamp of late_ratio fraction of messages by 5 to 10 minutes.
        """
        df_ticks = df_ticks.copy()
        if "event_timestamp" not in df_ticks.columns:
            return df_ticks

        n_rows = len(df_ticks)
        num_late = int(n_rows * late_ratio)
        if num_late == 0:
            return df_ticks

        late_indices = self.rng.choice(n_rows, size=num_late, replace=False)
        delays = self.rng.integers(min_delay_seconds, max_delay_seconds + 1, size=num_late)

        published_ts = pd.to_datetime(df_ticks["event_timestamp"]).copy()
        for idx, delay in zip(late_indices, delays):
            published_ts.iloc[idx] = published_ts.iloc[idx] + pd.Timedelta(seconds=int(delay))

        df_ticks["published_timestamp"] = published_ts
        df_ticks["is_late"] = False
        df_ticks.iloc[late_indices, df_ticks.columns.get_loc("is_late")] = True

        return df_ticks

    def inject_streaming_duplicates(
        self,
        df_ticks: pd.DataFrame,
        duplicate_ratio: float = 0.015,
    ) -> pd.DataFrame:
        """
        Simulates Kafka producer at-least-once retries by duplicating 1.5% of tick events.
        """
        if duplicate_ratio <= 0 or df_ticks.empty:
            return df_ticks

        df_ticks = df_ticks.copy()
        n_rows = len(df_ticks)
        num_dups = max(1, int(n_rows * duplicate_ratio))
        dup_indices = self.rng.choice(n_rows, size=num_dups, replace=False)

        dups = df_ticks.iloc[dup_indices].copy()
        # Mark duplicate flag for verification audit
        if "is_duplicate" not in df_ticks.columns:
            df_ticks["is_duplicate"] = False
        dups["is_duplicate"] = True

        combined = pd.concat([df_ticks, dups], ignore_index=True)
        return combined

    def inject_market_regime_drift(
        self,
        df: pd.DataFrame,
        drift_date: str = "2024-08-01",
        volatility_multiplier: float = 2.2,
        drift_penalty: float = -0.0015,
    ) -> pd.DataFrame:
        """
        Simulates transition from low-volatility Bull market regime to high-volatility Bear regime.
        """
        df = df.copy()
        if "trade_date" not in df.columns:
            return df

        is_bear = pd.to_datetime(df["trade_date"]) >= pd.to_datetime(drift_date)
        df["market_regime"] = np.where(is_bear, "BEAR_HIGH_VOL", "BULL_LOW_VOL")

        # In bear regime, expand daily high-low spreads and increase volatility
        if "high_price" in df.columns and "low_price" in df.columns and "close_price" in df.columns:
            spread = (df["high_price"] - df["low_price"]) * (volatility_multiplier - 1.0)
            df.loc[is_bear, "high_price"] = np.round(df.loc[is_bear, "high_price"] + spread * 0.5, 2)
            df.loc[is_bear, "low_price"] = np.round(
                np.maximum(0.5, df.loc[is_bear, "low_price"] - spread * 0.5), 2
            )

        return df
