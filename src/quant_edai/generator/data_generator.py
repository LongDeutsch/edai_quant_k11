"""
Synthetic Quant Data Generator Engine.

Generates reproducible market time-series, unstructured financial news, labels, and company dimensions
under controlled stochastic processes (Geometric Brownian Motion + Jump-Diffusion),
with automated injection of the 6 required rubric data problems.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd

from quant_edai.generator.faults_injector import FaultsInjector


class QuantDataGenerator:
    """Core synthetic data generation engine for the Quant EDAI Platform."""

    def __init__(
        self,
        seed: int = 42,
        scale: float = 0.1,
        seed_file: Optional[str] = None,
        out_dir: str = "/tmp/lakehouse_source",
        start_date: str = "2024-01-02",
        end_date: str = "2024-10-31",
    ):
        self.seed = seed
        self.scale = max(0.001, scale)
        self.out_dir = Path(out_dir)
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.rng = np.random.default_rng(seed)
        self.faults = FaultsInjector(seed=seed)

        # Load ticker metadata from seed file
        self.seed_file = seed_file or str(
            Path(__file__).parents[3] / "configs" / "seeds" / "tickers.json"
        )
        self.tickers_meta = self._load_seed_tickers()

    def _load_seed_tickers(self) -> Dict[str, Any]:
        """Loads ticker seeds; falls back to default Top 16 US Equities if file missing."""
        if os.path.exists(self.seed_file):
            with open(self.seed_file, "r") as f:
                return json.load(f)
        # Built-in fallback
        return {
            "mega_caps": ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "TSLA"],
            "tickers": [
                {"ticker": "AAPL", "base_price": 182.5, "base_volatility": 0.22, "base_volume": 55000000, "market_cap_category": "Mega-Cap", "sector": "Technology", "industry": "Consumer Electronics", "name": "Apple Inc."},
                {"ticker": "NVDA", "base_price": 128.0, "base_volatility": 0.38, "base_volume": 65000000, "market_cap_category": "Mega-Cap", "sector": "Technology", "industry": "Semiconductors", "name": "NVIDIA Corporation"},
                {"ticker": "MSFT", "base_price": 420.0, "base_volatility": 0.20, "base_volume": 25000000, "market_cap_category": "Mega-Cap", "sector": "Technology", "industry": "Software - Infrastructure", "name": "Microsoft Corporation"},
                {"ticker": "AMZN", "base_price": 185.0, "base_volatility": 0.26, "base_volume": 35000000, "market_cap_category": "Mega-Cap", "sector": "Consumer Cyclical", "industry": "Internet Retail", "name": "Amazon.com Inc."},
                {"ticker": "GOOGL", "base_price": 165.0, "base_volatility": 0.24, "base_volume": 28000000, "market_cap_category": "Mega-Cap", "sector": "Communication Services", "industry": "Internet Content", "name": "Alphabet Inc."},
                {"ticker": "META", "base_price": 490.0, "base_volatility": 0.32, "base_volume": 20000000, "market_cap_category": "Mega-Cap", "sector": "Communication Services", "industry": "Internet Content", "name": "Meta Platforms Inc."},
                {"ticker": "TSLA", "base_price": 215.0, "base_volatility": 0.45, "base_volume": 75000000, "market_cap_category": "Mega-Cap", "sector": "Consumer Cyclical", "industry": "Auto Manufacturers", "name": "Tesla Inc."},
                {"ticker": "SPY", "base_price": 540.0, "base_volatility": 0.15, "base_volume": 60000000, "market_cap_category": "Benchmark", "sector": "Benchmark", "industry": "ETF", "name": "SPDR S&P 500 ETF Trust"}
            ]
        }

    def generate_trading_calendar(self) -> pd.DatetimeIndex:
        """Generates business trading days (excluding weekends)."""
        all_dates = pd.date_range(self.start_date, self.end_date, freq="B")
        return all_dates

    def generate_market_ohlcv(self) -> pd.DataFrame:
        """
        Generates daily OHLCV prices using Geometric Brownian Motion with Jump-Diffusion.
        Applies volume skew, schema evolution, and offline duplicate anomalies.
        """
        trading_days = self.generate_trading_calendar()
        num_days = len(trading_days)
        records = []
        now_ts = pd.Timestamp.now(tz="UTC")

        for item in self.tickers_meta["tickers"]:
            ticker = item["ticker"]
            s0 = float(item.get("base_price", 100.0))
            vol = float(item.get("base_volatility", 0.25))
            base_vol = int(item.get("base_volume", 1000000))

            dt = 1.0 / 252.0
            mu = 0.08  # 8% annualized expected drift

            # Daily log returns: r_t = (mu - 0.5 * vol^2)*dt + vol*sqrt(dt)*Z
            daily_innovations = self.rng.normal(0, 1, size=num_days)
            # Occasional jump diffusion
            jumps = (self.rng.uniform(0, 1, size=num_days) < 0.02) * self.rng.normal(-0.02, 0.04, size=num_days)
            log_returns = (mu - 0.5 * vol**2) * dt + vol * np.sqrt(dt) * daily_innovations + jumps
            price_path = s0 * np.exp(np.cumsum(log_returns))

            for day_idx, t_date in enumerate(trading_days):
                close_p = float(price_path[day_idx])
                day_vol = vol * np.sqrt(dt)
                open_p = float(close_p * (1.0 + self.rng.normal(0, day_vol * 0.4)))
                high_p = float(max(open_p, close_p) * (1.0 + abs(self.rng.normal(0, day_vol * 0.6))))
                low_p = float(min(open_p, close_p) * (1.0 - abs(self.rng.normal(0, day_vol * 0.6))))
                low_p = max(0.01, low_p)

                # Base volume with log-normal noise
                vol_noise = self.rng.lognormal(0, 0.3)
                volume = int(max(1000, base_vol * vol_noise * self.scale))

                records.append({
                    "trade_id": f"trd_{ticker}_{t_date.strftime('%Y%m%d')}",
                    "ticker_symbol": ticker,
                    "trade_date": t_date.strftime("%Y-%m-%d"),
                    "open_price": round(open_p, 2),
                    "high_price": round(high_p, 2),
                    "low_price": round(low_p, 2),
                    "close_price": round(close_p, 2),
                    "volume": volume,
                    "exchange": "NASDAQ" if item.get("sector") == "Technology" else "NYSE",
                    "ingestion_timestamp": now_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })

        df = pd.DataFrame(records)

        # 1. Inject Volume Skew (80% into Mega-Caps)
        mega_caps = self.tickers_meta.get("mega_caps", [])
        df = self.faults.inject_volume_skew(df, mega_caps=mega_caps, target_volume_ratio=0.80)

        # 2. Inject Market Regime Drift (post-2024-08-01 Bear regime)
        df = self.faults.inject_market_regime_drift(df, drift_date="2024-08-01")

        # 3. Inject Schema Evolution (post-2024-06-01 vwap & trade_count)
        df = self.faults.inject_schema_evolution(df, boundary_date="2024-06-01")

        # 4. Inject Offline Duplicates (~2.0%)
        df = self.faults.inject_offline_duplicates(df, duplicate_ratio=0.02)

        return df

    def generate_labels(self, df_market: pd.DataFrame) -> pd.DataFrame:
        """
        Generates forward-looking return direction labels (Up=1, Down=0) for ML training.
        Row contains (ticker_symbol, label_date, next_day_close, daily_return_fwd, direction_label).
        """
        # Exclude duplicate rows for pristine label generation
        df_clean = df_market.drop_duplicates(subset=["ticker_symbol", "trade_date"]).sort_values(
            by=["ticker_symbol", "trade_date"]
        )

        labels = []
        for ticker, group in df_clean.groupby("ticker_symbol"):
            group = group.copy().reset_index(drop=True)
            group["next_day_close"] = group["close_price"].shift(-1)
            group["daily_return_fwd"] = (group["next_day_close"] - group["close_price"]) / group["close_price"]
            group["direction_label"] = (group["daily_return_fwd"] > 0).astype(int)

            # Drop the last day since it has no forward label
            valid = group.dropna(subset=["next_day_close"])
            for _, row in valid.iterrows():
                labels.append({
                    "ticker_symbol": ticker,
                    "label_date": row["trade_date"],
                    "close_price": row["close_price"],
                    "next_day_close": round(row["next_day_close"], 2),
                    "daily_return_fwd": round(row["daily_return_fwd"], 6),
                    "direction_label": int(row["direction_label"]),
                })

        return pd.DataFrame(labels)

    def generate_financial_news(self, df_market: pd.DataFrame) -> pd.DataFrame:
        """Generates unstructured financial news articles linked to tickers and dates."""
        mega_caps = set(self.tickers_meta.get("mega_caps", []))
        templates = [
            ("beats Q{q} revenue estimates driven by AI demand", 0.65),
            ("faces regulatory scrutiny over market dominance", -0.45),
            ("announces expansion into cloud and enterprise services", 0.35),
            ("reports supply chain bottlenecks impacting Q{q} guidance", -0.55),
            ("unveils next-generation chip architecture at developer conference", 0.80),
            ("analysts maintain overweight rating with target price upgrade", 0.50),
            ("shares dip amidst broad market macroeconomic headwind", -0.30),
        ]

        articles = []
        unique_dates = df_market["trade_date"].unique()

        article_id_counter = 100000
        for item in self.tickers_meta["tickers"]:
            ticker = item["ticker"]
            name = item["name"]
            is_mega = ticker in mega_caps

            # Mega-caps generate 4x more news articles (news skew)
            prob_news = 0.60 if is_mega else 0.15

            for t_date in unique_dates:
                if self.rng.uniform(0, 1) < prob_news:
                    template_text, base_sentiment = templates[self.rng.integers(0, len(templates))]
                    q_num = self.rng.integers(1, 5)
                    headline = f"{name} ({ticker}) {template_text.format(q=q_num)}"
                    sentiment = float(np.clip(base_sentiment + self.rng.normal(0, 0.15), -1.0, 1.0))

                    articles.append({
                        "article_id": f"art_{article_id_counter}",
                        "ticker_symbol": ticker,
                        "publish_date": t_date,
                        "published_ts": f"{t_date}T{self.rng.integers(8, 20):02d}:{self.rng.integers(0, 60):02d}:00Z",
                        "headline": headline,
                        "summary": f"Market report discussing {name}'s performance, macroeconomic factors, and guidance.",
                        "sentiment_score": round(sentiment, 4),
                        "source_outlet": self.rng.choice(["Bloomberg", "Reuters", "WSJ", "CNBC"]),
                    })
                    article_id_counter += 1

        df_news = pd.DataFrame(articles)
        return df_news

    def generate_dim_companies(self) -> pd.DataFrame:
        """Generates Slowly Changing Dimension (SCD Type 2) company dimension table."""
        companies = []
        for idx, item in enumerate(self.tickers_meta["tickers"]):
            ticker = item["ticker"]
            # Historic version (active from 2020-01-01 to 2024-03-31)
            companies.append({
                "company_key": int(f"100{idx+1}01"),
                "ticker_symbol": ticker,
                "company_name": item["name"],
                "sector": item["sector"],
                "industry": item["industry"],
                "market_cap_category": "Mid-Cap" if item.get("market_cap_category") == "Large-Cap" else item.get("market_cap_category"),
                "valid_from_ts": "2020-01-01 00:00:00",
                "valid_to_ts": "2024-03-31 23:59:59",
                "is_current": False,
            })
            # Current active version (promoted to Large/Mega cap from 2024-04-01)
            companies.append({
                "company_key": int(f"100{idx+1}02"),
                "ticker_symbol": ticker,
                "company_name": item["name"],
                "sector": item["sector"],
                "industry": item["industry"],
                "market_cap_category": item.get("market_cap_category"),
                "valid_from_ts": "2024-04-01 00:00:00",
                "valid_to_ts": "9999-12-31 23:59:59",
                "is_current": True,
            })

        return pd.DataFrame(companies)

    def compute_cardinality_profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Computes cardinality metrics for each column:
        Cardinality Ratio = unique values / total rows.
        Categorizes columns into HIGH (>= 0.90), MEDIUM (>= 0.05), LOW (>= 0.001), ULTRA_LOW (< 0.001).
        """
        total_rows = len(df)
        if total_rows == 0:
            return {}

        profile = {}
        for col in df.columns:
            unique_cnt = int(df[col].nunique(dropna=False))
            ratio = round(unique_cnt / total_rows, 6)
            if ratio >= 0.90:
                category = "HIGH"
            elif ratio >= 0.05:
                category = "MEDIUM"
            elif ratio >= 0.001:
                category = "LOW"
            else:
                category = "ULTRA_LOW"

            profile[col] = {
                "unique_values": unique_cnt,
                "total_rows": total_rows,
                "cardinality_ratio": ratio,
                "category": category,
            }
        return profile

    def run(self) -> Dict[str, Any]:
        """Runs end-to-end data generation and writes outputs to disk."""
        self.out_dir.mkdir(parents=True, exist_ok=True)

        print(f"[Generator] Starting generation: SCALE={self.scale}, SEED={self.seed} -> {self.out_dir}")

        # 1. Market OHLCV
        df_market = self.generate_market_ohlcv()
        market_dir = self.out_dir / "raw_market_ohlcv"
        market_dir.mkdir(parents=True, exist_ok=True)
        # Write partitioned by trade_date
        df_market.to_parquet(market_dir, partition_cols=["trade_date"], index=False)

        # 2. Labels
        df_labels = self.generate_labels(df_market)
        labels_dir = self.out_dir / "label_ticker_daily_return"
        labels_dir.mkdir(parents=True, exist_ok=True)
        df_labels.to_parquet(labels_dir / "labels.parquet", index=False)

        # 3. Financial News
        df_news = self.generate_financial_news(df_market)
        news_dir = self.out_dir / "raw_financial_news"
        news_dir.mkdir(parents=True, exist_ok=True)
        df_news.to_parquet(news_dir, partition_cols=["publish_date"], index=False)

        # 4. Dimension Companies
        df_dim = self.generate_dim_companies()
        dim_dir = self.out_dir / "dim_companies"
        dim_dir.mkdir(parents=True, exist_ok=True)
        df_dim.to_parquet(dim_dir / "dim_companies.parquet", index=False)

        # Statistics computation
        total_market_vol = int(df_market["volume"].sum())
        mega_caps = set(self.tickers_meta.get("mega_caps", []))
        mega_vol = int(df_market.loc[df_market["ticker_symbol"].isin(mega_caps), "volume"].sum())
        skew_ratio = round(mega_vol / max(1, total_market_vol), 4)

        # Count duplicates
        clean_rows = len(df_market.drop_duplicates(subset=["ticker_symbol", "trade_date"]))
        total_rows = len(df_market)
        dup_rate = round((total_rows - clean_rows) / max(1, clean_rows), 4)

        # Schema evolution count
        evolved_rows = int(df_market["vwap"].notna().sum())

        # Cardinality profile across all generated entities (Rubric M1-05 requirement)
        cardinality_profile = {
            "market_ohlcv": self.compute_cardinality_profile(df_market),
            "financial_news": self.compute_cardinality_profile(df_news),
            "dim_companies": self.compute_cardinality_profile(df_dim),
            "labels": self.compute_cardinality_profile(df_labels),
        }

        stats = {
            "scale": self.scale,
            "seed": self.seed,
            "total_market_rows": total_rows,
            "unique_market_rows": clean_rows,
            "offline_duplicate_rate": dup_rate,
            "total_volume": total_market_vol,
            "mega_cap_volume": mega_vol,
            "volume_skew_ratio": skew_ratio,
            "schema_evolved_rows": evolved_rows,
            "total_news_articles": len(df_news),
            "total_label_rows": len(df_labels),
            "total_dimension_rows": len(df_dim),
            "cardinality_profile": cardinality_profile,
            "out_dir": str(self.out_dir),
        }

        stats_path = self.out_dir / "generation_stats.json"
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2)

        # Synchronize with evidence directory if present
        evidence_dir = Path(__file__).parents[3] / "evidence" / "phase1"
        if evidence_dir.exists():
            with open(evidence_dir / "data_generation_stats.json", "w") as f:
                json.dump(stats, f, indent=2)

        print(f"[Generator] Completed successfully! Stats: {json.dumps(stats, indent=2)}")
        return stats


def main():
    parser = argparse.ArgumentParser(description="Quant EDAI Synthetic Data Generator")
    parser.add_argument("--scale", type=float, default=0.1, help="Scale factor (0.1 for smoke/local, 1.0 for full)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible outputs")
    parser.add_argument("--out-dir", type=str, default="/tmp/lakehouse_source", help="Output directory")
    parser.add_argument("--seed-file", type=str, default=None, help="Path to seed tickers JSON")

    args = parser.parse_args()
    gen = QuantDataGenerator(
        scale=args.scale,
        seed=args.seed,
        out_dir=args.out_dir,
        seed_file=args.seed_file,
    )
    gen.run()


if __name__ == "__main__":
    main()
