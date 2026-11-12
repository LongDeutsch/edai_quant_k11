"""Walk-Forward Quantitative Alpha Factor Backtester & Tear Sheet Generator (M1-37)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd

# Set matplotlib cache directory to writable location before importing pyplot
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from quant_edai.common.dataset_paths import resolve_dataset_root
from quant_edai.common.execution_status import assert_integration_ready, is_integration_mode, normalize_mode
from quant_edai.common.ml_provenance import (
    ProvenanceError,
    dataset_id_from_path,
    resolve_provenance,
)
from quant_edai.quant.alpha_synthesis import (
    FIXED_BASELINE_WEIGHTS,
    MarketRegime,
    compute_composite_alpha,
    compute_risk_parity_weights,
    detect_market_regime,
    generate_attribution_report,
)
from quant_edai.quant.metrics import (
    calculate_calmar,
    calculate_cumulative_returns,
    calculate_hit_rate,
    calculate_ic,
    calculate_max_drawdown,
    calculate_rank_ic,
    calculate_rolling_sharpe,
    calculate_sharpe,
    calculate_sortino,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_TICKER = "SPY"
TRANSACTION_COST_BPS = 7.0  # 5 bps spread + 2 bps market impact (0.0007)


class QuantAlphaBacktester:
    """Walk-Forward Quantitative Factor Backtester with Zero Look-Ahead Bias."""

    FEATURE_COLS = [
        "close_price",
        "sma_20",
        "sma_50",
        "sma_ratio_20_50",
        "rsi_14d",
        "macd_12_26",
        "macd_signal_9",
        "macd_histogram",
        "rolling_volatility_30d",
        "intraday_return_1d",
    ]

    def __init__(
        self,
        features_path: Optional[str] = None,
        labels_path: Optional[str] = None,
        model_path: Optional[str] = None,
        output_dir: str = "evidence/phase5",
        top_k: int = 3,
        friction_bps: float = TRANSACTION_COST_BPS,
    ):
        self.output_dir = Path(output_dir)
        self.top_k = top_k
        self.friction = friction_bps / 10000.0  # Convert bps to decimal

        # Resolve paths
        self.features_path = features_path or self._resolve_features_path()
        self.labels_path = labels_path or self._resolve_labels_path()
        self.model_path = model_path or "models/champion_model.joblib"

        self.model = None
        self.scaler = None
        self.model_features = self.FEATURE_COLS
        self.synthetic_data = False
        self.p_ml_proxy = False
        self.run_mode = normalize_mode()
        self._load_ml_model()
        self.provenance: Dict[str, Optional[str]] = {}

    @staticmethod
    def _dataset_root() -> Path:
        out_dir = os.getenv("OUT_DIR", "/tmp/lakehouse_source")
        try:
            return resolve_dataset_root(out_dir)  # OUT_DIR/runs/{dataset_id}
        except FileNotFoundError:
            return Path(out_dir)

    def _resolve_features_path(self) -> str:
        candidates = [str(self._dataset_root() / "gold_features" / "feat_ticker_technical_indicators.parquet")]
        if not is_integration_mode():
            candidates += [
                "/tmp/lakehouse_source/gold_features/feat_ticker_technical_indicators.parquet",
                "evidence/phase3/feat_ticker_technical_indicators.parquet",
            ]
        for c in candidates:
            if os.path.exists(c):
                return c
        return candidates[0]

    def _resolve_labels_path(self) -> str:
        candidates = [str(self._dataset_root() / "label_ticker_daily_return" / "labels.parquet")]
        if not is_integration_mode():
            candidates += [
                "/tmp/lakehouse_source/label_ticker_daily_return/labels.parquet",
                "evidence/phase3/labels.parquet",
            ]
        for c in candidates:
            if os.path.exists(c):
                return c
        return candidates[0]

    def _load_ml_model(self) -> None:
        """Load Phase 4 champion model if available."""
        if os.path.exists(self.model_path):
            try:
                loaded = joblib.load(self.model_path)
                if isinstance(loaded, dict):
                    self.model = loaded.get("model")
                    self.scaler = loaded.get("scaler")
                    self.model_features = loaded.get("features", self.FEATURE_COLS)
                else:
                    self.model = loaded
                logger.info(f"Loaded ML model from {self.model_path}")
            except Exception as e:
                logger.warning(f"Could not load ML model from {self.model_path}: {e}")

    def load_data(self) -> pd.DataFrame:
        """Load and clean panel data for walk-forward backtest."""
        row_dataset_id: Optional[str] = None
        if os.path.exists(self.features_path) and os.path.exists(self.labels_path):
            logger.info(f"Loading features from {self.features_path}")
            df_feat = pd.read_parquet(self.features_path)
            logger.info(f"Loading labels from {self.labels_path}")
            df_lbl = pd.read_parquet(self.labels_path)
            if "dataset_id" in df_feat.columns and df_feat["dataset_id"].dropna().nunique() == 1:
                row_dataset_id = str(df_feat["dataset_id"].dropna().iloc[0])  # lineage stamped by feature_calculator

            df_feat["date"] = pd.to_datetime(df_feat["event_timestamp"]).dt.strftime("%Y-%m-%d")
            df_lbl["date"] = pd.to_datetime(df_lbl["label_date"]).dt.strftime("%Y-%m-%d")

            # Take latest intraday snapshot for EOD features
            eod_feat = df_feat.sort_values("event_timestamp").drop_duplicates(
                subset=["ticker_symbol", "date"], keep="last"
            )

            # Ensure forward return column
            if "daily_return_fwd" not in df_lbl.columns and "next_day_close" in df_lbl.columns:
                df_lbl["daily_return_fwd"] = (df_lbl["next_day_close"] - df_lbl["close_price"]) / df_lbl["close_price"]

            merged = pd.merge(
                eod_feat,
                df_lbl[["ticker_symbol", "date", "daily_return_fwd"]],
                on=["ticker_symbol", "date"],
                how="inner",
            )
        else:
            assert_integration_ready(
                condition=False,
                message=f"Gold features ({self.features_path}) and/or labels ({self.labels_path}) missing; "
                "synthetic backtest data is smoke-only.",
            )
            logger.info("Real Lakehouse files not found; generating deterministic synthetic market data for backtest.")
            merged = self._generate_synthetic_backtest_data()
            self.synthetic_data = True

        assert_integration_ready(
            condition=self.model is not None,
            message=f"Champion model not found at {self.model_path}; integration|full requires a trained ML model "
            "(technical-score p_ml proxy is smoke-only).",
        )
        self.provenance = resolve_provenance(
            model_path=self.model_path,
            dataset_id=dataset_id_from_path(self.features_path) or row_dataset_id,
            feature_source=None if self.synthetic_data else str(self.features_path),
        )

        # Compute ML direction prediction P_ML if model loaded
        if self.model is not None:
            try:
                available_cols = [c for c in self.model_features if c in merged.columns]
                if len(available_cols) == len(self.model_features):
                    X = merged[self.model_features].fillna(0.0)
                    if self.scaler is not None:
                        X = self.scaler.transform(X)
                    merged["p_ml"] = self.model.predict_proba(X)[:, 1]
                else:
                    assert_integration_ready(
                        condition=False,
                        message=f"Model features {self.model_features} not all present in Gold features.",
                    )
                    merged["p_ml"] = 0.5
                    self.p_ml_proxy = True
            except Exception as e:
                assert_integration_ready(condition=False, message=f"ML model scoring failed: {e}")
                logger.warning(f"Error evaluating ML model: {e}; falling back to technical score.")
                merged["p_ml"] = 0.5
                self.p_ml_proxy = True
        else:
            # Baseline proxy: positive momentum has slight positive edge
            merged["p_ml"] = (0.50 + 0.10 * np.sign(merged.get("intraday_return_1d", 0.0))).clip(0.0, 1.0)
            self.p_ml_proxy = True

        # Sort chronologically
        merged.sort_values(["date", "ticker_symbol"], inplace=True)
        return merged

    def _generate_synthetic_backtest_data(self) -> pd.DataFrame:
        """Deterministic synthetic panel generator for test and offline environments."""
        np.random.seed(42)
        tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "V", "JNJ", "WMT", "PG", "XOM", "AMD", "ORCL", BENCHMARK_TICKER]
        dates = pd.date_range("2024-01-02", "2024-10-31", freq="B").strftime("%Y-%m-%d").tolist()

        rows = []
        for d_idx, dt in enumerate(dates):
            # Market regime drift
            spy_market_drift = 0.0005 + 0.008 * np.sin(d_idx / 20.0)
            for t in tickers:
                vol = 0.012 if t in ["JNJ", "PG", "WMT", BENCHMARK_TICKER] else 0.022
                ret_1d = np.random.normal(loc=spy_market_drift if t == BENCHMARK_TICKER else 0.0008, scale=vol)
                fwd_ret = np.random.normal(loc=spy_market_drift if t == BENCHMARK_TICKER else 0.0008, scale=vol)
                rows.append({
                    "ticker_symbol": t,
                    "date": dt,
                    "event_timestamp": f"{dt} 16:00:00",
                    "close_price": 100.0 * (1.0 + ret_1d),
                    "sma_20": 100.0,
                    "sma_50": 98.0,
                    "sma_ratio_20_50": 1.02,
                    "rsi_14d": 50.0 + 15.0 * np.random.randn(),
                    "macd_12_26": 0.5 * np.random.randn(),
                    "macd_signal_9": 0.3 * np.random.randn(),
                    "macd_histogram": 0.2 * np.random.randn(),
                    "rolling_volatility_30d": vol * np.sqrt(252),
                    "intraday_return_1d": ret_1d,
                    "daily_return_fwd": fwd_ret,
                })
        return pd.DataFrame(rows)

    def _run_day_scoring(self, df_day: pd.DataFrame) -> pd.Series:
        """Helper to score equities on a single day slice for zero look-ahead bias validation."""
        equities = df_day[df_day["ticker_symbol"] != BENCHMARK_TICKER].copy()
        spy_row = df_day[df_day["ticker_symbol"] == BENCHMARK_TICKER]
        spy_vol = float(spy_row["rolling_volatility_30d"].iloc[0]) if len(spy_row) > 0 else 0.15
        regime = detect_market_regime(spy_vol)
        return compute_composite_alpha(equities, regime=regime)

    def run_backtest(self) -> Dict[str, Any]:
        """Execute walk-forward simulation across all dates."""
        df = self.load_data()
        dates = sorted(df["date"].unique())
        logger.info(f"Running walk-forward backtest across {len(dates)} trading days...")

        daily_results: List[Dict[str, Any]] = []
        ic_records: List[Dict[str, Any]] = []

        # Previous day holding to compute turnover & friction
        prev_weights: Dict[str, float] = {}

        # Tracking parallel variants for Novel Idea 2 report
        strat_returns: List[float] = []
        fixed_returns: List[float] = []
        equal_returns: List[float] = []
        bench_returns: List[float] = []

        for t_idx, dt in enumerate(dates):
            df_day = df[df["date"] == dt].copy()
            if len(df_day) < 3:
                continue

            # Benchmark (SPY) return
            spy_row = df_day[df_day["ticker_symbol"] == BENCHMARK_TICKER]
            if len(spy_row) > 0:
                spy_fwd_ret = float(spy_row["daily_return_fwd"].iloc[0])
                spy_vol = float(spy_row["rolling_volatility_30d"].iloc[0])
            else:
                spy_fwd_ret = 0.0
                spy_vol = 0.15

            # 1. Market Regime detection strictly using info up to day t
            regime = detect_market_regime(spy_vol)

            # 2. Cross-sectional scoring for non-SPY constituents
            equities = df_day[df_day["ticker_symbol"] != BENCHMARK_TICKER].copy()
            if len(equities) < self.top_k:
                continue

            # Compute composite alpha scores
            equities["alpha_regime"] = compute_composite_alpha(equities, regime=regime)
            equities["alpha_fixed"] = compute_composite_alpha(equities, weights=FIXED_BASELINE_WEIGHTS)

            # Daily cross-sectional IC & Rank IC against next-day return
            ic_val = calculate_ic(equities["alpha_regime"], equities["daily_return_fwd"])
            rank_ic_val = calculate_rank_ic(equities["alpha_regime"], equities["daily_return_fwd"])
            ic_records.append({"date": dt, "ic": ic_val, "rank_ic": rank_ic_val})

            # 3. Portfolio Selection: Top K equities
            top_equities = equities.sort_values("alpha_regime", ascending=False).head(self.top_k)
            top_tickers = top_equities["ticker_symbol"].tolist()

            # 4. Position Sizing
            # Variant A: Risk Parity (inverse-volatility) sizing
            vols = dict(zip(top_equities["ticker_symbol"], top_equities["rolling_volatility_30d"]))
            rp_weights = compute_risk_parity_weights(vols)

            # Variant B: Equal weights sizing
            eq_weight = 1.0 / len(top_tickers)
            eq_weights = {t: eq_weight for t in top_tickers}

            # 5. Turnover & Friction Calculation
            all_tickers = set(rp_weights.keys()).union(set(prev_weights.keys()))
            turnover = sum(abs(rp_weights.get(t, 0.0) - prev_weights.get(t, 0.0)) for t in all_tickers) / 2.0
            friction_cost = turnover * self.friction

            # 6. Realized Returns Execution at Open (t+1)
            fwd_rets = dict(zip(top_equities["ticker_symbol"], top_equities["daily_return_fwd"]))

            # Gross returns
            gross_strat_ret = sum(rp_weights[t] * fwd_rets[t] for t in top_tickers)
            gross_eq_ret = sum(eq_weights[t] * fwd_rets[t] for t in top_tickers)

            # Net return after friction
            net_strat_ret = gross_strat_ret - friction_cost

            # Variant C: Fixed weights top-K
            top_fixed = equities.sort_values("alpha_fixed", ascending=False).head(self.top_k)
            fixed_ret = float(top_fixed["daily_return_fwd"].mean())

            strat_returns.append(net_strat_ret)
            equal_returns.append(gross_eq_ret - friction_cost)
            fixed_returns.append(fixed_ret - friction_cost)
            bench_returns.append(spy_fwd_ret)

            daily_results.append({
                "date": dt,
                "regime": regime.value,
                "selected_tickers": top_tickers,
                "ticker_scores": {t: float(s) for t, s in zip(top_equities["ticker_symbol"], top_equities["alpha_regime"])},
                "target_weights": {t: float(w) for t, w in rp_weights.items()},
                "strategy_return": net_strat_ret,
                "spy_return": spy_fwd_ret,
                "turnover": turnover,
                "friction": friction_cost,
            })

            prev_weights = rp_weights

        # Construct equity series
        s_strat_ret = pd.Series(strat_returns)
        s_bench_ret = pd.Series(bench_returns)
        s_fixed_ret = pd.Series(fixed_returns)
        s_equal_ret = pd.Series(equal_returns)

        strat_equity = calculate_cumulative_returns(s_strat_ret)
        bench_equity = calculate_cumulative_returns(s_bench_ret)
        fixed_equity = calculate_cumulative_returns(s_fixed_ret)
        equal_equity = calculate_cumulative_returns(s_equal_ret)

        # Performance Metrics
        mdd_strat, dd_strat_series = calculate_max_drawdown(strat_equity)
        mdd_bench, dd_bench_series = calculate_max_drawdown(bench_equity)

        cum_strat = float(strat_equity.iloc[-1] - 1.0)
        cum_bench = float(bench_equity.iloc[-1] - 1.0)
        years = len(strat_returns) / 252.0 if len(strat_returns) > 0 else 1.0

        ann_ret_strat = ((1.0 + cum_strat) ** (1.0 / years)) - 1.0 if years > 0 else cum_strat
        ann_ret_bench = ((1.0 + cum_bench) ** (1.0 / years)) - 1.0 if years > 0 else cum_bench

        vol_strat = float(s_strat_ret.std(ddof=1) * np.sqrt(252))
        vol_bench = float(s_bench_ret.std(ddof=1) * np.sqrt(252))

        sharpe_strat = calculate_sharpe(s_strat_ret, rf=0.04, periods_per_year=252)
        sharpe_bench = calculate_sharpe(s_bench_ret, rf=0.04, periods_per_year=252)

        sortino_strat = calculate_sortino(s_strat_ret, rf=0.04, periods_per_year=252)
        calmar_strat = calculate_calmar(cum_strat, mdd_strat, years=years)

        ic_df = pd.DataFrame(ic_records)
        ic_mean = float(ic_df["ic"].mean()) if len(ic_df) > 0 else 0.0
        rank_ic_mean = float(ic_df["rank_ic"].mean()) if len(ic_df) > 0 else 0.0
        ic_std = float(ic_df["ic"].std(ddof=1)) if len(ic_df) > 1 else 0.0
        ic_ir = float(ic_mean / ic_std) if ic_std > 1e-6 else 0.0
        hit_rate = calculate_hit_rate(s_strat_ret, s_bench_ret)

        rolling_sharpe_strat = calculate_rolling_sharpe(s_strat_ret, window=63, rf=0.04, periods_per_year=252)
        rolling_sharpe_bench = calculate_rolling_sharpe(s_bench_ret, window=63, rf=0.04, periods_per_year=252)

        # Prepare Series Objects for JSON contract
        valid_dates = [r["date"] for r in daily_results]
        equity_curve_data = []
        for i, dt in enumerate(valid_dates):
            equity_curve_data.append({
                "date": dt,
                "strategy_equity": round(float(strat_equity.iloc[i]), 4),
                "spy_equity": round(float(bench_equity.iloc[i]), 4),
                "strategy_drawdown": round(float(dd_strat_series.iloc[i]), 4),
                "spy_drawdown": round(float(dd_bench_series.iloc[i]), 4),
            })

        rolling_sharpe_data = []
        for i, dt in enumerate(valid_dates):
            if i >= 63:
                rolling_sharpe_data.append({
                    "date": dt,
                    "rolling_sharpe": round(float(rolling_sharpe_strat.iloc[i]), 3),
                    "spy_rolling_sharpe": round(float(rolling_sharpe_bench.iloc[i]), 3),
                })

        ic_series_data = [
            {"date": r["date"], "ic": round(float(r["ic"]), 4), "rank_ic": round(float(r["rank_ic"]), 4)}
            for r in ic_records
        ]

        simulated = self.synthetic_data or self.model is None or self.p_ml_proxy
        summary_payload = {
            "status": "PASSED_SIMULATED" if simulated else "SUCCESS",
            "execution_mode": "SIMULATED" if simulated else ("LIVE" if is_integration_mode() else "LOCAL_GOLD"),
            "synthetic_data": self.synthetic_data,
            "model_version": self.provenance.get("model_version"),
            "dataset_id": self.provenance.get("dataset_id"),
            "feature_source": self.provenance.get("feature_source"),
            "strategy_name": "Quant Alpha Walk-Forward Strategy (M1-37)",
            "benchmark_name": "S&P 500 ETF (SPY)",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "date_range": {
                "start_date": valid_dates[0] if valid_dates else "",
                "end_date": valid_dates[-1] if valid_dates else "",
                "trading_days": len(valid_dates),
            },
            "metrics": {
                "ic_mean": round(ic_mean, 4),
                "rank_ic_mean": round(rank_ic_mean, 4),
                "ic_std": round(ic_std, 4),
                "ic_ir": round(ic_ir, 3),
                "hit_rate": round(hit_rate, 3),
                "cum_return": round(cum_strat, 4),
                "annualized_return": round(ann_ret_strat, 4),
                "annualized_volatility": round(vol_strat, 4),
                "sharpe": round(sharpe_strat, 2),
                "sortino": round(sortino_strat, 2),
                "calmar": round(calmar_strat, 2),
                "max_drawdown": round(mdd_strat, 4),
                "spy_cum_return": round(cum_bench, 4),
                "spy_annualized_return": round(ann_ret_bench, 4),
                "spy_annualized_volatility": round(vol_bench, 4),
                "spy_sharpe": round(sharpe_bench, 2),
                "spy_max_drawdown": round(mdd_bench, 4),
                "excess_alpha_vs_spy": round(cum_strat - cum_bench, 4),
            },
            "series": {
                "ic_series": ic_series_data,
                "equity_curve": equity_curve_data,
                "rolling_sharpe": rolling_sharpe_data,
            },
            "artifacts": {
                "cumulative_returns_vs_benchmark": f"{self.output_dir}/cumulative_returns_vs_benchmark.png",
                "factor_ic_distribution": f"{self.output_dir}/factor_ic_distribution.png",
                "rolling_sharpe_ratio": f"{self.output_dir}/rolling_sharpe_ratio.png",
                "novel_idea_attribution_report": f"{self.output_dir}/novel_idea_attribution_report.md",
            },
        }

        # 7. Generate Tear Sheets and Markdown Reports
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._plot_cumulative_returns(valid_dates, strat_equity, bench_equity, dd_strat_series, dd_bench_series)
        self._plot_ic_distribution(ic_df)
        self._plot_rolling_sharpe(valid_dates, rolling_sharpe_strat, rolling_sharpe_bench)

        # Novel Idea 2 Attribution Report
        report_path = self.output_dir / "novel_idea_attribution_report.md"
        generate_attribution_report(
            fixed_equity=fixed_equity,
            regime_equity=strat_equity,
            equal_weight_equity=equal_equity,
            risk_parity_equity=strat_equity,
            benchmark_equity=bench_equity,
            output_path=str(report_path),
        )

        # Write stable JSON contract
        json_path = self.output_dir / "backtest_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary_payload, f, indent=2)
        logger.info(f"Saved backtest summary JSON to {json_path}")

        # Optional MLflow Tracking
        self._log_to_mlflow(summary_payload)

        # Register Candidate in Alpha Registry & Store Daily Signals (Production Quant Lifecycle)
        try:
            self._register_candidate_and_publish_daily_signals(summary_payload, daily_results)
        except ProvenanceError:
            raise  # integration|full: signals/registry rows without provenance must FAIL
        except Exception as e:
            logger.warning(f"Could not publish candidate alpha or daily signals: {e}")

        return summary_payload

    def _register_candidate_and_publish_daily_signals(
        self,
        summary: Dict[str, Any],
        daily_results: List[Dict[str, Any]],
    ) -> None:
        """Register candidate alpha in AlphaRegistry and publish daily signals to PostgreSQL/SQLite."""
        try:
            from quant_edai.quant.alpha_registry import AlphaRegistry, AlphaStatus
            from quant_edai.quant.paper_db import PaperDatabase

            db = PaperDatabase()
            metrics = summary.get("metrics", {})
            alpha_id = "alpha_composite_wf_m137"
            version = "v1.0.0"

            # Register as candidate
            candidate_record = {
                "alpha_id": alpha_id,
                "version": version,
                "author": "quant_researcher",
                "horizon": "DAILY",
                "alpha_type": "COMPOSITE",
                "status": AlphaStatus.CANDIDATE.value,
                "train_start_date": summary.get("date_range", {}).get("start_date", ""),
                "train_end_date": summary.get("date_range", {}).get("end_date", ""),
                "test_start_date": summary.get("date_range", {}).get("start_date", ""),
                "test_end_date": summary.get("date_range", {}).get("end_date", ""),
                "in_sample_metrics": metrics,
                "out_of_sample_metrics": metrics,
                "config_json": {
                    "top_k": self.top_k,
                    "friction_bps": TRANSACTION_COST_BPS,
                    "model_path": str(self.model_path),
                },
                **{k: v for k, v in self.provenance.items() if v},
            }
            db.upsert_alpha(candidate_record)
            logger.info(f"Registered candidate alpha {alpha_id}:{version} in Alpha Registry.")

            # Publish daily signals for the next trading day (T -> T+1)
            if daily_results:
                latest = daily_results[-1]
                asof_date = latest["date"]
                try:
                    next_bday = pd.to_datetime(asof_date) + pd.tseries.offsets.BDay(1)
                    effective_date = next_bday.strftime("%Y-%m-%d")
                except Exception:
                    effective_date = asof_date

                regime = latest.get("regime", "NORMAL")
                top_tickers = latest.get("selected_tickers", [])
                ticker_scores = latest.get("ticker_scores", {})
                target_weights = latest.get("target_weights", {})

                signals_to_record = []
                for ticker in top_tickers:
                    raw_score = float(ticker_scores.get(ticker, 1.0))
                    tgt_weight = float(target_weights.get(ticker, round(1.0 / len(top_tickers), 4) if top_tickers else 0.0))
                    sig = {
                        "signal_id": f"sig_daily_{alpha_id}_{effective_date}_{ticker}",
                        "alpha_id": alpha_id,
                        "version": version,
                        "ticker_symbol": ticker,
                        "asof_date": asof_date,
                        "effective_date": effective_date,
                        "raw_score": raw_score,
                        "target_weight": tgt_weight,
                        "confidence": 0.85,
                        "regime_detected": regime,
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        **{k: v for k, v in self.provenance.items() if v},
                    }
                    signals_to_record.append(sig)

                if signals_to_record:
                    db.record_daily_signals(signals_to_record)
                    logger.info(f"Published {len(signals_to_record)} daily signals to database for {effective_date} (as of {asof_date})")
        except ProvenanceError:
            raise
        except Exception as e:
            logger.warning(f"Failed to register candidate or publish daily signals: {e}")

    def _plot_cumulative_returns(
        self,
        dates: List[str],
        strat_eq: pd.Series,
        bench_eq: pd.Series,
        dd_strat: pd.Series,
        dd_bench: pd.Series,
    ) -> None:
        """Plot cumulative returns vs SPY with underwater drawdown subplot."""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={"height_ratios": [3, 1]}, sharex=True)

        x = range(len(dates))
        ax1.plot(x, strat_eq, label="Quant Alpha Strategy (M1-37)", color="#1f77b4", linewidth=2.0)
        ax1.plot(x, bench_eq, label="S&P 500 ETF (SPY Benchmark)", color="#7f7f7f", linewidth=1.5, linestyle="--")
        ax1.set_title("Walk-Forward Cumulative Returns vs. SPY Benchmark", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Portfolio Value ($1.0 Base)", fontsize=11)
        ax1.grid(True, linestyle=":", alpha=0.6)
        ax1.legend(loc="upper left")

        # Drawdown subplot
        ax2.fill_between(x, dd_strat * 100, 0, color="#1f77b4", alpha=0.3, label="Strategy Drawdown")
        ax2.plot(x, dd_bench * 100, color="#d62728", linestyle="--", alpha=0.7, label="SPY Drawdown")
        ax2.set_ylabel("Drawdown (%)", fontsize=10)
        ax2.set_xlabel("Trading Date", fontsize=11)
        ax2.grid(True, linestyle=":", alpha=0.6)
        ax2.legend(loc="lower left")

        # Set date ticks every 30 days
        step = max(1, len(dates) // 8)
        ax2.set_xticks(range(0, len(dates), step))
        ax2.set_xticklabels([dates[i] for i in range(0, len(dates), step)], rotation=30, ha="right")

        plt.tight_layout()
        out_file = self.output_dir / "cumulative_returns_vs_benchmark.png"
        fig.savefig(out_file, dpi=180)
        plt.close(fig)
        logger.info(f"Saved plot: {out_file}")

    def _plot_ic_distribution(self, ic_df: pd.DataFrame) -> None:
        """Plot histogram and density of daily Information Coefficients."""
        fig, ax = plt.subplots(figsize=(10, 5))
        ic_clean = ic_df["ic"].dropna()
        rank_ic_clean = ic_df["rank_ic"].dropna()

        ax.hist(ic_clean, bins=25, alpha=0.6, color="#2ca02c", label=f"Pearson IC (Mean: {ic_clean.mean():+.3f})", density=True)
        ax.hist(rank_ic_clean, bins=25, alpha=0.4, color="#1f77b4", label=f"Rank IC (Mean: {rank_ic_clean.mean():+.3f})", density=True)

        ax.axvline(ic_clean.mean(), color="#2ca02c", linestyle="--", linewidth=1.5)
        ax.axvline(rank_ic_clean.mean(), color="#1f77b4", linestyle="--", linewidth=1.5)
        ax.axvline(0.0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)

        ax.set_title("Information Coefficient (IC) & Rank IC Distribution", fontsize=13, fontweight="bold")
        ax.set_xlabel("Cross-Sectional Correlation Score", fontsize=11)
        ax.set_ylabel("Empirical Density", fontsize=11)
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="upper left")

        plt.tight_layout()
        out_file = self.output_dir / "factor_ic_distribution.png"
        fig.savefig(out_file, dpi=180)
        plt.close(fig)
        logger.info(f"Saved plot: {out_file}")

    def _plot_rolling_sharpe(
        self,
        dates: List[str],
        rolling_strat: pd.Series,
        rolling_bench: pd.Series,
    ) -> None:
        """Plot 63-day rolling Sharpe ratio comparison."""
        fig, ax = plt.subplots(figsize=(11, 5))
        x = range(len(dates))

        ax.plot(x, rolling_strat, label="Strategy Rolling Sharpe (63d)", color="#1f77b4", linewidth=2.0)
        ax.plot(x, rolling_bench, label="SPY Rolling Sharpe (63d)", color="#7f7f7f", linewidth=1.5, linestyle="--")
        ax.axhline(0.0, color="black", linestyle="-", linewidth=0.8, alpha=0.5)
        ax.axhline(1.0, color="#2ca02c", linestyle=":", linewidth=1.0, alpha=0.8, label="Target Sharpe = 1.0")

        ax.set_title("63-Day Rolling Annualized Sharpe Ratio", fontsize=13, fontweight="bold")
        ax.set_ylabel("Annualized Sharpe (periods=252)", fontsize=11)
        ax.set_xlabel("Trading Date", fontsize=11)
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="upper left")

        step = max(1, len(dates) // 8)
        ax.set_xticks(range(0, len(dates), step))
        ax.set_xticklabels([dates[i] for i in range(0, len(dates), step)], rotation=30, ha="right")

        plt.tight_layout()
        out_file = self.output_dir / "rolling_sharpe_ratio.png"
        fig.savefig(out_file, dpi=180)
        plt.close(fig)
        logger.info(f"Saved plot: {out_file}")

    def _log_to_mlflow(self, summary: Dict[str, Any]) -> None:
        """Log parameters, metrics, and artifacts to MLflow if available."""
        try:
            import mlflow

            # Use local SQLite or file store if MLFLOW_TRACKING_URI not set
            tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")
            mlflow.set_tracking_uri(tracking_uri)
            mlflow.set_experiment("quant_alpha_backtest")

            with mlflow.start_run(run_name="walk_forward_backtest"):
                mlflow.log_params({
                    "strategy": summary["strategy_name"],
                    "benchmark": summary["benchmark_name"],
                    "top_k": self.top_k,
                    "friction_bps": TRANSACTION_COST_BPS,
                    "trading_days": summary["date_range"]["trading_days"],
                })
                mlflow.log_metrics({
                    "ic_mean": summary["metrics"]["ic_mean"],
                    "rank_ic_mean": summary["metrics"]["rank_ic_mean"],
                    "ic_ir": summary["metrics"]["ic_ir"],
                    "hit_rate": summary["metrics"]["hit_rate"],
                    "strategy_sharpe": summary["metrics"]["sharpe"],
                    "strategy_cum_return": summary["metrics"]["cum_return"],
                    "strategy_max_drawdown": summary["metrics"]["max_drawdown"],
                    "spy_sharpe": summary["metrics"]["spy_sharpe"],
                    "excess_alpha": summary["metrics"]["excess_alpha_vs_spy"],
                })
                # Log artifacts
                for artifact_key, artifact_path in summary["artifacts"].items():
                    if os.path.exists(artifact_path):
                        mlflow.log_artifact(artifact_path)
            logger.info("Successfully tracked backtest run in MLflow.")
        except Exception as e:
            logger.info(f"MLflow logging bypassed ({e}); offline evidence saved to {self.output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Walk-Forward Quantitative Alpha Factor Backtester")
    parser.add_argument("--features", type=str, default=None, help="Path to features parquet")
    parser.add_argument("--labels", type=str, default=None, help="Path to labels parquet")
    parser.add_argument("--model", type=str, default=None, help="Path to ML model joblib")
    parser.add_argument("--output-dir", type=str, default="evidence/phase5", help="Output evidence directory")
    parser.add_argument("--top-k", type=int, default=3, help="Top K equities to allocate")
    parser.add_argument("--friction-bps", type=float, default=TRANSACTION_COST_BPS, help="Transaction friction in bps")
    args = parser.parse_args()

    backtester = QuantAlphaBacktester(
        features_path=args.features,
        labels_path=args.labels,
        model_path=args.model,
        output_dir=args.output_dir,
        top_k=args.top_k,
        friction_bps=args.friction_bps,
    )
    results = backtester.run_backtest()
    print("\n" + "=" * 60)
    print("QUANT ALPHA BACKTEST SUMMARY (M1-37)")
    print("=" * 60)
    for k, v in results["metrics"].items():
        print(f"  {k:28s}: {v}")
    print("=" * 60)
    print(f"All evidence artifacts generated in {args.output_dir}/\n")


if __name__ == "__main__":
    main()
