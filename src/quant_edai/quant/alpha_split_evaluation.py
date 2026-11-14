"""Independent IS and OOS evaluation for every registered alpha.

The scorer, position rule, and cost model are the candle path already used by
Market Charts. This module only chooses the universe, runs that path on each
split, and stores the result. It does not invent trades or fill missing
metrics with zero.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from quant_edai.quant.metrics import (
    calculate_max_drawdown,
    calculate_sharpe,
    finite_float,
    periods_per_year,
)

logger = logging.getLogger(__name__)

EVAL_DATASET = "ds_canonical_4y_eval_s05"
RESEARCH_SPLITS = {
    "IS": ("2022-11-01", "2025-04-30"),
    "OOS": ("2025-05-01", "2026-04-30"),
}
BOOKSIZE = 100_000.0
SLIPPAGE_BPS = 5.0
COMMISSION_BPS = 1.0
COST_BPS = SLIPPAGE_BPS + COMMISSION_BPS
MIN_BARS = {"Daily": 60, "5m": 200}
NEAR_ZERO_IS_SHARPE = 0.1
ADMIN_APPROVERS = {"ADMIN", "ADMIN_PM", "ADMIN_QUANT"}
MEGA_CAPS = ("AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA")
IMPLEMENTED_STRATEGIES = {
    "momentum_z",
    "vwap_dev_z",
    "rsi_mr",
    "macd_hist_z",
    "sma_ratio",
    "vol_breakout",
    "volume_z",
    "ml_prob",
    "composite_eq",
    "regime_adapt",
}
ML_STRATEGIES = {"ml_prob", "composite_eq", "regime_adapt"}
_CHAMPION_COLUMNS = {
    "close_price": "close",
    "sma_20": "sma_20",
    "sma_50": "sma_50",
    "rsi_14d": "rsi_14",
    "macd_12_26": "macd_line",
    "macd_signal_9": "macd_signal",
    "macd_histogram": "macd_histogram",
    "rolling_volatility_30d": "bar_vol_ann",
    "intraday_return_1d": "bar_return",
}


def is_admin_approver(user: Optional[str]) -> bool:
    """Only the named admin accounts may freeze a Top-5 ensemble."""
    return str(user or "").strip().upper() in ADMIN_APPROVERS


def sharpe_degradation(
    is_sharpe: Optional[float],
    oos_sharpe: Optional[float],
) -> Dict[str, Any]:
    """Percentage decay only when both Sharpes exist and the IS Sharpe is positive.

    A missing, nonpositive, or near-zero IS Sharpe has no meaningful percentage.
    The absolute change is still returned when both numbers exist.
    """
    is_value = finite_float(is_sharpe)
    oos_value = finite_float(oos_sharpe)
    if is_value is None or oos_value is None:
        return {"degradation_pct": None, "sharpe_change": None, "comparable": False}
    change = oos_value - is_value
    if is_value <= NEAR_ZERO_IS_SHARPE:
        return {"degradation_pct": None, "sharpe_change": change, "comparable": False}
    return {
        "degradation_pct": (is_value - oos_value) / is_value * 100.0,
        "sharpe_change": change,
        "comparable": True,
    }


def timeframe_for_alpha(alpha_type: Optional[str]) -> str:
    """Intraday alphas use 5-minute bars. Daily and other research alphas use daily bars."""
    if str(alpha_type or "").upper() == "INTRADAY":
        return "5m"
    return "Daily"


def resolve_signal_rule(
    alpha_type: Optional[str],
    config: Optional[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (strategy, status, reason). A missing rule is not a flat backtest."""
    cfg = config or {}
    strategy = cfg.get("strategy")
    if strategy in IMPLEMENTED_STRATEGIES:
        if strategy == "ml_prob" and str(alpha_type or "").upper() == "INTRADAY":
            return (
                None,
                "INELIGIBLE",
                "No intraday probability model is registered. The daily champion model is not applied to 5-minute bars.",
            )
        return str(strategy), None, None
    if not strategy:
        return None, "INELIGIBLE", "No signal rule is configured for this alpha."
    return None, "INELIGIBLE", f"Signal rule {strategy} is not implemented on the candle path."


def eval_run_id(split_id: str, timeframe: str, version: str, alpha_id: str) -> str:
    """Stable id for one alpha version on one split. Reruns update this row only."""
    tf = "1d" if timeframe == "Daily" else timeframe
    version_token = re.sub(r"[^A-Za-z0-9]", "", version or "na")
    raw = f"ev_{split_id}_{tf}_{version_token}_{alpha_id}"
    if len(raw) <= 64:
        return raw
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"{raw[:55]}_{digest}"


def _json_obj(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _safe_token(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value or ""):
        raise ValueError(f"Unsafe {label}")
    return value


def _safe_day(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""):
        raise ValueError("Unsafe date")
    return value


def attach_bar_features(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Volatility and return of these bars. Daily feature rows are not copied in."""
    out = frame.copy()
    close = out["close"].astype(float)
    returns = close.pct_change()
    out["bar_return"] = returns
    out["bar_vol_ann"] = returns.rolling(30, min_periods=20).std() * np.sqrt(periods_per_year(timeframe))
    return out


def daily_champion_probabilities(frame: pd.DataFrame) -> Tuple[List[Optional[float]], Optional[str]]:
    """p_ml from the frozen champion on this daily bar's own indicators."""
    from quant_edai.serving.dashboard.dashboard_service import (
        CHAMPION_FEATURE_FALLBACK,
        _load_champion_model,
    )

    model, names, version = _load_champion_model()
    width = len(frame)
    if model is None or not hasattr(model, "predict_proba"):
        return [None] * width, version
    feature_names = list(names or CHAMPION_FEATURE_FALLBACK)
    work = frame.copy()
    if "sma_20" in work.columns and "sma_50" in work.columns:
        denom = work["sma_50"].replace(0, np.nan)
        work["sma_ratio_20_50"] = work["sma_20"] / denom
    columns = []
    for name in feature_names:
        if name == "sma_ratio_20_50" and "sma_ratio_20_50" in work.columns:
            columns.append(work["sma_ratio_20_50"])
            continue
        source = _CHAMPION_COLUMNS.get(name)
        if source is None or source not in work.columns:
            return [None] * width, version
        columns.append(work[source])
    matrix = pd.concat(columns, axis=1)
    matrix.columns = feature_names
    valid = matrix.notna().all(axis=1)
    probabilities: List[Optional[float]] = [None] * width
    if not bool(valid.any()):
        return probabilities, version
    try:
        predicted = model.predict_proba(matrix.loc[valid])[:, 1]
    except Exception as exc:
        logger.warning(f"Champion probability batch failed: {exc}")
        return [None] * width, version
    cursor = 0
    for index, ok in enumerate(valid.tolist()):
        if ok:
            probabilities[index] = float(predicted[cursor])
            cursor += 1
    return probabilities, version


def combine_name_books(
    books: Sequence[Dict[str, Any]],
    *,
    booksize: float,
    timeframe: str,
) -> Dict[str, Any]:
    """Equal-weight the names that have a bar at each timestamp."""
    series = []
    legs: List[float] = []
    signal_count = 0
    names = []
    for book in books:
        signal_count += int(book["signal_count"])
        legs.extend(float(pnl) for pnl in book["leg_pnls"])
        names.append({
            "ticker": book["ticker"],
            "bars": int(book["bars"]),
            "signal_count": int(book["signal_count"]),
            "trade_count": len(book["leg_pnls"]),
            "pnl": book["pnl"],
        })
        series.append(pd.Series(book["bar_returns"], index=pd.to_datetime(book["bar_time"], utc=True)))
    if not series:
        return {"status": "INELIGIBLE", "message": "No ticker had enough bars for this split."}
    portfolio = pd.concat(series, axis=1).mean(axis=1, skipna=True).dropna()
    minimum = MIN_BARS[timeframe]
    if len(portfolio) < minimum:
        return {
            "status": "INELIGIBLE",
            "message": f"Only {len(portfolio)} aligned bars. This timeframe needs at least {minimum}.",
        }
    sharpe = calculate_sharpe(
        portfolio.to_numpy(dtype=float),
        rf=0.04,
        periods_per_year=periods_per_year(timeframe),
    )
    equity = float(booksize)
    curve = []
    for ret in portfolio.to_numpy(dtype=float):
        equity *= 1.0 + float(ret)
        curve.append(equity)
    drawdown, _ = calculate_max_drawdown(curve)
    closed = len(legs)
    wins = sum(1 for pnl in legs if pnl > 0)
    return {
        "status": "COMPLETED",
        "message": None,
        "sharpe": sharpe.rounded(4),
        "sharpe_reason": sharpe.reason,
        "max_drawdown": None if not curve else float(drawdown),
        "total_return_pct": None if not curve else (curve[-1] / float(booksize) - 1.0) * 100.0,
        "net_pnl": None if not curve else float(curve[-1] - booksize),
        "final_equity": None if not curve else float(curve[-1]),
        "trade_count": closed,
        "win_rate": None if closed == 0 else wins / closed,
        "signal_count": signal_count,
        "return_sum": float(portfolio.sum()),
        "return_count": int(len(portfolio)),
        "names": names,
    }


def ensure_evaluation_schema(conn) -> None:
    """Allow missing metrics to stay NULL and record the evaluation status."""
    cur = conn.cursor()
    statements = [
        "ALTER TABLE alpha_backtest_results ADD COLUMN IF NOT EXISTS evaluation_status TEXT",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN total_trades DROP NOT NULL",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN win_rate DROP NOT NULL",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN max_drawdown DROP NOT NULL",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN total_return_pct DROP NOT NULL",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN net_pnl DROP NOT NULL",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN sharpe_ratio DROP NOT NULL",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN slippage_bps DROP NOT NULL",
        "ALTER TABLE alpha_backtest_results ALTER COLUMN commission_bps DROP NOT NULL",
    ]
    for statement in statements:
        cur.execute(statement)
    conn.commit()
    cur.close()


def _blank_metrics() -> Dict[str, Any]:
    return {
        "sharpe": None,
        "sharpe_reason": None,
        "max_drawdown": None,
        "total_return_pct": None,
        "net_pnl": None,
        "final_equity": None,
        "trade_count": None,
        "win_rate": None,
        "signal_count": None,
        "return_sum": None,
        "return_count": None,
        "names": [],
    }


def _registry_rows(conn) -> List[Dict[str, Any]]:
    frame = pd.read_sql(
        """
        SELECT alpha_id, version, alpha_type, status, config_json,
               metrics_snapshot, out_of_sample_metrics
        FROM alpha_registry
        ORDER BY alpha_id, version
        """,
        conn,
    )
    return frame.to_dict("records")


def _clickhouse_ready() -> bool:
    from quant_edai.serving.dashboard.dashboard_service import run_clickhouse_query

    probe = run_clickhouse_query("SELECT 1 AS ok", timeout=20)
    return not probe.empty and "ok" in probe.columns


def _daily_tickers(dataset_id: str, split_id: str, start: str, end: str) -> List[str]:
    from quant_edai.serving.dashboard.dashboard_service import run_clickhouse_query

    dataset_id = _safe_token(dataset_id, "dataset")
    split_id = _safe_token(split_id, "split")
    start, end = _safe_day(start), _safe_day(end)
    query = f"""
        SELECT ticker_symbol
        FROM quant_dw.market_daily_ohlcv FINAL
        WHERE dataset_id = '{dataset_id}'
          AND split_id = '{split_id}'
          AND trade_date >= '{start}'
          AND trade_date <= '{end}'
        GROUP BY ticker_symbol
        HAVING count() >= {MIN_BARS["Daily"]}
        ORDER BY ticker_symbol
    """
    frame = run_clickhouse_query(query, timeout=60)
    if frame.empty or "ticker_symbol" not in frame.columns:
        return []
    return [str(ticker) for ticker in frame["ticker_symbol"].tolist()]


def _load_frame(cache: Dict[Tuple[str, str, str], pd.DataFrame], ticker: str, timeframe: str, split_id: str, dataset_id: str) -> pd.DataFrame:
    key = (ticker, timeframe, split_id)
    if key in cache:
        return cache[key]
    from quant_edai.serving.dashboard.dashboard_service import MarketDataService

    start, end = RESEARCH_SPLITS[split_id]
    candles = MarketDataService.fetch_candlesticks(
        dataset_id,
        ticker,
        timeframe,
        start_date=start,
        end_date=end,
        split_id=split_id,
    )
    if candles is None or candles.empty or len(candles) < MIN_BARS[timeframe]:
        cache[key] = pd.DataFrame()
        return cache[key]
    prepared = attach_bar_features(MarketDataService.calculate_indicators(candles), timeframe)
    cache[key] = prepared
    return prepared


def _score_name(
    frame: pd.DataFrame,
    *,
    strategy: str,
    config: Dict[str, Any],
    ticker: str,
    timeframe: str,
    probabilities: Optional[List[Optional[float]]],
    notion: float,
) -> Dict[str, Any]:
    from quant_edai.serving.dashboard.dashboard_service import MarketDataService

    entry = float(config.get("entry_z", 1.0))
    exit_z = float(config.get("exit_z", 0.25))
    scored = MarketDataService.score_candle_frame(
        frame,
        strategy,
        config,
        entry_z=entry,
        exit_z=exit_z,
        ticker=ticker,
        p_ml=probabilities,
        allow_asof=False,
    )
    book = MarketDataService.simulate_scored_book(
        scored,
        entry_z=entry,
        exit_z=exit_z,
        booksize=notion,
        timeframe=timeframe,
        cost_bps=COST_BPS,
    )
    z_scores = scored["z_score"].astype(float) if "z_score" in scored.columns else pd.Series(dtype=float)
    return {
        "ticker": ticker,
        "bars": int(len(scored)),
        "signal_count": int((z_scores.abs() >= entry).sum()) if len(z_scores) else 0,
        "bar_returns": list(book.get("bar_returns") or []),
        "bar_time": scored["bar_time"].tolist() if "bar_time" in scored.columns else [],
        "leg_pnls": list(book.get("leg_pnls") or []),
        "pnl": book.get("pnl"),
    }


def _result_row(
    alpha: Dict[str, Any],
    split_id: str,
    timeframe: str,
    status: str,
    message: Optional[str],
    metrics: Dict[str, Any],
    *,
    dataset_id: str,
    model_version: Optional[str],
    ml_component: str,
    strategy: Optional[str],
    tickers: Sequence[str],
    prior_registry: Any,
) -> Dict[str, Any]:
    start, end = RESEARCH_SPLITS[split_id]
    version = str(alpha["version"])
    alpha_id = str(alpha["alpha_id"])
    config = _json_obj(alpha.get("config_json"))
    payload = {
        "dataset_id": dataset_id,
        "alpha_id": alpha_id,
        "alpha_version": version,
        "split_id": split_id,
        "timeframe": timeframe,
        "evaluation_status": status,
        "message": message,
        "strategy": strategy,
        "parameters": {
            "entry_z": config.get("entry_z", 1.0),
            "exit_z": config.get("exit_z", 0.25),
            "booksize": BOOKSIZE,
            "weighting": "equal_weight_mean_of_names_present",
            "fill": "next_bar",
            "cash_interest": 0,
        },
        "model_version": model_version,
        "ml_component": ml_component,
        "execution": {
            "slippage_bps": SLIPPAGE_BPS,
            "commission_bps": COMMISSION_BPS,
            "cost_bps": COST_BPS,
        },
        "tickers": list(tickers),
        "signal_count": metrics.get("signal_count"),
        "trade_count": metrics.get("trade_count"),
        "sharpe_reason": metrics.get("sharpe_reason"),
        "return_sum": metrics.get("return_sum"),
        "return_count": metrics.get("return_count"),
        "final_equity": metrics.get("final_equity"),
        "names": metrics.get("names") or [],
        "prior_registry_metrics": prior_registry,
        "source": "canonical_candle_backtest",
    }
    return {
        "run_id": eval_run_id(split_id, timeframe, version, alpha_id),
        "alpha_id": alpha_id,
        "alpha_version": version,
        "dataset_id": dataset_id,
        "split_id": split_id,
        "timeframe": timeframe,
        "period_start": start,
        "period_end": end,
        "evaluation_status": status,
        "message": message,
        "sharpe": metrics.get("sharpe"),
        "max_drawdown": metrics.get("max_drawdown"),
        "total_return_pct": metrics.get("total_return_pct"),
        "net_pnl": metrics.get("net_pnl"),
        "trade_count": metrics.get("trade_count"),
        "win_rate": metrics.get("win_rate"),
        "signal_count": metrics.get("signal_count"),
        "metrics_json": payload,
        "lifecycle_status": alpha.get("status"),
    }


def _persist_run(conn, row: Dict[str, Any]) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alpha_backtest_results (
            run_id, alpha_id, alpha_version, dataset_id, ticker_symbol, timeframe,
            period_start, period_end, total_trades, win_rate, sharpe_ratio, sortino_ratio,
            max_drawdown, total_return_pct, net_pnl, slippage_bps, commission_bps,
            metrics_json, created_at, split_id, evaluation_status
        ) VALUES (
            %s, %s, %s, %s, 'ALL', %s,
            %s, %s, %s, %s, %s, NULL,
            %s, %s, %s, %s, %s,
            %s::jsonb, %s, %s, %s
        )
        ON CONFLICT (run_id) DO UPDATE SET
            total_trades = EXCLUDED.total_trades,
            win_rate = EXCLUDED.win_rate,
            sharpe_ratio = EXCLUDED.sharpe_ratio,
            max_drawdown = EXCLUDED.max_drawdown,
            total_return_pct = EXCLUDED.total_return_pct,
            net_pnl = EXCLUDED.net_pnl,
            slippage_bps = EXCLUDED.slippage_bps,
            commission_bps = EXCLUDED.commission_bps,
            metrics_json = EXCLUDED.metrics_json,
            split_id = EXCLUDED.split_id,
            evaluation_status = EXCLUDED.evaluation_status,
            timeframe = EXCLUDED.timeframe,
            period_start = EXCLUDED.period_start,
            period_end = EXCLUDED.period_end
        """,
        (
            row["run_id"],
            row["alpha_id"],
            row["alpha_version"],
            row["dataset_id"],
            row["timeframe"],
            row["period_start"],
            row["period_end"],
            row["trade_count"],
            row["win_rate"],
            row["sharpe"],
            row["max_drawdown"],
            row["total_return_pct"],
            row["net_pnl"],
            SLIPPAGE_BPS,
            COMMISSION_BPS,
            json.dumps(row["metrics_json"], default=str),
            datetime.now(timezone.utc),
            row["split_id"],
            row["evaluation_status"],
        ),
    )
    column = "metrics_snapshot" if row["split_id"] == "IS" else "out_of_sample_metrics"
    snapshot = {
        "sharpe": row["sharpe"],
        "max_drawdown": row["max_drawdown"],
        "win_rate": row["win_rate"],
        "trade_count": row["trade_count"],
        "net_pnl": row["net_pnl"],
        "total_return_pct": row["total_return_pct"],
        "evaluation_status": row["evaluation_status"],
        "run_id": row["run_id"],
        "sharpe_reason": row["metrics_json"].get("sharpe_reason"),
        "signal_count": row["signal_count"],
        "seeded": False,
        "source": "canonical_candle_backtest",
        "dataset_id": row["dataset_id"],
        "split_id": row["split_id"],
        "timeframe": row["timeframe"],
        "model_version": row["metrics_json"].get("model_version"),
    }
    cur.execute(
        f"UPDATE alpha_registry SET {column} = %s WHERE alpha_id = %s AND version = %s",
        (json.dumps(snapshot, default=str), row["alpha_id"], row["alpha_version"]),
    )
    conn.commit()
    cur.close()


def evaluate_registered_alphas(
    dataset_id: str = EVAL_DATASET,
    splits: Iterable[str] = ("IS", "OOS"),
    alpha_ids: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Run the candle backtest for each registered alpha on the requested splits.

    TRADING is never accepted. Existing run ids that do not belong to this
    evaluation are left as they are. Repeating a run updates its own row.
    """
    from quant_edai.serving.dashboard.dashboard_service import get_postgres_connection

    requested = [split for split in splits if split in RESEARCH_SPLITS]
    if not requested:
        raise ValueError("Only IS and OOS can be evaluated. TRADING is excluded.")
    conn = get_postgres_connection()
    ensure_evaluation_schema(conn)
    alphas = _registry_rows(conn)
    if alpha_ids is not None:
        wanted = set(alpha_ids)
        alphas = [row for row in alphas if row["alpha_id"] in wanted]
    clickhouse_ok = _clickhouse_ready()
    cache: Dict[Tuple[str, str, str], pd.DataFrame] = {}
    probability_cache: Dict[Tuple[str, str], Tuple[List[Optional[float]], Optional[str]]] = {}
    ticker_cache: Dict[Tuple[str, str], List[str]] = {}
    results: List[Dict[str, Any]] = []

    for alpha in alphas:
        alpha_type = str(alpha.get("alpha_type") or "")
        timeframe = timeframe_for_alpha(alpha_type)
        config = _json_obj(alpha.get("config_json"))
        strategy, blocked_status, blocked_reason = resolve_signal_rule(alpha_type, config)
        for split_id in requested:
            prior = alpha.get("metrics_snapshot") if split_id == "IS" else alpha.get("out_of_sample_metrics")
            try:
                if not clickhouse_ok:
                    row = _result_row(
                        alpha, split_id, timeframe, "FAILED",
                        "ClickHouse did not answer the market-data probe.",
                        _blank_metrics(), dataset_id=dataset_id, model_version=None,
                        ml_component="not_used", strategy=strategy, tickers=[], prior_registry=prior,
                    )
                elif blocked_status:
                    row = _result_row(
                        alpha, split_id, timeframe, blocked_status, blocked_reason,
                        _blank_metrics(), dataset_id=dataset_id, model_version=None,
                        ml_component="not_used", strategy=None, tickers=[], prior_registry=prior,
                    )
                else:
                    cache_key = (timeframe, split_id)
                    if timeframe == "Daily":
                        if cache_key not in ticker_cache:
                            start, end = RESEARCH_SPLITS[split_id]
                            ticker_cache[cache_key] = _daily_tickers(dataset_id, split_id, start, end)
                        tickers = ticker_cache[cache_key]
                    else:
                        tickers = []
                        for ticker in MEGA_CAPS:
                            frame = _load_frame(cache, ticker, timeframe, split_id, dataset_id)
                            if not frame.empty:
                                tickers.append(ticker)
                    if not tickers:
                        row = _result_row(
                            alpha, split_id, timeframe, "INELIGIBLE",
                            "No ticker in this split has enough bars.",
                            _blank_metrics(), dataset_id=dataset_id, model_version=None,
                            ml_component="daily_champion" if uses_ml else "not_used",
                            strategy=strategy, tickers=[], prior_registry=prior,
                        )
                    else:
                        loaded = []
                        for ticker in tickers:
                            frame = _load_frame(cache, ticker, timeframe, split_id, dataset_id)
                            if not frame.empty:
                                loaded.append((ticker, frame))
                        notion = BOOKSIZE / len(loaded) if loaded else BOOKSIZE
                        books = []
                        model_version = None
                        ml_component = "not_used"
                        for ticker, frame in loaded:
                            probabilities = None
                            if strategy in ML_STRATEGIES and timeframe == "Daily":
                                prob_key = (ticker, split_id)
                                if prob_key not in probability_cache:
                                    probability_cache[prob_key] = daily_champion_probabilities(frame)
                                probabilities, model_version = probability_cache[prob_key]
                                ml_component = "daily_champion"
                            elif strategy in ML_STRATEGIES:
                                ml_component = "omitted_no_intraday_model"
                            books.append(_score_name(
                                frame,
                                strategy=strategy,
                                config=config,
                                ticker=ticker,
                                timeframe=timeframe,
                                probabilities=probabilities,
                                notion=notion,
                            ))
                        combined = combine_name_books(books, booksize=BOOKSIZE, timeframe=timeframe)
                        status = combined["status"]
                        metrics = combined if status == "COMPLETED" else _blank_metrics()
                        row = _result_row(
                            alpha, split_id, timeframe, status, combined.get("message"),
                            metrics, dataset_id=dataset_id, model_version=model_version,
                            ml_component=ml_component if books else "not_used",
                            strategy=strategy, tickers=[book["ticker"] for book in books],
                            prior_registry=prior,
                        )
                _persist_run(conn, row)
            except Exception as exc:
                logger.exception("Evaluation failed for %s %s", alpha.get("alpha_id"), split_id)
                conn.rollback()
                row = _result_row(
                    alpha, split_id, timeframe, "FAILED", str(exc),
                    _blank_metrics(), dataset_id=dataset_id, model_version=None,
                    ml_component="not_used", strategy=strategy, tickers=[], prior_registry=prior,
                )
                try:
                    _persist_run(conn, row)
                except Exception:
                    logger.exception("Could not persist the failed evaluation")
                    conn.rollback()
            logger.info(
                "%s %s %s trades=%s sharpe=%s",
                row["alpha_id"], split_id, row["evaluation_status"],
                row["trade_count"], row["sharpe"],
            )
            results.append(row)
    conn.close()
    return results


def load_validation_comparison(
    dataset_id: str = EVAL_DATASET,
    horizon: Optional[str] = None,
) -> pd.DataFrame:
    """Registry rows joined to the stored IS and OOS runs. Missing numbers stay null."""
    from quant_edai.serving.dashboard.dashboard_service import get_postgres_connection

    conn = get_postgres_connection()
    ensure_evaluation_schema(conn)
    query = """
        SELECT
            reg.alpha_id,
            reg.version,
            reg.alpha_type AS horizon,
            reg.status AS lifecycle_status,
            isr.evaluation_status AS is_status,
            isr.sharpe_ratio AS is_sharpe,
            isr.max_drawdown AS is_mdd,
            isr.win_rate AS is_win_rate,
            isr.total_trades AS is_trades,
            isr.net_pnl AS is_pnl,
            isr.run_id AS is_run_id,
            isr.timeframe AS timeframe,
            isr.metrics_json AS is_metrics,
            oos.evaluation_status AS oos_status,
            oos.sharpe_ratio AS oos_sharpe,
            oos.max_drawdown AS oos_mdd,
            oos.win_rate AS oos_win_rate,
            oos.total_trades AS oos_trades,
            oos.net_pnl AS oos_pnl,
            oos.run_id AS oos_run_id,
            oos.metrics_json AS oos_metrics
        FROM alpha_registry reg
        LEFT JOIN alpha_backtest_results isr
          ON isr.alpha_id = reg.alpha_id
         AND isr.alpha_version = reg.version
         AND isr.dataset_id = %s
         AND isr.split_id = 'IS'
         AND isr.run_id LIKE 'ev_%%'
        LEFT JOIN alpha_backtest_results oos
          ON oos.alpha_id = reg.alpha_id
         AND oos.alpha_version = reg.version
         AND oos.dataset_id = %s
         AND oos.split_id = 'OOS'
         AND oos.run_id LIKE 'ev_%%'
    """
    params: List[Any] = [dataset_id, dataset_id]
    if horizon:
        query += " WHERE reg.alpha_type = %s"
        params.append(horizon.upper())
    query += " ORDER BY reg.alpha_id, reg.version"
    frame = pd.read_sql(query, conn, params=params)
    conn.close()
    if frame.empty:
        return frame

    def _reason(payload: Any) -> Optional[str]:
        return _json_obj(payload).get("sharpe_reason")

    def _signals(payload: Any) -> Optional[int]:
        value = _json_obj(payload).get("signal_count")
        return None if value is None else int(value)

    rows = []
    for record in frame.to_dict("records"):
        is_sharpe = finite_float(record.get("is_sharpe"))
        oos_sharpe = finite_float(record.get("oos_sharpe"))
        compared = sharpe_degradation(is_sharpe, oos_sharpe)
        rows.append({
            **record,
            "is_status": record.get("is_status") or "NOT_EVALUATED",
            "oos_status": record.get("oos_status") or "NOT_EVALUATED",
            "is_sharpe": is_sharpe,
            "oos_sharpe": oos_sharpe,
            "is_sharpe_reason": _reason(record.get("is_metrics")),
            "oos_sharpe_reason": _reason(record.get("oos_metrics")),
            "is_signals": _signals(record.get("is_metrics")),
            "oos_signals": _signals(record.get("oos_metrics")),
            "degradation_pct": compared["degradation_pct"],
            "sharpe_change": compared["sharpe_change"],
            "comparable": compared["comparable"],
            "note": _json_obj(record.get("is_metrics")).get("message")
            or _json_obj(record.get("oos_metrics")).get("message"),
        })
    return pd.DataFrame(rows)


def approval_block_reason(
    alpha_ids: Sequence[str],
    horizon: str,
    approved_by: Optional[str],
    dataset_id: str = EVAL_DATASET,
    versions: Optional[Sequence[str]] = None,
) -> Optional[str]:
    """Why a Top-5 freeze must be refused. None means the selection may be saved."""
    if not is_admin_approver(approved_by):
        return "Approver is not an admin."
    if len(alpha_ids) != 5 or len(set(alpha_ids)) != 5:
        return "Select exactly five distinct alphas."
    if versions is not None and len(versions) != 5:
        return "Each selected alpha needs a version."
    from quant_edai.serving.dashboard.dashboard_service import get_postgres_connection

    conn = get_postgres_connection()
    ensure_evaluation_schema(conn)
    cur = conn.cursor()
    missing = []
    for index, alpha_id in enumerate(alpha_ids):
        version = versions[index] if versions is not None else None
        cur.execute(
            """
            SELECT 1
            FROM alpha_backtest_results isr
            JOIN alpha_backtest_results oos
              ON oos.alpha_id = isr.alpha_id
             AND oos.alpha_version = isr.alpha_version
             AND oos.dataset_id = isr.dataset_id
            JOIN alpha_registry reg
              ON reg.alpha_id = isr.alpha_id
             AND reg.version = isr.alpha_version
            WHERE isr.alpha_id = %s
              AND (%s IS NULL OR isr.alpha_version = %s)
              AND isr.dataset_id = %s
              AND isr.split_id = 'IS'
              AND oos.split_id = 'OOS'
              AND isr.run_id LIKE 'ev_%%'
              AND oos.run_id LIKE 'ev_%%'
              AND isr.evaluation_status = 'COMPLETED'
              AND oos.evaluation_status = 'COMPLETED'
              AND isr.sharpe_ratio IS NOT NULL
              AND oos.sharpe_ratio IS NOT NULL
              AND reg.alpha_type = %s
            LIMIT 1
            """,
            (alpha_id, version, version, dataset_id, horizon.upper()),
        )
        if cur.fetchone() is None:
            label = f"{alpha_id}@{version}" if version else alpha_id
            missing.append(label)
    cur.close()
    conn.close()
    if missing:
        return (
            "These alphas do not have a completed IS and OOS evaluation with a defined Sharpe: "
            + ", ".join(missing)
        )
    return None


POLICY_NAME = "oos_sharpe_then_drawdown_v1"
POLICY_DOCUMENT = (
    "An eligible version belongs to the requested horizon, exists in alpha_registry, and has "
    "COMPLETED In-Sample and Out-of-Sample evaluations on the selected canonical dataset. "
    "Both Sharpes are finite and both run ids start with ev_. "
    "Eligible versions are ranked by Out-of-Sample Sharpe descending, then Out-of-Sample max drawdown "
    "descending so a drawdown closer to zero wins a tie, then Out-of-Sample trade count descending, "
    "then In-Sample Sharpe descending, then alpha id and version ascending. "
    "The first five are the suggestion. An admin may replace members and set a positive weight for each "
    "member. The weights must sum to 1. Daily versions are never copied in to fill an Intraday ensemble."
)


def weight_block_reason(weights: Optional[Sequence[float]], count: int = 5) -> Optional[str]:
    """Why a weight vector cannot be frozen. None means the weights are usable."""
    values = list(weights or [])
    if len(values) != count:
        return "Enter one weight for each selected alpha."
    try:
        parsed = [float(item) for item in values]
    except (TypeError, ValueError):
        return "Weights must be numeric."
    if any(item <= 0 or item > 1 for item in parsed):
        return "Each weight must be greater than 0 and at most 1."
    if abs(sum(parsed) - 1.0) > 1e-4:
        return f"Weights sum to {sum(parsed):.4f}. They must sum to 1."
    return None


def candidate_exclusion_reason(row: Dict[str, Any], horizon: str) -> Optional[str]:
    """None when this registry version may be offered for the horizon ensemble."""
    actual = str(row.get("horizon") or "").upper()
    if actual != horizon.upper():
        return f"horizon is {actual or 'empty'}, required {horizon.upper()}"
    is_status = str(row.get("is_status") or "NOT_EVALUATED")
    oos_status = str(row.get("oos_status") or "NOT_EVALUATED")
    if is_status != "COMPLETED" or oos_status != "COMPLETED":
        note = row.get("note")
        reason = f"IS {is_status}, OOS {oos_status}"
        return f"{reason}: {note}" if note else reason
    if finite_float(row.get("is_sharpe")) is None or finite_float(row.get("oos_sharpe")) is None:
        return "IS or OOS Sharpe is undefined"
    if not str(row.get("is_run_id") or "").startswith("ev_"):
        return "IS run_id is missing"
    if not str(row.get("oos_run_id") or "").startswith("ev_"):
        return "OOS run_id is missing"
    return None


def rank_ensemble_candidates(frame: pd.DataFrame, horizon: str) -> pd.DataFrame:
    """Rank one horizon. Other horizons are left out rather than used as substitutes."""
    horizon = horizon.upper()
    columns = [
        "alpha_id", "version", "horizon", "eligible", "exclusion_reason", "rank",
        "is_status", "oos_status", "is_sharpe", "oos_sharpe", "is_mdd", "oos_mdd",
        "is_trades", "oos_trades", "is_run_id", "oos_run_id", "note", "policy_name",
    ]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    work = frame[frame["horizon"].astype(str).str.upper() == horizon].copy()
    if work.empty:
        return pd.DataFrame(columns=columns)
    records = work.to_dict("records")
    work["exclusion_reason"] = [candidate_exclusion_reason(row, horizon) for row in records]
    work["eligible"] = work["exclusion_reason"].isna()
    work["policy_name"] = POLICY_NAME
    eligible = work[work["eligible"]].copy()
    if eligible.empty:
        work["rank"] = pd.NA
    else:
        eligible = eligible.sort_values(
            by=["oos_sharpe", "oos_mdd", "oos_trades", "is_sharpe", "alpha_id", "version"],
            ascending=[False, False, False, False, True, True],
            na_position="last",
        )
        eligible["rank"] = list(range(1, len(eligible) + 1))
        work = work.drop(columns=["rank"], errors="ignore")
        work = work.merge(
            eligible[["alpha_id", "version", "rank"]],
            on=["alpha_id", "version"],
            how="left",
        )
    return work.sort_values(
        by=["eligible", "rank", "alpha_id"],
        ascending=[False, True, True],
        na_position="last",
    ).reset_index(drop=True)


def _ensure_approval_table(cur, is_postgres: bool) -> None:
    timestamp = "TIMESTAMP WITH TIME ZONE" if is_postgres else "TEXT"
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS alpha_ensemble_approval (
            approval_id TEXT PRIMARY KEY,
            ensemble_version INTEGER NOT NULL,
            horizon TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            selected_alpha_ids TEXT NOT NULL,
            alpha_versions TEXT NOT NULL,
            weights_json TEXT NOT NULL,
            approval_status TEXT NOT NULL,
            approved_by TEXT NOT NULL,
            approved_at {timestamp} NOT NULL,
            policy_name TEXT NOT NULL,
            policy_document TEXT NOT NULL,
            rank_snapshot TEXT NOT NULL,
            member_evidence TEXT NOT NULL,
            UNIQUE (horizon, ensemble_version)
        )
        """
    )


def persist_ensemble_approval(conn, payload: Dict[str, Any], *, is_postgres: bool) -> Dict[str, Any]:
    """Insert one frozen ensemble version. Existing approval rows are left unchanged."""
    if is_postgres and os.environ.get("PYTEST_CURRENT_TEST"):
        return {"ok": False, "reason": "Tests cannot write production ensemble approvals."}
    approver = str(payload.get("approved_by") or "")
    if is_postgres and approver.strip().lower() in {"test", "pytest"}:
        return {"ok": False, "reason": "A test writer cannot approve a production ensemble."}
    weight_reason = weight_block_reason(payload.get("weights"))
    if weight_reason:
        return {"ok": False, "reason": weight_reason}
    horizon = str(payload.get("horizon") or "").upper()
    alpha_ids = [str(item) for item in payload.get("alpha_ids") or []]
    versions = [str(item) for item in payload.get("versions") or []]
    if horizon not in {"INTRADAY", "DAILY"} or len(alpha_ids) != 5 or len(versions) != 5:
        return {"ok": False, "reason": "A frozen ensemble needs a horizon and five alpha versions."}
    weights = [float(item) for item in payload["weights"]]
    cur = conn.cursor()
    try:
        _ensure_approval_table(cur, is_postgres)
        if not is_postgres:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pm_governance_audit (
                    audit_id INTEGER PRIMARY KEY,
                    action_type TEXT NOT NULL,
                    alpha_id TEXT,
                    version TEXT,
                    executed_by TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    details TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
        placeholder = "%s" if is_postgres else "?"
        cur.execute(
            f"SELECT COALESCE(MAX(ensemble_version), 0) FROM alpha_ensemble_approval WHERE horizon = {placeholder}",
            (horizon,),
        )
        ensemble_version = int(cur.fetchone()[0]) + 1
        approval_id = f"ens_{horizon.lower()}_v{ensemble_version}"
        approved_at = datetime.now(timezone.utc).isoformat()
        evidence = json.dumps(payload.get("member_evidence") or [], default=str)
        snapshot = json.dumps(payload.get("rank_snapshot") or [], default=str)
        cur.execute(
            f"""
            INSERT INTO alpha_ensemble_approval (
                approval_id, ensemble_version, horizon, dataset_id, selected_alpha_ids,
                alpha_versions, weights_json, approval_status, approved_by, approved_at,
                policy_name, policy_document, rank_snapshot, member_evidence
            ) VALUES ({", ".join([placeholder] * 14)})
            """,
            (
                approval_id, ensemble_version, horizon, payload.get("dataset_id"),
                json.dumps(alpha_ids), json.dumps(versions), json.dumps(weights),
                "APPROVED_FROZEN", approver, approved_at, POLICY_NAME, POLICY_DOCUMENT,
                snapshot, evidence,
            ),
        )
        cur.execute(
            f"""
            INSERT INTO pm_governance_audit (
                action_type, alpha_id, version, executed_by, reason, details, created_at
            ) VALUES ({", ".join([placeholder] * 7)})
            """,
            (
                "ENSEMBLE_APPROVE_FREEZE",
                approval_id,
                str(ensemble_version),
                approver,
                POLICY_NAME,
                evidence,
                approved_at,
            ),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return {"ok": False, "reason": f"Ensemble approval was not stored: {exc}"}
    finally:
        cur.close()
    return {
        "ok": True,
        "reason": None,
        "approval_id": approval_id,
        "ensemble_version": ensemble_version,
        "approved_at": approved_at,
    }


def latest_frozen_ensemble(
    horizon: str = "INTRADAY",
    dataset_id: str = EVAL_DATASET,
) -> Optional[Dict[str, Any]]:
    """Return the newest immutable approval for this horizon and dataset."""
    from quant_edai.serving.dashboard.dashboard_service import get_postgres_connection

    conn = get_postgres_connection()
    cur = conn.cursor()
    try:
        _ensure_approval_table(cur, True)
        conn.commit()
        cur.execute(
            """
            SELECT approval_id, ensemble_version, horizon, dataset_id, selected_alpha_ids,
                   alpha_versions, weights_json, approval_status, approved_by, approved_at, policy_name
            FROM alpha_ensemble_approval
            WHERE horizon = %s
              AND dataset_id = %s
              AND approval_status = 'APPROVED_FROZEN'
              AND upper(approved_by) IN ('ADMIN', 'ADMIN_PM', 'ADMIN_QUANT')
            ORDER BY ensemble_version DESC
            LIMIT 1
            """,
            (horizon.upper(), dataset_id),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if row is None:
        return None
    return {
        "ensemble_id": row[0],
        "ensemble_version": int(row[1]),
        "horizon": row[2],
        "dataset_id": row[3],
        "selected_alpha_ids": _as_list(row[4]),
        "alpha_versions": [str(item) for item in _as_list(row[5])],
        "weights": [float(item) for item in _as_list(row[6])],
        "approval_status": row[7],
        "approved_by": row[8],
        "approved_at": row[9],
        "updated_by": row[8],
        "policy_name": row[10],
    }


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _stamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def assess_replay_ensemble(
    config: Optional[Dict[str, Any]],
    registry: Sequence[Dict[str, Any]],
    evaluations: Sequence[Dict[str, Any]],
    *,
    horizon: str = "INTRADAY",
    dataset_id: str = EVAL_DATASET,
) -> Dict[str, Any]:
    """Decide whether a stored ensemble may drive a replay.

    A failure stays a configuration error. It does not become a HOLD signal.
    """
    horizon = horizon.upper()
    ids = [str(item) for item in _as_list((config or {}).get("selected_alpha_ids"))]
    pinned_versions = [str(item) for item in _as_list((config or {}).get("alpha_versions"))]
    result: Dict[str, Any] = {
        "ok": False,
        "reason": None,
        "ensemble_id": (config or {}).get("ensemble_id") or "default",
        "horizon": horizon,
        "dataset_id": (config or {}).get("dataset_id") or dataset_id,
        "model_version": (config or {}).get("model_version"),
        "approved_by": (config or {}).get("approved_by"),
        "approved_at": _stamp((config or {}).get("approved_at")),
        "approval_status": (config or {}).get("approval_status"),
        "updated_by": (config or {}).get("updated_by"),
        "selected_alpha_ids": ids,
        "alpha_versions": pinned_versions,
        "ensemble_version": (config or {}).get("ensemble_version"),
        "policy_name": (config or {}).get("policy_name"),
        "alphas": [],
        "weights": [],
    }
    if not config:
        result["reason"] = (
            "No default INTRADAY ensemble is stored. Replay will not start."
        )
        return result

    problems: List[str] = []
    writer = str(config.get("updated_by") or "").strip().lower()
    if writer in {"test", "pytest"}:
        problems.append(
            f"updated_by={config.get('updated_by')} is a test writer, so this row is not a production approval"
        )
    if str(config.get("approval_status") or "") != "APPROVED_FROZEN":
        problems.append(
            f"approval_status is {config.get('approval_status') or 'empty'}; required APPROVED_FROZEN"
        )
    if not is_admin_approver(config.get("approved_by")):
        problems.append(f"approved_by={config.get('approved_by') or 'empty'} is not an admin")
    if len(ids) != 5 or len(set(ids)) != 5:
        problems.append("the ensemble must name exactly five distinct alphas")
    pinned: Dict[str, str] = {}
    if pinned_versions:
        if len(pinned_versions) != len(ids):
            problems.append("the frozen versions do not line up with the alpha ids")
        else:
            pinned = dict(zip(ids, pinned_versions))

    registry_by_id: Dict[str, List[Dict[str, Any]]] = {}
    for row in registry:
        registry_by_id.setdefault(str(row.get("alpha_id") or ""), []).append(row)

    completed: Dict[Tuple[str, str], Dict[str, float]] = {}
    for row in evaluations:
        if str(row.get("dataset_id") or dataset_id) != dataset_id:
            continue
        if not str(row.get("run_id") or "").startswith("ev_"):
            continue
        if str(row.get("evaluation_status") or "") != "COMPLETED":
            continue
        sharpe = finite_float(row.get("sharpe_ratio"))
        split_id = str(row.get("split_id") or "")
        if sharpe is None or split_id not in {"IS", "OOS"}:
            continue
        version = row.get("version", row.get("alpha_version"))
        completed.setdefault((str(row.get("alpha_id")), str(version)), {})[split_id] = float(sharpe)

    missing: List[str] = []
    alphas: List[Dict[str, Any]] = []
    for alpha_id in ids:
        versions = [
            row for row in registry_by_id.get(alpha_id, [])
            if str(row.get("alpha_type") or "").upper() == horizon
        ]
        if not versions:
            missing.append(alpha_id)
            continue
        if pinned:
            version = pinned[alpha_id]
            matched = [row for row in versions if str(row.get("version")) == version]
            splits = completed.get((alpha_id, version), {})
            if not matched or "IS" not in splits or "OOS" not in splits:
                problems.append(
                    f"{alpha_id}@{version} is not an evaluated {horizon} version on {dataset_id}"
                )
                continue
            chosen = [(version, splits)]
        else:
            chosen = []
            for row in versions:
                version = str(row.get("version"))
                splits = completed.get((alpha_id, version), {})
                if "IS" in splits and "OOS" in splits:
                    chosen.append((version, splits))
            if len(chosen) != 1:
                problems.append(
                    f"{alpha_id} has {len(chosen)} {horizon} versions with completed IS and OOS Sharpe"
                )
                continue
        version, splits = chosen[0]
        alphas.append({
            "alpha_id": alpha_id,
            "version": version,
            "alpha_type": horizon,
            "weight": None,
            "is_sharpe": splits["IS"],
            "oos_sharpe": splits["OOS"],
        })
    if missing:
        problems.append("REJECTED_ALPHA_NOT_IN_REGISTRY:" + ",".join(missing))

    raw_weights = _as_list(config.get("weights") if config.get("weights") is not None else config.get("weights_json"))
    if raw_weights and len(raw_weights) != len(ids):
        problems.append("weights do not match the alpha list")
    if not raw_weights and len(ids) == 5:
        raw_weights = [0.2, 0.2, 0.2, 0.2, 0.2]
    weights: List[float] = []
    try:
        weights = [float(item) for item in raw_weights]
    except (TypeError, ValueError):
        problems.append("weights are not numeric")
    if weights and abs(sum(weights) - 1.0) > 1e-6:
        problems.append("weights must sum to 1")
    if not problems and len(alphas) == len(ids):
        for alpha, weight in zip(alphas, weights):
            alpha["weight"] = weight

    result["alphas"] = alphas
    result["weights"] = weights
    if problems:
        reason = "Replay start refused. " + "; ".join(problems) + "."
        if writer in {"test", "pytest"} or missing:
            reason += (
                " The previous INTRADAY membership was overwritten in place and is not in pm_governance_audit."
                " The admin-approved ensemble on record is the DAILY set, which is not eligible for INTRADAY replay."
            )
        result["reason"] = reason
        return result
    result["ok"] = True
    return result


def load_approved_replay_ensemble(
    horizon: str = "INTRADAY",
    dataset_id: str = EVAL_DATASET,
) -> Dict[str, Any]:
    """Read the latest immutable approval and judge it against the registry and IS/OOS runs."""
    from quant_edai.serving.dashboard.dashboard_service import get_postgres_connection

    config = latest_frozen_ensemble(horizon=horizon, dataset_id=dataset_id)
    if config is None:
        return {
            "ok": False,
            "reason": (
                f"No immutable APPROVED_FROZEN {horizon.upper()} ensemble exists for dataset {dataset_id}. "
                "Replay will not start. Daily alphas are not substituted, and the revoked test fixture is ignored."
            ),
            "ensemble_id": None,
            "horizon": horizon.upper(),
            "dataset_id": dataset_id,
            "approved_by": None,
            "approved_at": None,
            "approval_status": None,
            "updated_by": None,
            "selected_alpha_ids": [],
            "alphas": [],
            "weights": [],
        }
    conn = get_postgres_connection()
    cur = conn.cursor()
    try:
        ids = _as_list((config or {}).get("selected_alpha_ids"))
        registry: List[Dict[str, Any]] = []
        evaluations: List[Dict[str, Any]] = []
        if ids:
            cur.execute(
                """
                SELECT alpha_id, version, alpha_type, status
                FROM alpha_registry
                WHERE alpha_id = ANY(%s)
                """,
                (ids,),
            )
            registry = [
                {"alpha_id": item[0], "version": item[1], "alpha_type": item[2], "status": item[3]}
                for item in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT alpha_id, alpha_version, split_id, evaluation_status, sharpe_ratio, dataset_id, run_id
                FROM alpha_backtest_results
                WHERE alpha_id = ANY(%s) AND dataset_id = %s AND run_id LIKE 'ev_%%'
                """,
                (ids, dataset_id),
            )
            evaluations = [
                {
                    "alpha_id": item[0],
                    "version": item[1],
                    "split_id": item[2],
                    "evaluation_status": item[3],
                    "sharpe_ratio": item[4],
                    "dataset_id": item[5],
                    "run_id": item[6],
                }
                for item in cur.fetchall()
            ]
    finally:
        cur.close()
        conn.close()
    return assess_replay_ensemble(
        config,
        registry,
        evaluations,
        horizon=horizon,
        dataset_id=dataset_id,
    )


def freeze_replay_session(
    session_id: str,
    dataset_id: str,
    ensemble: Dict[str, Any],
    split_id: str = "TRADING",
) -> Dict[str, Any]:
    """Store the approved ensemble on the session. A later start cannot swap it."""
    if not ensemble.get("ok"):
        return {"ok": False, "reason": ensemble.get("reason"), "ensemble": ensemble}
    payload = {
        "ensemble_id": ensemble.get("ensemble_id"),
        "horizon": ensemble.get("horizon"),
        "dataset_id": ensemble.get("dataset_id"),
        "model_version": ensemble.get("model_version"),
        "ensemble_version": ensemble.get("ensemble_version"),
        "policy_name": ensemble.get("policy_name"),
        "approved_by": ensemble.get("approved_by"),
        "approved_at": ensemble.get("approved_at"),
        "split_id": split_id,
        "alphas": [
            {
                "alpha_id": alpha["alpha_id"],
                "version": alpha["version"],
                "weight": alpha["weight"],
            }
            for alpha in ensemble.get("alphas") or []
        ],
    }
    body = json.dumps(payload, sort_keys=True, default=str)
    from quant_edai.serving.dashboard.dashboard_service import get_postgres_connection

    conn = get_postgres_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS replay_session_ensemble (
                session_id VARCHAR(64) PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                split_id TEXT NOT NULL,
                ensemble_json TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            INSERT INTO replay_session_ensemble (session_id, dataset_id, split_id, ensemble_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
            """,
            (session_id, dataset_id, split_id, body),
        )
        cur.execute(
            """
            SELECT ensemble_json, dataset_id, split_id
            FROM replay_session_ensemble
            WHERE session_id = %s
            """,
            (session_id,),
        )
        stored = cur.fetchone()
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if stored is None or stored[0] != body or stored[1] != dataset_id or stored[2] != split_id:
        return {
            "ok": False,
            "reason": "This session is already frozen to a different ensemble, dataset, or split.",
            "ensemble": ensemble,
        }
    frozen = dict(ensemble)
    frozen["frozen"] = True
    frozen["session_id"] = session_id
    frozen["split_id"] = split_id
    return {"ok": True, "reason": None, "ensemble": frozen}
