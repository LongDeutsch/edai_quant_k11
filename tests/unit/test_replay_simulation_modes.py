"""Replay mode, bar quality, and book reconciliation. SQLite only."""

from types import SimpleNamespace

import pandas as pd
import pytest

from quant_edai.quant.alpha_serving import AlphaSignalServer
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine
from quant_edai.serving.dashboard.dashboard_service import (
    ReplayController,
    _GLOBAL_REPLAY_STATE,
    aggregate_complete_ohlcv,
    build_replay_equity_figure,
    duplicate_bar_error,
    lookup_asof_features,
    session_restart_block_reason,
    single_alpha_block_reason,
    ticker_selection_reason,
    trading_replay_bars_aggregated_sql,
    trading_replay_bars_sql,
)


class _FixedSignal:
    def __init__(self, z_score: float):
        self.z_score = z_score
        self.strict_calls = []

    def get_intraday_signal(self, **kwargs):
        self.strict_calls.append(kwargs.get("strict"))
        action = "BUY" if self.z_score >= 0.5 else "SELL" if self.z_score <= -0.5 else "FLAT"
        return SimpleNamespace(
            action=action,
            z_score=self.z_score,
            notes="fixed-test-signal",
            alpha_scores=[{
                "alpha_id": "alpha_macd_z_intraday",
                "version": "v1.0.0",
                "z_score": self.z_score,
                "action": action,
            }],
        )


def _bar(ts: str, price: float) -> MarketBar:
    return MarketBar(
        ticker_symbol="AAPL",
        timestamp=ts,
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        volume=1000.0,
        vwap=price,
        p_ml=0.55,
    )


def _engine(db: PaperDatabase, signal: _FixedSignal, session_id: str = "sess_mode") -> PaperTradingEngine:
    engine = PaperTradingEngine(
        session_id=session_id,
        db=db,
        alpha_source="store",
        signal_server=signal,
        z_entry_threshold=0.50,
        slippage_bps=5.0,
        commission_bps=1.0,
        tp_bps=20.0,
        sl_bps=12.0,
    )
    engine.signal_alpha_ids = ["alpha_macd_z_intraday"]
    engine.signal_alpha_versions = {"alpha_macd_z_intraday": "v1.0.0"}
    engine.signal_strict = True
    engine.enforce_top_k_gate = True
    return engine


def test_single_alpha_uses_strict_scoring_and_one_id(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "strict.db"))
    db.init_db(reset=True)
    server = AlphaSignalServer(db=db)
    z_score, confidence = server._compute_single_intraday_alpha(
        ticker="AAPL",
        price=100.0,
        p_ml=None,
        config={"strategy": "momentum_z"},
        rolling_buffers={},
        features={},
        strict=True,
    )
    assert (z_score, confidence) == (0.0, 0.0)
    signal = _FixedSignal(1.2)
    engine = _engine(db, signal)
    engine.on_bar(_bar("2024-03-15T14:30:00Z", 100.0))
    assert engine.signal_strict is True
    assert engine.signal_alpha_ids == ["alpha_macd_z_intraday"]
    assert signal.strict_calls == [True]
    assert engine.EXECUTION_POLICY == "next_bar_risk_v1"
    assert engine.sl_bps == 12.0
    assert engine.tp_bps == 20.0


def test_repeat_run_matches_fills_without_reading_speed(tmp_path):
    def _trades(path):
        db = PaperDatabase(sqlite_path=str(path))
        db.init_db(reset=True)
        signal = _FixedSignal(1.2)
        engine = _engine(db, signal, session_id="sess_speed")
        engine.on_bar(_bar("2024-03-15T14:30:00Z", 100.0))
        signal.z_score = -1.2
        engine.on_bar(_bar("2024-03-15T14:31:00Z", 100.2))
        rows = engine.db.get_recent_trades("sess_speed")
        return [(row["direction"], row["fill_price"], row["realized_pnl"]) for row in rows]

    assert _trades(tmp_path / "a.db") == _trades(tmp_path / "b.db")
    assert not hasattr(PaperTradingEngine, "speed_multiplier")


def test_sparse_bucket_is_kept_and_full_bucket_keeps_ohlc():
    rows = []
    for minute, price in enumerate([10, 11, 9, 12, 13]):
        rows.append({
            "bar_time": f"2024-03-15T14:{minute:02d}:00Z",
            "ticker_symbol": "AAPL",
            "open": price,
            "high": price + 1,
            "low": price - 1,
            "close": price,
            "volume": 10,
            "vwap": price,
        })
    rows.append({
        "bar_time": "2024-03-15T14:05:00Z",
        "ticker_symbol": "AAPL",
        "open": 20, "high": 21, "low": 19, "close": 20, "volume": 10, "vwap": 20,
    })
    aggregated = aggregate_complete_ohlcv(pd.DataFrame(rows), 5)
    assert len(aggregated) == 2
    assert float(aggregated.iloc[0]["open"]) == 10
    assert float(aggregated.iloc[0]["high"]) == 14
    assert float(aggregated.iloc[0]["low"]) == 8
    assert float(aggregated.iloc[0]["close"]) == 13
    assert float(aggregated.iloc[1]["close"]) == 20
    sql = trading_replay_bars_aggregated_sql("ds", ["AAPL"], 5)
    assert "HAVING count()" not in sql
    assert "sum(volume) AS total_volume" in sql
    assert "sum(volume) AS volume" not in sql
    assert "GROUP BY bar_time, ticker_symbol" in sql
    assert "HAVING" not in trading_replay_bars_sql("ds", ["AAPL"])


def test_duplicate_bar_key_is_rejected():
    frame = pd.DataFrame([
        {"bar_time": "2024-03-15T14:00:00Z", "ticker_symbol": "AAPL"},
        {"bar_time": "2024-03-15T14:00:00Z", "ticker_symbol": "AAPL"},
    ])
    assert duplicate_bar_error(frame)
    assert duplicate_bar_error(frame.drop_duplicates()) is None


def test_asof_feature_does_not_use_same_day_or_future_rows():
    frame = pd.DataFrame([
        {"ticker_symbol": "AAPL", "event_timestamp": "2024-03-14T00:00:00Z", "rsi_14d": 40.0},
        {"ticker_symbol": "AAPL", "event_timestamp": "2024-03-15T00:00:00Z", "rsi_14d": 70.0},
        {"ticker_symbol": "AAPL", "event_timestamp": "2024-03-16T00:00:00Z", "rsi_14d": 80.0},
    ])
    from quant_edai.serving.dashboard.dashboard_service import index_asof_features

    indexed = index_asof_features(frame)
    payload = lookup_asof_features(indexed, "AAPL", "2024-03-15T14:30:00Z")
    assert payload["rsi_14d"] == 40.0


def test_book_reconciles_after_open_mark_and_close(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "book.db"))
    db.init_db(reset=True)
    signal = _FixedSignal(1.2)
    engine = _engine(db, signal, session_id="sess_book")
    engine.on_bar(_bar("2024-03-15T14:30:00Z", 100.0))
    opened = engine.book_metrics()
    assert opened["open_positions"] == 1
    signal.z_score = 1.2
    engine.on_bar(_bar("2024-03-15T14:31:00Z", 100.10))
    marked = engine.book_metrics()
    assert marked["open_positions"] == 1
    assert marked["unrealized_pnl"] != 0.0
    signal.z_score = -1.2
    engine.on_bar(_bar("2024-03-15T14:32:00Z", 100.10))
    flat = engine.book_metrics()
    assert engine.positions == {}
    assert flat["unrealized_pnl"] == 0.0
    assert flat["open_positions"] == 0
    assert flat["realized_pnl"] != 0.0
    assert flat["equity"] == pytest.approx(flat["cash"])


def test_stop_and_eod_keep_their_reasons(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "risk.db"))
    db.init_db(reset=True)
    signal = _FixedSignal(1.2)
    engine = _engine(db, signal, session_id="sess_risk")
    engine.on_bar(_bar("2024-03-15T14:30:00Z", 100.0))
    stopped = engine.on_bar(_bar("2024-03-15T14:31:00Z", 99.5))
    assert any(event.startswith("SL_") for event in stopped["events"])
    assert engine.positions == {}

    signal.z_score = 1.2
    engine.on_bar(_bar("2024-03-15T14:32:00Z", 100.0))
    assert engine.apply_end_policy("MARK_TO_MARKET", "2024-03-15T14:32:00Z") == []
    assert engine.positions
    closed = engine.apply_end_policy("FORCE_CLOSE", "2024-03-15T14:32:00Z")
    assert closed == ["AAPL"]
    assert engine.positions == {}
    reason = db._sqlite_conn.execute(
        "SELECT exit_reason FROM paper_position_lifecycle WHERE session_id = ? AND exit_reason = 'END_OF_REPLAY'",
        ("sess_risk",),
    ).fetchone()
    assert reason[0] == "END_OF_REPLAY"

    signal.z_score = 1.2
    engine.on_bar(_bar("2024-03-15T14:33:00Z", 100.0))
    eod = engine.on_bar(_bar("2024-03-15T20:00:00Z", 100.0))
    assert any(event.startswith("EOD_FLAT") for event in eod["events"])


def test_existing_session_and_ticker_selection_block_a_restart(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "occupied.db"))
    db.init_db(reset=True)
    assert db.session_has_activity("sess_old") is False
    db.record_snapshot({
        "session_id": "sess_old",
        "cash_balance": 100000.0,
        "invested_equity": 0.0,
        "total_portfolio_value": 100000.0,
        "realized_pnl_cumulative": 0.0,
        "unrealized_pnl_current": 0.0,
        "rolling_sharpe_ratio": 0.0,
        "max_drawdown_pct": 0.0,
        "active_positions_count": 0,
        "latency_ms": 0.0,
        "recorded_at": "2024-03-15T14:30:00Z",
    })
    assert db.session_has_activity("sess_old") is True
    assert "new session id" in session_restart_block_reason(True, "sess_old")
    assert ticker_selection_reason(["AAPL"], [])
    assert ticker_selection_reason([], ["AAPL"])
    assert ticker_selection_reason(["ZZZ"], ["AAPL"])
    assert ticker_selection_reason(["AAPL"], ["AAPL"]) is None
    assert single_alpha_block_reason("", "v1", None)
    assert single_alpha_block_reason("alpha_x", "v9", None)
    assert single_alpha_block_reason("alpha_x", "v1", {"alpha_type": "DAILY", "config_json": {"strategy": "momentum_z"}})
    assert single_alpha_block_reason("alpha_x", "v1", {"alpha_type": "INTRADAY", "config_json": {"strategy": "not_a_strategy"}})
    assert single_alpha_block_reason(
        "alpha_x", "v1", {"alpha_type": "INTRADAY", "config_json": {"strategy": "momentum_z"}},
    ) is None


def test_stale_worker_cannot_overwrite_live_metrics():
    with _GLOBAL_REPLAY_STATE.lock:
        previous_generation = _GLOBAL_REPLAY_STATE.generation
        previous_metrics = dict(_GLOBAL_REPLAY_STATE.live_metrics)
        _GLOBAL_REPLAY_STATE.generation = 4
        _GLOBAL_REPLAY_STATE.live_metrics = {"equity": 10.0, "session_id": "current"}
    try:
        assert ReplayController.publish_live_metrics(3, {"equity": 1.0, "session_id": "stale"}) is False
        assert ReplayController.get_state()["live_metrics"]["equity"] == 10.0
        assert ReplayController.publish_live_metrics(4, {"equity": 12.0, "session_id": "current"}) is True
        assert ReplayController.get_state()["live_metrics"]["equity"] == 12.0
    finally:
        with _GLOBAL_REPLAY_STATE.lock:
            _GLOBAL_REPLAY_STATE.generation = previous_generation
            _GLOBAL_REPLAY_STATE.live_metrics = previous_metrics


def test_postgres_requirement_does_not_create_sqlite(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("USE_SQLITE_ONLY", raising=False)
    with pytest.raises(ConnectionError):
        PaperDatabase(
            db_url="postgresql://quant:quant_password@127.0.0.1:1/quant_db",
            require_postgres=True,
        )
    assert list(tmp_path.glob("*.db")) == []


def test_empty_history_still_builds_a_chart():
    figure = build_replay_equity_figure([])
    assert len(figure.data) >= 1
    assert any("No completed bars yet" in (item.text or "") for item in figure.layout.annotations)
