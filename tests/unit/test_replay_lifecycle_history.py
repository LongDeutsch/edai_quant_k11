"""Replay lifecycle history stays on SQLite and does not touch production Postgres."""

from types import SimpleNamespace

from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine


class _FixedSignal:
    def __init__(self, z_score: float):
        self.z_score = z_score

    def get_intraday_signal(self, **_kwargs):
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


def _run(db: PaperDatabase, path):
    signal = _FixedSignal(1.2)
    engine = PaperTradingEngine(
        session_id="sess_lifecycle_unit",
        db=db,
        alpha_source="store",
        signal_server=signal,
        z_entry_threshold=0.50,
        slippage_bps=5.0,
        commission_bps=1.0,
        tp_bps=20.0,
        sl_bps=12.0,
    )
    opened = engine.on_bar(_bar("2024-03-15T14:30:00Z", 100.0))
    signal.z_score = 1.2
    marked = engine.on_bar(_bar("2024-03-15T14:31:00Z", 100.10))
    signal.z_score = -1.2
    closed = engine.on_bar(_bar("2024-03-15T14:32:00Z", 100.10))
    signal.z_score = 0.2
    held = engine.on_bar(_bar("2024-03-15T14:33:00Z", 100.10))
    closed_session = engine.on_bar(_bar("2024-03-15T20:00:00Z", 100.10))
    path.append((opened, marked, closed, held, closed_session, list(engine.db.get_recent_trades("sess_lifecycle_unit"))))
    return engine


def test_open_mark_close_keeps_scores_and_unrealized_history(tmp_path):
    db = PaperDatabase(sqlite_path=str(tmp_path / "lifecycle.db"))
    db.init_db(reset=True)
    first = []
    engine = _run(db, first)
    opened, marked, closed, held, closed_session, trades = first[0]

    assert opened["signal_state"] == "BUY"
    assert marked["signal_state"] == "BUY"
    assert marked["positions_count"] >= 1
    assert closed["signal_state"] == "SELL"
    assert held["signal_state"] == "HOLD"
    assert held["alpha_scores"]
    assert closed_session["signal_state"] == "SESSION_CLOSED"
    assert closed_session["alpha_scores"] == []

    marks = db._sqlite_conn.execute(
        "SELECT unrealized_pnl FROM paper_position_marks WHERE session_id = ?",
        ("sess_lifecycle_unit",),
    ).fetchall()
    assert any(float(row[0]) != 0.0 for row in marks)

    lifecycle = db._sqlite_conn.execute(
        """
        SELECT status, unrealized_pnl, last_open_unrealized, exit_reason, realized_pnl
        FROM paper_position_lifecycle WHERE session_id = ?
        """,
        ("sess_lifecycle_unit",),
    ).fetchone()
    assert lifecycle[0] == "CLOSED"
    assert float(lifecycle[1]) == 0.0
    assert float(lifecycle[2]) != 0.0
    assert lifecycle[3] == "SIGNAL_EXIT"
    assert float(lifecycle[4]) != 0.0
    assert engine.positions == {}

    scores = db._sqlite_conn.execute(
        "SELECT COUNT(*) FROM paper_alpha_scores WHERE session_id = ?",
        ("sess_lifecycle_unit",),
    ).fetchone()[0]
    assert scores >= 1

    snaps = db._sqlite_conn.execute(
        """
        SELECT recorded_at, unrealized_pnl_current, active_positions_count
        FROM paper_portfolio_snapshots WHERE session_id = ? ORDER BY snapshot_id
        """,
        ("sess_lifecycle_unit",),
    ).fetchall()
    assert [row[0] for row in snaps] == [
        "2024-03-15T14:30:00Z",
        "2024-03-15T14:31:00Z",
        "2024-03-15T14:32:00Z",
        "2024-03-15T14:33:00Z",
        "2024-03-15T20:00:00Z",
    ]
    assert float(snaps[1][1]) != 0.0
    assert int(snaps[1][2]) >= 1
    assert float(snaps[2][1]) == 0.0
    assert int(snaps[2][2]) == 0
    assert any(float(row[1]) != 0.0 for row in snaps)
    assert float(snaps[-1][1]) == 0.0

    second_db = PaperDatabase(sqlite_path=str(tmp_path / "lifecycle_repeat.db"))
    second_db.init_db(reset=True)
    second = []
    _run(second_db, second)
    first_trades = [(row["direction"], row["fill_price"], row["realized_pnl"]) for row in trades]
    second_trades = [(row["direction"], row["fill_price"], row["realized_pnl"]) for row in second[0][5]]
    assert first_trades == second_trades


def test_step_queues_one_bar_only_while_paused():
    from quant_edai.serving.dashboard.dashboard_service import ReplayController, _GLOBAL_REPLAY_STATE

    assert ReplayController.step_replay() is False
    with _GLOBAL_REPLAY_STATE.lock:
        _GLOBAL_REPLAY_STATE.is_running = True
        _GLOBAL_REPLAY_STATE.is_paused = True
        _GLOBAL_REPLAY_STATE.pending_steps = 0
    try:
        assert ReplayController.step_replay() is True
        assert ReplayController.get_state()["pending_steps"] == 1
        ReplayController.resume_replay()
        assert ReplayController.get_state()["pending_steps"] == 0
        assert ReplayController.get_state()["is_paused"] is False
    finally:
        with _GLOBAL_REPLAY_STATE.lock:
            _GLOBAL_REPLAY_STATE.is_running = False
            _GLOBAL_REPLAY_STATE.is_paused = False
            _GLOBAL_REPLAY_STATE.pending_steps = 0
