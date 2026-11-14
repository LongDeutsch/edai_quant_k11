"""Page 3: Real-Time Trading Simulation.

The worker owns execution. This page only configures a session and reads the
metrics that worker publishes after each completed bar.
"""

from __future__ import annotations

from datetime import timedelta
import logging

import pandas as pd
import streamlit as st

from quant_edai.quant.alpha_split_evaluation import load_approved_replay_ensemble
from quant_edai.serving.dashboard.dashboard_service import (
    MarketDataService,
    ReplayController,
    build_replay_equity_figure,
    get_postgres_connection,
    single_alpha_block_reason,
)

logger = logging.getLogger("DashboardTrading")

st.set_page_config(
    page_title="Real-Time Trading | Quant Terminal",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Real-Time Trading Simulation")
st.caption("Paper replay through the existing engine. Speed and this page's refresh do not change fills.")


def _alpha_options() -> list[dict]:
    conn = get_postgres_connection()
    try:
        frame = pd.read_sql(
            """
            SELECT alpha_id, version, alpha_type, config_json
            FROM alpha_registry
            WHERE alpha_type = 'INTRADAY'
            ORDER BY alpha_id, version
            """,
            conn,
        )
    finally:
        conn.close()
    options = []
    for _, row in frame.iterrows():
        payload = row.to_dict()
        if single_alpha_block_reason(payload.get("alpha_id"), payload.get("version"), payload):
            continue
        options.append({
            "alpha_id": str(payload["alpha_id"]),
            "version": str(payload["version"]),
            "label": f"{payload['alpha_id']} @ {payload['version']}",
        })
    return options


st.subheader("Simulation configuration")
state = ReplayController.get_state()
config_left, config_right = st.columns(2)
with config_left:
    adv_mode = st.checkbox("Advanced dataset list", value=False, key="replay_advanced_datasets")
    ds_options = MarketDataService.get_catalog_options(advanced_mode=adv_mode)
    trading_default_idx = 2 if len(ds_options) >= 3 else 0
    selected_preset = st.selectbox(
        "Market dataset",
        ds_options,
        index=trading_default_idx,
        key="replay_dataset_preset",
    )
    selected_ds = MarketDataService.resolve_dataset_id(selected_preset)
    st.caption(f"Resolved dataset `{selected_ds}`, split TRADING.")
    alpha_mode = st.radio("Alpha mode", ["Approved ensemble", "Single alpha"], key="replay_alpha_mode")
    alpha_error = ""
    if alpha_mode == "Single alpha":
        try:
            choices = _alpha_options()
        except Exception as exc:
            choices = []
            alpha_error = str(exc)
        if alpha_error:
            st.error(f"Registry query failed: {alpha_error}")
        elif not choices:
            st.warning("No INTRADAY alpha with a strict strategy is registered.")
        else:
            st.session_state["replay_alpha_choices"] = choices
            st.selectbox(
                "Alpha id and version",
                [item["label"] for item in choices],
                key="replay_single_alpha_label",
            )
    else:
        try:
            ensemble_preview = load_approved_replay_ensemble(dataset_id=selected_ds)
        except Exception as exc:
            ensemble_preview = {"ok": False, "reason": str(exc), "alphas": []}
        if ensemble_preview.get("reason") and not ensemble_preview.get("ok"):
            st.error(ensemble_preview["reason"])
        elif ensemble_preview.get("alphas"):
            st.dataframe(
                pd.DataFrame(ensemble_preview["alphas"])[["alpha_id", "version", "weight"]],
                use_container_width=True,
                hide_index=True,
            )
with config_right:
    try:
        trading_tickers = MarketDataService.fetch_trading_tickers(selected_ds)
    except Exception as exc:
        trading_tickers = []
        st.error(f"TRADING ticker query failed: {exc}")
    if not trading_tickers:
        st.warning("No TRADING tickers were returned. Replay will not start with a guessed ticker.")
    selected_tickers = st.multiselect(
        "Tickers",
        trading_tickers,
        key="replay_tickers",
        placeholder="Select TRADING tickers",
    )
    timeframe = st.selectbox("Timeframe", ["1m", "5m", "10m", "15m"], key="replay_timeframe")
    end_policy = st.selectbox(
        "End of replay",
        ["MARK_TO_MARKET", "FORCE_CLOSE"],
        key="replay_end_policy",
        help="MARK_TO_MARKET leaves positions open. FORCE_CLOSE flattens them at the last mark.",
    )
    speed = st.selectbox("Replay speed", [1, 5, 10, 25, 50], index=2, key="replay_speed")
    session_id = st.text_input("Session ID", key="replay_session_id", placeholder="sess_new_01")
    st.caption("A session that already has trades cannot be started again. Use a new session id.")


@st.fragment(run_every=timedelta(seconds=1))
def _live_replay_panel() -> None:
    """Refresh KPIs, charts, and controls without rerunning the configuration form."""
    live = ReplayController.get_state()
    if live.get("is_running"):
        ReplayController.set_speed(int(st.session_state.get("replay_speed") or 10))

    st.subheader("Replay controls")
    if live.get("is_running") and not live.get("is_paused"):
        status = "RUNNING"
    elif live.get("is_paused"):
        status = "PAUSED"
    elif live.get("last_error"):
        status = "FAILED"
    elif live.get("completed"):
        status = "COMPLETED"
    else:
        status = "IDLE"
    progress = (live["processed_bars"] / max(1, live["total_bars"])) if live.get("total_bars") else 0.0
    st.progress(
        min(1.0, progress),
        text=(
            f"{status} | {live.get('processed_bars', 0)}/{live.get('total_bars', 0)} bars"
            f" | {live.get('current_time') or 'no bar yet'}"
            f" | {live.get('timeframe') or st.session_state.get('replay_timeframe')} "
            f"{live.get('alpha_mode') or 'ensemble'}"
        ),
    )
    if live.get("last_error"):
        st.error(live["last_error"])

    start_col, pause_col, step_col, stop_col = st.columns(4)
    with start_col:
        if st.button("Start", type="primary", use_container_width=True, key="replay_start", disabled=bool(live.get("is_running"))):
            mode_name = "single" if st.session_state.get("replay_alpha_mode") == "Single alpha" else "ensemble"
            label = st.session_state.get("replay_single_alpha_label")
            choice = next(
                (
                    item for item in (st.session_state.get("replay_alpha_choices") or [])
                    if item.get("label") == label
                ),
                {},
            )
            ReplayController.start_replay(
                session_id=(st.session_state.get("replay_session_id") or "").strip(),
                dataset_id=MarketDataService.resolve_dataset_id(st.session_state.get("replay_dataset_preset")),
                speed=int(st.session_state.get("replay_speed") or 10),
                tickers=list(st.session_state.get("replay_tickers") or []),
                timeframe=st.session_state.get("replay_timeframe") or "1m",
                alpha_mode=mode_name,
                single_alpha_id=(choice or {}).get("alpha_id"),
                single_alpha_version=(choice or {}).get("version"),
                end_policy=st.session_state.get("replay_end_policy") or "MARK_TO_MARKET",
            )
            st.rerun(scope="fragment")
    with pause_col:
        if live.get("is_running") and not live.get("is_paused"):
            if st.button("Pause", use_container_width=True, key="replay_pause"):
                ReplayController.pause_replay()
                st.rerun(scope="fragment")
        elif live.get("is_running") and live.get("is_paused"):
            if st.button("Resume", use_container_width=True, key="replay_resume"):
                ReplayController.resume_replay()
                st.rerun(scope="fragment")
        else:
            st.button("Pause", use_container_width=True, key="replay_pause_idle", disabled=True)
    with step_col:
        if st.button("Step", use_container_width=True, key="replay_step", disabled=not (live.get("is_running") and live.get("is_paused"))):
            ReplayController.step_replay()
            st.rerun(scope="fragment")
    with stop_col:
        if st.button("Stop", use_container_width=True, key="replay_stop", disabled=not live.get("is_running")):
            ReplayController.stop_replay()
            st.rerun(scope="fragment")

    st.subheader("Live portfolio")
    metrics = live.get("live_metrics") or {}
    if not metrics:
        st.caption("Source: no completed bar yet. The chart stays visible before the ledger has a snapshot.")
        cash = equity = realized = unrealized = 0.0
        drawdown = 0.0
        open_positions = 0
        as_of = "no completed bar yet"
        source = "not yet"
        metric_session = live.get("session_id") or st.session_state.get("replay_session_id") or ""
    else:
        cash = float(metrics.get("cash") or 0.0)
        equity = float(metrics.get("equity") or 0.0)
        realized = float(metrics.get("realized_pnl") or 0.0)
        unrealized = float(metrics.get("unrealized_pnl") or 0.0)
        drawdown = float(metrics.get("drawdown") or 0.0) * 100.0
        open_positions = int(metrics.get("open_positions") or 0)
        as_of = str(metrics.get("last_bar") or "")
        source = str(metrics.get("source") or "engine_memory")
        metric_session = str(metrics.get("session_id") or "")
        st.caption(f"As of `{as_of}` · source `{source}` · session `{metric_session}`")
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Cash", f"${cash:,.2f}")
    k2.metric("Equity", f"${equity:,.2f}")
    k3.metric("Realized PnL", f"${realized:+,.2f}")
    k4.metric("Unrealized PnL", f"${unrealized:+,.2f}")
    k5.metric("Drawdown", f"{drawdown:.2f}%")
    k6.metric("Open positions", open_positions)
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Signals evaluated", int(live.get("signals_scored") or 0))
    s2.metric("Orders submitted", int(live.get("orders") or 0))
    s3.metric("Fills", int(live.get("fills") or 0))
    s4.metric("Rejections", int(live.get("rejections") or 0))
    s5.metric("Last processed bar", as_of if metrics else "—")

    st.subheader("Real-time charts")
    st.plotly_chart(
        build_replay_equity_figure(live.get("equity_history") or []),
        use_container_width=True,
        key="replay_equity_chart",
    )

    st.subheader("Signals, positions, and fills")
    members = (live.get("ensemble") or {}).get("alphas") or []
    if members:
        st.caption("Approved weights are stored with the session. The signal combination remains an equal average.")
        st.dataframe(pd.DataFrame(members), use_container_width=True, hide_index=True)
    signal_rows = []
    for ticker in live.get("tickers") or []:
        data = (live.get("latest_signals") or {}).get(ticker) or {}
        signal_rows.append({
            "Ticker": ticker,
            "Action": data.get("action") or "WAITING",
            "Status": data.get("status") or "No bar received yet",
            "Z": data.get("z_combined"),
            "Position": data.get("position") or "",
            "Unrealized PnL": data.get("unrealized_pnl"),
            "Last event": data.get("last_bar_time") or "",
            "Score scope": data.get("score_scope") or "",
        })
    if signal_rows:
        st.dataframe(pd.DataFrame(signal_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("Select at least one TRADING ticker. Signals appear after the first processed bar.")
    score_rows = []
    for ticker, data in (live.get("latest_signals") or {}).items():
        for score in data.get("alpha_scores") or []:
            score_rows.append({
                "Ticker": ticker,
                "Alpha": score.get("alpha_id"),
                "Version": score.get("version"),
                "Z-Score": score.get("z_score"),
                "Action": score.get("action"),
                "As of": data.get("evaluated_at") or data.get("last_bar_time"),
                "Scope": data.get("score_scope") or "last evaluated",
            })
    if score_rows:
        st.caption("Last evaluated scores stay on screen when a later bar has no new signal.")
        st.dataframe(pd.DataFrame(score_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("Individual alpha scores appear after the first evaluated bar.")

    book = live.get("open_book") or []
    if book:
        st.dataframe(pd.DataFrame(book), use_container_width=True, hide_index=True)
    elif live.get("fills"):
        st.info("No open positions. The book is flat. Closed fills remain below.")
    else:
        st.info("No trades executed in this session yet.")
    fills = live.get("recent_fills") or []
    if fills:
        st.dataframe(pd.DataFrame(fills), use_container_width=True, hide_index=True)
    else:
        st.caption("Fills appear here after the engine submits an order.")


_live_replay_panel()

st.subheader("Ledger audit")
st.caption("This block reads PostgreSQL only when you ask it to. It is not the live KPI source.")
audit_session = (st.session_state.get("replay_session_id") or "").strip()
if st.button("Load ledger", key="replay_load_ledger", disabled=not audit_session):
    try:
        conn = get_postgres_connection()
        snapshot = pd.read_sql(
            """
            SELECT snapshot_id, recorded_at, cash_balance, total_portfolio_value,
                   realized_pnl_cumulative, unrealized_pnl_current, active_positions_count
            FROM paper_portfolio_snapshots
            WHERE session_id = %s
            ORDER BY snapshot_id DESC
            LIMIT 1
            """,
            conn,
            params=[audit_session],
        )
        trades = pd.read_sql(
            """
            SELECT trade_id, ticker_symbol, direction, fill_price, realized_pnl, executed_at
            FROM paper_trades
            WHERE session_id = %s
            ORDER BY executed_at DESC
            LIMIT 20
            """,
            conn,
            params=[audit_session],
        )
        lifecycle = pd.read_sql(
            """
            SELECT ticker_symbol, side, quantity, entry_time, entry_price, last_mark_time,
                   last_mark_price, last_open_unrealized, unrealized_pnl, exit_time,
                   exit_reason, realized_pnl, status
            FROM paper_position_lifecycle
            WHERE session_id = %s
            ORDER BY entry_time DESC
            LIMIT 50
            """,
            conn,
            params=[audit_session],
        )
        conn.close()
        st.session_state["replay_audit"] = {
            "session_id": audit_session,
            "snapshot": snapshot,
            "trades": trades,
            "lifecycle": lifecycle,
        }
    except Exception as exc:
        st.session_state["replay_audit"] = {"session_id": audit_session, "error": str(exc)}

audit = st.session_state.get("replay_audit") or {}
if audit.get("session_id") != audit_session:
    st.caption("Load the ledger to reconcile the final in-memory metrics with persisted rows.")
elif audit.get("error"):
    st.warning(audit["error"])
else:
    snapshot = audit.get("snapshot")
    if snapshot is None or snapshot.empty:
        st.caption("The ledger has no portfolio snapshot for this session yet.")
    else:
        row = snapshot.iloc[0]
        st.caption(
            f"Latest snapshot {row['snapshot_id']} at {row['recorded_at']}: "
            f"equity ${float(row['total_portfolio_value']):,.2f}, "
            f"realized ${float(row['realized_pnl_cumulative']):+,.2f}, "
            f"unrealized ${float(row['unrealized_pnl_current']):+,.2f}."
        )
        live_metrics = ReplayController.get_state().get("live_metrics") or {}
        if live_metrics.get("session_id") == audit_session:
            equity_gap = abs(float(live_metrics.get("equity") or 0.0) - float(row["total_portfolio_value"]))
            st.caption(f"In-memory equity differs from this snapshot by ${equity_gap:,.2f}.")
    trades = audit.get("trades")
    if trades is not None and not trades.empty:
        st.dataframe(trades, use_container_width=True, hide_index=True)
    else:
        st.caption("No persisted fills for this session.")
    lifecycle = audit.get("lifecycle")
    if lifecycle is not None and not lifecycle.empty:
        st.dataframe(lifecycle, use_container_width=True, hide_index=True)
    else:
        st.caption("No position lifecycle rows for this session.")
