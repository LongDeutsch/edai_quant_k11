"""Page 1: Market Charts & Technical Analytics Terminal.

Features:
- Multi-timeframe aggregated candlesticks: 5m, 10m, 15m (from ClickHouse market_intraday_bars FINAL) & Daily (from market_daily_ohlcv FINAL).
- Volume bar charts and timeframe-consistent indicators: SMA(20), SMA(50), EMA(12), EMA(26), VWAP, RSI(14), MACD(12, 26, 9).
- Semantic Trade Execution Markers (▲ BUY, ▼ SELL, ✖ Stop Loss, ◆ Take Profit).
- Version and dataset isolation with query caching.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
import streamlit as st

from quant_edai.quant.metrics import format_sharpe
from quant_edai.serving.dashboard.dashboard_service import AlphaResearchService, MarketDataService
from quant_edai.serving.dashboard.market_chart import build_chart_payload, render_market_chart

st.set_page_config(
    page_title="Market Charts | Quant Terminal",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📈 Market Charts & Technical Analytics")
st.caption("Multi-timeframe candlestick visualization, technical overlays, and trade execution markers powered by ClickHouse OLAP.")

# -----------------------------------------------------------------------------
# Sidebar Filters & Controls
# -----------------------------------------------------------------------------
st.sidebar.header("🕹️ Market Controls")

adv_mode = st.sidebar.checkbox("⚙️ Advanced Mode", value=False, help="Show raw ClickHouse test/idempotency datasets.")
ds_options = MarketDataService.get_catalog_options(advanced_mode=adv_mode)
selected_preset = st.sidebar.selectbox("Active Dataset Context", ds_options, index=0)
selected_dataset = MarketDataService.resolve_dataset_id(selected_preset)
selected_split = MarketDataService.resolve_split_id(selected_preset)

# Dynamic dataset capabilities discovery from ClickHouse
@st.cache_data(ttl=60)
def load_dataset_capabilities(dataset_id: str):
    return MarketDataService.discover_dataset_capabilities(dataset_id)

caps = load_dataset_capabilities(selected_dataset)
st.sidebar.caption(f"Resolved Dataset ID: `{selected_dataset}`" + (f" | Split: `{selected_split}`" if selected_split else ""))
st.sidebar.caption(f"📊 Discovered: **{caps['daily_count']:,}** Daily bars | **{caps['intraday_count']:,}** Intraday 1m bars")

tickers = caps["tickers"]
default_ticker = "AAPL" if "AAPL" in tickers else (tickers[0] if tickers else "AAPL")
selected_ticker = st.sidebar.selectbox("Ticker Symbol", tickers, index=tickers.index(default_ticker) if default_ticker in tickers else 0)

# Timeframe default comes from the preset catalog (Trading Replay is Daily).
supported_tfs = caps["supported_timeframes"]
default_tf = MarketDataService.resolve_default_timeframe(selected_preset, supported_tfs)
default_idx = supported_tfs.index(default_tf) if default_tf in supported_tfs else 0

selected_timeframe = st.sidebar.selectbox(
    "Candle Timeframe",
    supported_tfs,
    index=default_idx,
    help="Timeframes dynamically discovered from ClickHouse: 1m/5m/10m/15m require intraday streaming bars; Daily requires historical OHLCV.",
)

filter_by_date = st.sidebar.checkbox("📅 Filter by Date Window", value=False, help="Override the preset candle window.")
manual_start = None
manual_end = None
if filter_by_date:
    col_d1, col_d2 = st.sidebar.columns(2)
    with col_d1:
        s_date = st.date_input("From Date", value=None)
        manual_start = str(s_date) if s_date else None
    with col_d2:
        e_date = st.date_input("To Date", value=None)
        manual_end = str(e_date) if e_date else None

st.sidebar.markdown("---")
st.sidebar.subheader("📐 Technical Overlays & Execution")
show_sma = st.sidebar.checkbox("SMA (20 & 50)", value=False)
show_ema = st.sidebar.checkbox("EMA (12 & 26)", value=False)
show_vwap = st.sidebar.checkbox("Intraday VWAP", value=False)
show_volume = st.sidebar.checkbox("Volume Bar Subplot", value=True)
show_rsi = st.sidebar.checkbox("RSI (14)", value=False)
show_macd = st.sidebar.checkbox("MACD (12, 26, 9)", value=False)

st.sidebar.markdown("---")
st.sidebar.subheader("🎯 Execution Markers & Alpha Signals")
show_trades = st.sidebar.checkbox("Display Execution Trades (PostgreSQL)", value=True)
selected_session = "ALL"
if show_trades:
    trade_sessions = MarketDataService.get_trade_sessions(dataset_id=selected_dataset, ticker=selected_ticker)
    if trade_sessions:
        selected_session = st.sidebar.selectbox("Filter by Trading Session", ["ALL"] + trade_sessions, index=0)
    else:
        st.sidebar.caption("No trade sessions recorded for this dataset/ticker.")

registered = AlphaResearchService.get_registered_alphas()
alpha_ids = registered["alpha_id"].drop_duplicates().tolist() if not registered.empty and "alpha_id" in registered.columns else []
selected_alpha_label = st.sidebar.selectbox(
    "Alpha Signal",
    ["All fills"] + alpha_ids,
    help="All fills shows every paper_trades row. Choosing an alpha filters those fills. Xác nhận scores that alpha on the candles in view.",
)
selected_alpha_id = None if selected_alpha_label == "All fills" else selected_alpha_label

fill_bounds = MarketDataService.get_fill_time_bounds(
    dataset_id=selected_dataset,
    ticker=selected_ticker,
    session_id=selected_session if selected_session != "ALL" else None,
)
if manual_start or manual_end:
    start_date_str, end_date_str = manual_start, manual_end
else:
    start_date_str, end_date_str = MarketDataService.resolve_candle_window(
        selected_preset,
        selected_timeframe,
        None if fill_bounds.error else fill_bounds.min_time,
        None if fill_bounds.error else fill_bounds.max_time,
    )
if start_date_str or end_date_str:
    st.sidebar.caption(f"Candle window: `{start_date_str or '…'}` → `{end_date_str or '…'}`")

signal_request = {
    "alpha_id": selected_alpha_id,
    "ticker": selected_ticker,
    "timeframe": selected_timeframe,
    "dataset_id": selected_dataset,
    "start": start_date_str,
    "end": end_date_str,
    "split": selected_split,
}
if st.sidebar.button("Xác nhận", disabled=selected_alpha_id is None, help="Score the selected alpha on this ticker and timeframe, then show signals and book metrics."):
    st.session_state["chart_signal_run"] = signal_request
confirmed_run = st.session_state.get("chart_signal_run") == signal_request
if selected_alpha_id and not confirmed_run:
    st.sidebar.caption("Bấm Xác nhận để chấm alpha này trên các nến đang xem.")

# -----------------------------------------------------------------------------
# Data Retrieval & Processing
# -----------------------------------------------------------------------------
@st.cache_data(ttl=10)
def load_and_prepare_data(dataset_id: str, ticker: str, timeframe: str, limit: int, start_date: Optional[str] = None, end_date: Optional[str] = None, split_id: Optional[str] = None):
    raw_df = MarketDataService.fetch_candlesticks(
        dataset_id=dataset_id,
        ticker=ticker,
        timeframe=timeframe,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
        split_id=split_id,
    )
    if raw_df.empty:
        return pd.DataFrame()
    return MarketDataService.calculate_indicators(raw_df)

with st.spinner("Fetching candles from ClickHouse OLAP..."):
    df_candles = load_and_prepare_data(selected_dataset, selected_ticker, selected_timeframe, 500, start_date_str, end_date_str, selected_split)

if df_candles.empty:
    st.warning(f"⚠️ No candlestick data found for dataset `{selected_dataset}` and ticker `{selected_ticker}` with timeframe `{selected_timeframe}` in ClickHouse.")
    if selected_timeframe != "Daily" and not caps["has_intraday"]:
        st.info(f"💡 Explanation: Dataset `{selected_dataset}` contains **{caps['daily_count']:,} Daily bars** but has zero intraday 1m streaming bars. Please select the **Daily** timeframe.")
    elif selected_timeframe == "Daily" and not caps["has_daily"]:
        st.info(f"💡 Explanation: Dataset `{selected_dataset}` contains **{caps['intraday_count']:,} Intraday 1m bars** but has zero daily aggregated rows. Please select an Intraday timeframe (5m/10m/15m).")
    st.stop()

# Ensure datetime consistency and formatted string labels
df_candles["bar_time"] = pd.to_datetime(df_candles["bar_time"], utc=True)
is_daily = selected_timeframe.lower() in ["daily", "1d"]

if is_daily:
    df_candles["time_label"] = df_candles["bar_time"].dt.strftime("%Y/%m/%d")
    df_candles["hover_time"] = df_candles["bar_time"].dt.strftime("%Y/%m/%d (Daily Close UTC)")
else:
    df_candles["time_label"] = df_candles["bar_time"].dt.strftime("%Y/%m/%d %H:%M")
    df_candles["hover_time"] = df_candles["bar_time"].dt.strftime("%Y/%m/%d %H:%M UTC")

if len(df_candles) < 20 and not is_daily:
    st.info(f"ℹ️ Active dataset `{selected_dataset}` has {len(df_candles)} bars for timeframe `{selected_timeframe}`. For dense continuous intraday bars, switch to '⚡ Dense Intraday Demo (1m/5m)'.")

# -----------------------------------------------------------------------------
# Top Metric Strip
# -----------------------------------------------------------------------------
latest_bar = df_candles.iloc[-1]
prev_close = df_candles.iloc[-2]["close"] if len(df_candles) > 1 else latest_bar["close"]
change_abs = latest_bar["close"] - prev_close
change_pct = (change_abs / prev_close) * 100.0 if prev_close > 0 else 0.0

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Latest Close", f"${latest_bar['close']:.2f}", delta=f"{change_abs:+.2f} ({change_pct:+.2f}%)")
m2.metric("Period High", f"${df_candles['high'].max():.2f}")
m3.metric("Period Low", f"${df_candles['low'].min():.2f}")
m4.metric("Total Volume", f"{int(df_candles['volume'].sum()):,}")
m5.metric("Bars Rendered", f"{len(df_candles):,} ({selected_timeframe})")

# -----------------------------------------------------------------------------
# Canvas chart: the full window is loaded; the browser paints the viewport
# -----------------------------------------------------------------------------
lines = []
if show_sma:
    if "sma_20" in df_candles:
        lines.append({"name": "SMA 20", "color": "#38BDF8", "values": df_candles["sma_20"]})
    if "sma_50" in df_candles:
        lines.append({"name": "SMA 50", "color": "#F59E0B", "values": df_candles["sma_50"]})
if show_ema:
    if "ema_12" in df_candles:
        lines.append({"name": "EMA 12", "color": "#A855F7", "values": df_candles["ema_12"]})
    if "ema_26" in df_candles:
        lines.append({"name": "EMA 26", "color": "#EC4899", "values": df_candles["ema_26"]})
if show_vwap and "vwap" in df_candles:
    lines.append({"name": "VWAP", "color": "#EAB308", "values": df_candles["vwap"]})

markers = []

def _finite_series(column: str) -> list:
    values = []
    for value in df_candles[column]:
        values.append(None if pd.isna(value) else round(float(value), 4))
    return values

# Execution markers use the candle window, with Daily end at the close of the last session.
if show_trades:
    min_t, max_t = MarketDataService.candle_marker_bounds(df_candles, selected_timeframe)
    trade_summary = MarketDataService.summarize_trade_markers(
        dataset_id=selected_dataset,
        ticker=selected_ticker,
        session_id=selected_session if selected_session != "ALL" else None,
        start_time=min_t,
        end_time=max_t,
        alpha_id=selected_alpha_id,
    )
    df_trades = trade_summary.frame
    if trade_summary.status == "error":
        st.error(trade_summary.message)
    elif trade_summary.status == "empty":
        st.caption(trade_summary.message)
    elif trade_summary.status == "outside":
        st.caption(trade_summary.message)
    elif not df_trades.empty:
        # Snap each trade to corresponding candle time_label and validate execution context
        for _, tr in df_trades.iterrows():
            t_exec = tr["executed_at"]
            match = df_candles[df_candles["bar_time"] <= t_exec]
            candle = match.iloc[-1] if not match.empty else df_candles.iloc[0]
            pos = len(match) - 1 if not match.empty else 0
            fp = float(tr["fill_price"])
            qty = float(tr["fill_quantity"])
            direction = str(tr["direction"]).upper()
            c_high = float(candle["high"])
            c_low = float(candle["low"])
            gap = " outside the candle" if fp > c_high or fp < c_low else ""
            markers.append({
                "index": pos,
                "price": round(fp, 4),
                "side": "up" if direction == "BUY" else "down",
                "color": "#10B981" if direction == "BUY" else "#EF4444",
                "label": f"{direction} {qty:.1f} @ ${fp:.2f}{gap}",
            })

        st.caption(trade_summary.message)

book = None
if confirmed_run and selected_alpha_id:
    try:
        with st.spinner("Scoring the alpha on these candles..."):
            book = MarketDataService.evaluate_candle_book(
                df_candles,
                alpha_id=selected_alpha_id,
                dataset_id=selected_dataset,
                ticker=selected_ticker,
                timeframe=selected_timeframe,
            )
    except Exception as exc:
        st.error(f"Alpha evaluation failed for `{selected_alpha_id}`: {exc}")
    else:
        for mark in book["markers"]:
            action = mark["action"]
            markers.append({
                "index": mark["index"],
                "price": mark["price"],
                "side": mark["side"],
                "color": "#34D399" if action == "BUY" else "#F87171",
                "label": f"Signal {action}",
            })
        if book["trade_count"] == 0:
            st.caption(
                f"`{selected_alpha_id}` on `{selected_ticker}` ({selected_timeframe}) did not open a position. "
                f"Max |z| was {book['max_abs_z']:.2f}; entry threshold is {book['entry_z']:.2f}."
            )
        else:
            st.caption(
                f"`{selected_alpha_id}` on `{selected_ticker}` ({selected_timeframe}): "
                f"{book['trade_count']} round trip(s). A score earns the next bar."
            )

oscillator = None
if show_rsi and "rsi_14" in df_candles:
    oscillator = {"name": "RSI", "series": [{"name": "RSI 14", "color": "#A78BFA", "values": _finite_series("rsi_14")}]}
elif show_macd and "macd_line" in df_candles:
    oscillator = {
        "name": "MACD",
        "series": [
            {"name": "MACD", "color": "#38BDF8", "values": _finite_series("macd_line")},
            {"name": "Signal", "color": "#F43F5E", "values": _finite_series("macd_signal")},
        ],
    }

payload = build_chart_payload(
    df_candles,
    ticker=selected_ticker,
    timeframe=selected_timeframe,
    lines=lines,
    markers=markers,
    show_volume=show_volume,
    oscillator=oscillator,
)
render_market_chart(payload)

if book is not None:
    st.subheader("Book")
    st.dataframe(
        pd.DataFrame([{
            "Alpha": selected_alpha_id,
            "Ticker": selected_ticker,
            "Timeframe": selected_timeframe,
            "Booksize": f"${book['booksize']:,.0f}",
            "Sharpe": format_sharpe(
                book["sharpe"],
                book.get("sharpe_reason"),
                trade_count=book["trade_count"],
            ),
            "Max drawdown": f"{book['max_drawdown']:.2f}%",
            "PnL": f"${book['pnl']:,.2f}",
            "Trades": book["trade_count"],
            "Win rate": f"{book['win_rate']:.1f}%",
        }]),
        hide_index=True,
        use_container_width=True,
    )
    st.caption(
        "One ticker takes the whole booksize. The score on a bar earns the next bar, "
        "after 6 bps of slippage and commission. This run is not written to paper_trades."
    )
