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
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from quant_edai.serving.dashboard.dashboard_service import MarketDataService

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
caps = MarketDataService.discover_dataset_capabilities(selected_dataset)
st.sidebar.caption(f"Resolved Dataset ID: `{selected_dataset}`" + (f" | Split: `{selected_split}`" if selected_split else ""))
st.sidebar.caption(f"📊 Discovered: **{caps['daily_count']:,}** Daily bars | **{caps['intraday_count']:,}** Intraday 1m bars")

tickers = caps["tickers"]
default_ticker = "AAPL" if "AAPL" in tickers else (tickers[0] if tickers else "AAPL")
selected_ticker = st.sidebar.selectbox("Ticker Symbol", tickers, index=tickers.index(default_ticker) if default_ticker in tickers else 0)

# Dynamic Timeframe Selection based on dataset capabilities
supported_tfs = caps["supported_timeframes"]
default_tf = caps["default_timeframe"]
default_idx = supported_tfs.index(default_tf) if default_tf in supported_tfs else 0

selected_timeframe = st.sidebar.selectbox(
    "Candle Timeframe",
    supported_tfs,
    index=default_idx,
    help="Timeframes dynamically discovered from ClickHouse: 1m/5m/10m/15m require intraday streaming bars; Daily requires historical OHLCV.",
)

if adv_mode:
    candle_limit = st.sidebar.slider("Max Candles to Display", min_value=50, max_value=1000, value=300, step=50)
else:
    candle_limit = 300

filter_by_date = st.sidebar.checkbox("📅 Filter by Date Window", value=False, help="Filter candles by start and end dates.")
start_date_str = None
end_date_str = None
if filter_by_date:
    col_d1, col_d2 = st.sidebar.columns(2)
    with col_d1:
        s_date = st.date_input("From Date", value=None)
        start_date_str = str(s_date) if s_date else None
    with col_d2:
        e_date = st.date_input("To Date", value=None)
        end_date_str = str(e_date) if e_date else None

st.sidebar.markdown("---")
st.sidebar.subheader("📐 Technical Overlays & Execution")
show_sma = st.sidebar.checkbox("SMA (20 & 50)", value=False)
show_ema = st.sidebar.checkbox("EMA (12 & 26)", value=False)
show_vwap = st.sidebar.checkbox("Intraday VWAP", value=False)
show_volume = st.sidebar.checkbox("Volume Bar Subplot", value=True)
show_rsi = st.sidebar.checkbox("RSI (14)", value=False)
show_macd = st.sidebar.checkbox("MACD (12, 26, 9)", value=False)

st.sidebar.markdown("---")
st.sidebar.subheader("🎯 Execution Markers")
show_trades = st.sidebar.checkbox("Display Execution Trades (PostgreSQL)", value=True)
selected_session = "ALL"
if show_trades:
    trade_sessions = MarketDataService.get_trade_sessions(dataset_id=selected_dataset, ticker=selected_ticker)
    if trade_sessions:
        selected_session = st.sidebar.selectbox("Filter by Trading Session", ["ALL"] + trade_sessions, index=0)
    else:
        st.sidebar.caption("No trade sessions recorded for this dataset/ticker.")

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
    df_candles = load_and_prepare_data(selected_dataset, selected_ticker, selected_timeframe, candle_limit, start_date_str, end_date_str, selected_split)

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
m5.metric("Bars Rendered", f"{len(df_candles)} ({selected_timeframe})")

# -----------------------------------------------------------------------------
# Chart Construction (Plotly Subplots)
# -----------------------------------------------------------------------------
rows = 1
row_heights = [0.7]

if show_volume:
    rows += 1
    row_heights.append(0.15)
if show_rsi or show_macd:
    rows += 1
    row_heights.append(0.15)

# Normalize heights
total_h = sum(row_heights)
row_heights = [h / total_h for h in row_heights]

fig = make_subplots(
    rows=rows,
    cols=1,
    shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=row_heights,
)

# 1. Main Candlestick Chart (Row 1)
candle_hover = [
    f"<b>{selected_ticker} ({selected_timeframe})</b><br>"
    f"Time: {t}<br>"
    f"Open: ${o:.2f}<br>"
    f"High: ${h:.2f}<br>"
    f"Low: ${l:.2f}<br>"
    f"Close: ${c:.2f}<br>"
    f"Volume: {int(v):,}"
    for t, o, h, l, c, v in zip(
        df_candles["hover_time"],
        df_candles["open"],
        df_candles["high"],
        df_candles["low"],
        df_candles["close"],
        df_candles["volume"],
    )
]

fig.add_trace(
    go.Candlestick(
        x=df_candles["time_label"],
        open=df_candles["open"],
        high=df_candles["high"],
        low=df_candles["low"],
        close=df_candles["close"],
        name=f"{selected_ticker} ({selected_timeframe})",
        increasing_line_color="#10B981",
        decreasing_line_color="#EF4444",
        increasing_fillcolor="#10B981",
        decreasing_fillcolor="#EF4444",
        increasing_line_width=1.5,
        decreasing_line_width=1.5,
        hovertext=candle_hover,
        hoverinfo="text",
    ),
    row=1, col=1,
)

# Overlays (SMA, EMA, VWAP)
if show_sma:
    if "sma_20" in df_candles:
        fig.add_trace(go.Scatter(x=df_candles["time_label"], y=df_candles["sma_20"], name="SMA 20", line=dict(color="#38BDF8", width=1.5)), row=1, col=1)
    if "sma_50" in df_candles:
        fig.add_trace(go.Scatter(x=df_candles["time_label"], y=df_candles["sma_50"], name="SMA 50", line=dict(color="#F59E0B", width=1.5)), row=1, col=1)

if show_ema:
    if "ema_12" in df_candles:
        fig.add_trace(go.Scatter(x=df_candles["time_label"], y=df_candles["ema_12"], name="EMA 12", line=dict(color="#A855F7", width=1.2, dash="dot")), row=1, col=1)
    if "ema_26" in df_candles:
        fig.add_trace(go.Scatter(x=df_candles["time_label"], y=df_candles["ema_26"], name="EMA 26", line=dict(color="#EC4899", width=1.2, dash="dot")), row=1, col=1)

if show_vwap and "vwap" in df_candles:
    fig.add_trace(go.Scatter(x=df_candles["time_label"], y=df_candles["vwap"], name="VWAP", line=dict(color="#EAB308", width=1.8, dash="dash")), row=1, col=1)

# Execution Markers Alignment (P0)
if show_trades:
    min_t = df_candles["bar_time"].min()
    max_t = df_candles["bar_time"].max()
    df_trades = MarketDataService.get_trade_markers(
        dataset_id=selected_dataset,
        ticker=selected_ticker,
        session_id=selected_session if selected_session != "ALL" else None,
        start_time=min_t,
        end_time=max_t,
    )
    if not df_trades.empty:
        # Snap each trade to corresponding candle time_label and validate execution context
        mapped_trades = []
        for _, tr in df_trades.iterrows():
            t_exec = tr["executed_at"]
            match = df_candles[df_candles["bar_time"] <= t_exec]
            candle = match.iloc[-1] if not match.empty else df_candles.iloc[0]
            t_label = candle["time_label"]
            c_high = float(candle["high"])
            c_low = float(candle["low"])
            fp = float(tr["fill_price"])
            slip = float(tr["slippage_bps"])
            qty = float(tr["fill_quantity"])
            pnl = float(tr["realized_pnl"])
            direction = str(tr["direction"]).upper()
            sess = str(tr["session_id"])
            dt_str = t_exec.strftime("%Y/%m/%d %H:%M:%S UTC")

            is_gap = fp > c_high or fp < c_low
            gap_note = f"<br>⚠️ Slippage Gap: fill ${fp:.2f} vs candle [{c_low:.2f}, {c_high:.2f}]" if is_gap else ""
            pnl_note = f"<br>Realized PnL: ${pnl:+.2f}" if pnl != 0.0 else ""

            ht = (
                f"<b>{direction} TRADE FILL</b><br>"
                f"Shares: {qty:.1f} @ ${fp:.2f}<br>"
                f"Executed At: {dt_str}<br>"
                f"Session: {sess} | Slippage: {slip:.1f} bps"
                f"{gap_note}{pnl_note}"
            )

            mapped_trades.append({
                "time_label": t_label,
                "fill_price": fp,
                "direction": direction,
                "hovertext": ht,
            })

        m_df = pd.DataFrame(mapped_trades)
        buys = m_df[m_df["direction"] == "BUY"]
        sells = m_df[m_df["direction"] == "SELL"]

        if not buys.empty:
            fig.add_trace(
                go.Scatter(
                    x=buys["time_label"],
                    y=buys["fill_price"],
                    mode="markers",
                    name=f"Trade Buy ({len(buys)})",
                    marker=dict(symbol="triangle-up", size=13, color="#10B981", line=dict(width=1.5, color="#FFFFFF")),
                    hovertext=buys["hovertext"].tolist(),
                    hoverinfo="text",
                ),
                row=1, col=1,
            )
        if not sells.empty:
            fig.add_trace(
                go.Scatter(
                    x=sells["time_label"],
                    y=sells["fill_price"],
                    mode="markers",
                    name=f"Trade Sell ({len(sells)})",
                    marker=dict(symbol="triangle-down", size=13, color="#EF4444", line=dict(width=1.5, color="#FFFFFF")),
                    hovertext=sells["hovertext"].tolist(),
                    hoverinfo="text",
                ),
                row=1, col=1,
            )
        st.caption(f"🎯 Displaying **{len(df_trades)}** verified execution trades from PostgreSQL (`paper_trades`) matching dataset `{selected_dataset}` and ticker `{selected_ticker}`.")
    else:
        st.caption(f"ℹ️ No matching execution trades found for `{selected_ticker}` in dataset `{selected_dataset}` within the rendered window.")

current_row = 2

# 2. Volume Subplot
if show_volume:
    vol_colors = ["#10B981" if c >= o else "#EF4444" for c, o in zip(df_candles["close"], df_candles["open"])]
    fig.add_trace(
        go.Bar(
            x=df_candles["time_label"],
            y=df_candles["volume"],
            name="Volume",
            marker_color=vol_colors,
            opacity=0.7,
        ),
        row=current_row, col=1,
    )
    fig.update_yaxes(title_text="Vol", row=current_row, col=1, showgrid=True, gridcolor="#334155")
    current_row += 1

# 3. Oscillators (RSI or MACD)
if show_rsi and "rsi_14" in df_candles:
    fig.add_trace(
        go.Scatter(x=df_candles["time_label"], y=df_candles["rsi_14"], name="RSI 14", line=dict(color="#A78BFA", width=1.5)),
        row=current_row, col=1,
    )
    fig.add_hline(y=70, line_dash="dot", line_color="#EF4444", opacity=0.6, row=current_row, col=1)
    fig.add_hline(y=30, line_dash="dot", line_color="#10B981", opacity=0.6, row=current_row, col=1)
    fig.update_yaxes(title_text="RSI", range=[0, 100], row=current_row, col=1, showgrid=True, gridcolor="#334155")

elif show_macd and "macd_line" in df_candles:
    fig.add_trace(go.Scatter(x=df_candles["time_label"], y=df_candles["macd_line"], name="MACD", line=dict(color="#38BDF8", width=1.5)), row=current_row, col=1)
    fig.add_trace(go.Scatter(x=df_candles["time_label"], y=df_candles["macd_signal"], name="Signal", line=dict(color="#F43F5E", width=1.2)), row=current_row, col=1)
    hist_colors = ["#10B981" if h >= 0 else "#EF4444" for h in df_candles["macd_histogram"]]
    fig.add_trace(go.Bar(x=df_candles["time_label"], y=df_candles["macd_histogram"], name="Hist", marker_color=hist_colors), row=current_row, col=1)
    fig.update_yaxes(title_text="MACD", row=current_row, col=1, showgrid=True, gridcolor="#334155")

# Tick labels formatting: evenly spaced ticks without overlapping
n_candles = len(df_candles)
tick_step = max(1, n_candles // 8)
tick_indices = list(range(0, n_candles, tick_step))
if tick_indices[-1] != n_candles - 1:
    tick_indices.append(n_candles - 1)
tick_vals = [df_candles["time_label"].iloc[i] for i in tick_indices]

# Layout styling
fig.update_layout(
    height=750,
    xaxis_rangeslider_visible=False,
    template="plotly_dark",
    margin=dict(l=50, r=40, t=30, b=40),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)

fig.update_xaxes(
    type="category",
    tickvals=tick_vals,
    ticktext=tick_vals,
    tickangle=-25,
    showgrid=True,
    gridcolor="#334155",
)
fig.update_yaxes(
    showgrid=True,
    gridcolor="#334155",
)

st.plotly_chart(fig, use_container_width=True)
