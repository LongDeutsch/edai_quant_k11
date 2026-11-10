"""Page 2: Real-Time Candlestick Charts with ClickHouse Serving & Trade Markers."""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.serving.candlestick_loader import load_candlestick_data

st.set_page_config(page_title="Candlestick Charts | Quant AI", page_icon="🕯️", layout="wide")

st.title("🕯️ Real-Time Candlestick Charts & Execution Markers")
st.markdown(
    "Interactive candles: **prefer ClickHouse OLAP** (`quant_dw.market_intraday_bars` / M1-19). "
    "Falls back to lakehouse Parquet only if ClickHouse is offline. "
    "Trade markers always come from the paper trading DB (SQLite/Postgres)."
)

tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "PG", "SPY"]

col_sel, col_info = st.columns([1, 3])
with col_sel:
    selected_ticker = st.selectbox("Select Equity Ticker", tickers, index=0)

df_candles, source_label = load_candlestick_data(selected_ticker)

with col_info:
    st.markdown(f"**Data Source**: `{source_label}` | **Bars Loaded**: `{len(df_candles)}`")

# Calculate technical indicators: SMA(20), SMA(50), VWAP
if len(df_candles) > 0:
    df_candles["sma_20"] = df_candles["close_price"].rolling(window=20, min_periods=5).mean()
    df_candles["sma_50"] = df_candles["close_price"].rolling(window=50, min_periods=10).mean()
    if "vwap" not in df_candles.columns or df_candles["vwap"].isna().all():
        cum_vol = df_candles["volume"].cumsum()
        cum_pv = (df_candles["close_price"] * df_candles["volume"]).cumsum()
        df_candles["vwap"] = cum_pv / cum_vol.replace(0, 1)

    # Query trades from PostgreSQL / SQLite
    db = PaperDatabase()
    all_trades = db.get_recent_trades(limit=500)
    ticker_trades = [t for t in all_trades if t.get("ticker_symbol") == selected_ticker]

    # Create figure with 2 subplots (Candles on top, Volume on bottom)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.75, 0.25])

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df_candles["event_timestamp"],
        open=df_candles["open_price"],
        high=df_candles["high_price"],
        low=df_candles["low_price"],
        close=df_candles["close_price"],
        name=f"{selected_ticker} 1m",
        increasing_line_color="#26a69a",
        decreasing_line_color="#ef5350",
    ), row=1, col=1)

    # Overlays: SMA 20, SMA 50, VWAP
    fig.add_trace(go.Scatter(x=df_candles["event_timestamp"], y=df_candles["sma_20"], line=dict(color="#2962FF", width=1.5), name="SMA(20)"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_candles["event_timestamp"], y=df_candles["sma_50"], line=dict(color="#FF6D00", width=1.5), name="SMA(50)"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_candles["event_timestamp"], y=df_candles["vwap"], line=dict(color="#AB47BC", width=1.5, dash="dot"), name="Intraday VWAP"), row=1, col=1)

    # Execution Markers overlay
    buy_trades = [t for t in ticker_trades if t.get("direction") == "BUY"]
    sell_trades = [t for t in ticker_trades if t.get("direction") == "SELL"]

    if buy_trades:
        fig.add_trace(go.Scatter(
            x=[t["executed_at"] for t in buy_trades],
            y=[float(t["fill_price"]) * 0.999 for t in buy_trades],
            mode="markers",
            marker=dict(symbol="triangle-up", size=12, color="#00E676", line=dict(color="black", width=1)),
            name="BUY Order",
            hovertext=[f"BUY {t['fill_quantity']} sh @ ${float(t['fill_price']):.2f}" for t in buy_trades],
        ), row=1, col=1)

    if sell_trades:
        fig.add_trace(go.Scatter(
            x=[t["executed_at"] for t in sell_trades],
            y=[float(t["fill_price"]) * 1.001 for t in sell_trades],
            mode="markers",
            marker=dict(symbol="triangle-down", size=12, color="#FF1744", line=dict(color="black", width=1)),
            name="SELL / EXIT",
            hovertext=[f"SELL {t['fill_quantity']} sh @ ${float(t['fill_price']):.2f} (PnL: ${float(t.get('realized_pnl', 0)):+.2f})" for t in sell_trades],
        ), row=1, col=1)

    # Volume Subplot
    colors = ["#26a69a" if c >= o else "#ef5350" for c, o in zip(df_candles["close_price"], df_candles["open_price"])]
    fig.add_trace(go.Bar(
        x=df_candles["event_timestamp"],
        y=df_candles["volume"],
        name="Volume",
        marker_color=colors,
    ), row=2, col=1)

    fig.update_layout(
        height=600,
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Trades summary for ticker
    if ticker_trades:
        st.subheader(f"Executed Trades for {selected_ticker} ({len(ticker_trades)} total)")
        st.dataframe(pd.DataFrame(ticker_trades)[["trade_id", "direction", "fill_price", "fill_quantity", "realized_pnl", "executed_at"]], use_container_width=True)
