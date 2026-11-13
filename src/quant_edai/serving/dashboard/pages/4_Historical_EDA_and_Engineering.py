"""Page 4: Historical EDA & Data Engineering Analytics.

Features:
- Exploratory Data Analysis (EDA) from ClickHouse: Returns distributions, cross-asset correlation matrix, price volatility.
- Lakehouse Data Quality & Lineage: Inspect Bronze -> Silver -> Gold file counts and partition health.
- Real Live OLAP Benchmark: ClickHouse vs PyArrow/Lakehouse scan on identical dataset (latency, rows scanned, speedup factor).
- Strictly verified runtime numbers, no hardcoded benchmark values.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from quant_edai.serving.dashboard.dashboard_service import (
    HistoricalEDAService,
    MarketDataService,
)

st.set_page_config(
    page_title="Historical EDA & Data Engineering | Quant Terminal",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🔬 Historical Market EDA & Data Engineering")
st.caption("Deep statistical exploration of market distributions, Medallion Lakehouse data quality, and live OLAP query benchmarking.")

# -----------------------------------------------------------------------------
# Tabs Layout
# -----------------------------------------------------------------------------
tab_eda, tab_quality, tab_bench = st.tabs([
    "📊 Market Distribution EDA",
    "🛡️ Lakehouse Quality & Lineage",
    "⚡ Live OLAP Benchmark (ClickHouse vs Lakehouse)",
])

adv_mode = st.sidebar.checkbox("⚙️ Advanced Mode", value=False, help="Show raw ClickHouse test/idempotency datasets.")
ds_options = MarketDataService.get_catalog_options(advanced_mode=adv_mode)
# Default to Daily Factor Research (index 1) for EDA
default_idx = 1 if len(ds_options) > 1 else 0
selected_preset = st.sidebar.selectbox("Active Dataset Preset", ds_options, index=default_idx)
selected_dataset = MarketDataService.resolve_dataset_id(selected_preset)
st.sidebar.caption(f"Resolved Dataset ID: `{selected_dataset}`")

# =============================================================================
# TAB 1: Market Distribution EDA
# =============================================================================
with tab_eda:
    st.subheader(f"Statistical Distribution & Cross-Asset Correlation: `{selected_dataset}`")

    # Split Distribution Overview
    st.markdown("#### 🕒 Chronological 3-Split Distribution Overview")
    from quant_edai.serving.dashboard.dashboard_service import run_clickhouse_query
    df_splits = run_clickhouse_query(f"""
        SELECT 
            if(split_id = '', 'LEGACY', split_id) AS split,
            count(*) AS daily_bars,
            min(trade_date) AS start_date,
            max(trade_date) AS end_date,
            uniqExact(ticker_symbol) AS tickers_count
        FROM quant_dw.market_daily_ohlcv FINAL
        WHERE dataset_id = '{selected_dataset}'
        GROUP BY split
        ORDER BY min(trade_date) ASC
    """)
    if not df_splits.empty:
        st.dataframe(df_splits, use_container_width=True, hide_index=True)
    else:
        st.info("No split metadata found for this dataset.")

    st.markdown("---")
    st.markdown("#### 📈 Price Volatility & Trading Volume Statistics")

    with st.spinner("Analyzing market distributions from ClickHouse..."):
        eda_data = HistoricalEDAService.get_market_eda_summary(selected_dataset)

    df_stats = eda_data.get("stats", pd.DataFrame())
    df_corr = eda_data.get("correlation", pd.DataFrame())

    if not df_stats.empty:
        st.dataframe(df_stats, use_container_width=True, hide_index=True)

        col_bar, col_corr = st.columns([1, 1])

        with col_bar:
            # Volume bar chart
            fig_vol = px.bar(
                df_stats,
                x="ticker_symbol",
                y="avg_volume",
                title="Average Daily Trading Volume across Tickers",
                template="plotly_dark",
                labels={"ticker_symbol": "Ticker", "avg_volume": "Avg Daily Volume"},
                color="avg_volume",
                color_continuous_scale="Blues",
            )
            fig_vol.update_layout(height=400, margin=dict(l=30, r=30, t=50, b=30))
            st.plotly_chart(fig_vol, use_container_width=True)

        with col_corr:
            if not df_corr.empty:
                fig_corr = px.imshow(
                    df_corr,
                    text_auto=True,
                    aspect="auto",
                    title="Cross-Ticker Daily Return Correlation Heatmap",
                    template="plotly_dark",
                    color_continuous_scale="RdBu_r",
                )
                fig_corr.update_layout(height=400, margin=dict(l=30, r=30, t=50, b=30))
                st.plotly_chart(fig_corr, use_container_width=True)
            else:
                st.info("Insufficient daily history in this dataset to generate correlation matrix.")
    else:
        st.warning(f"No daily OHLCV rows found in ClickHouse for dataset `{selected_dataset}`.")


# =============================================================================
# TAB 2: Lakehouse Quality & Lineage
# =============================================================================
with tab_quality:
    st.subheader("Medallion Lakehouse Architecture Health")
    st.caption("Inspecting partitioned parquet files and storage lineage across MinIO/Lakehouse storage zones.")

    lakehouse_stats = HistoricalEDAService.get_lakehouse_quality_metrics()

    q1, q2, q3 = st.columns(3)
    q1.metric("Bronze Zone Files", f"{lakehouse_stats['bronze_files_count']:,} Parquet Files", delta="Raw Ingest")
    q2.metric("Silver Zone Files", f"{lakehouse_stats['silver_files_count']:,} Parquet Files", delta="Deduplicated Ticks")
    q3.metric("Gold Zone Features", f"{lakehouse_stats['gold_files_count']:,} Parquet Files", delta="Technical Indicators")

    st.markdown("---")
    st.subheader("Data Lineage & Provenance Flow")
    st.markdown("""
    - **Bronze Ingestion (DP1)**: `s3://lakehouse/bronze/raw_market_ticks/` $\\longrightarrow$ Preserves raw source immutability & SHA256 manifests.
    - **Silver Resample (DP2)**: PySpark event-time deduplication with watermarking $\\longrightarrow$ `quant_silver.market_ticks_dedup`.
    - **Gold Features (DP3)**: 10 institutional technical indicators $\\longrightarrow$ `default.feat_technical_indicators`.
    - **Serving Sync (DP4)**: ClickHouse synchronization to `quant_dw` with `ORDER BY (dataset_id, ...)` isolation.
    """)


# =============================================================================
# TAB 3: Live OLAP Benchmark
# =============================================================================
with tab_bench:
    st.subheader("⚡ Live OLAP Execution Benchmark")
    st.markdown("""
    Measure **real query execution latency** between **ClickHouse OLAP** (columnar vectorized index) 
    and **PyArrow Lakehouse Parquet Scan** on the exact same dataset.
    """)

    st.info("""
    💡 **Architectural Benchmark Methodology**:
    - **ClickHouse Serving**: Vectorized SIMD execution over `ReplacingMergeTree`, sub-50ms latency designed for real-time serving and dashboards.
    - **PyArrow Scan**: Single-node C++ columnar scan of partitioned Parquet files on disk.
    - **Spark SQL (Iceberg Catalog)**: Distributed JVM batch processing designed for multi-node ETL and feature generation across terabytes. Running interactive sub-second queries on Spark incurs JVM planning and catalog overhead (~5-15s), making ClickHouse the strictly optimal choice for Dashboard serving.
    """)

    bench_ticker = st.selectbox("Benchmark Target Ticker", ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL"], index=0)
    bench_btn = st.button("⚡ Execute Live Benchmark", type="primary")

    if bench_btn:
        with st.spinner("Executing real queries on ClickHouse and Lakehouse Parquet..."):
            bench_res = HistoricalEDAService.run_live_olap_benchmark(dataset_id=selected_dataset, ticker=bench_ticker)

        st.success(f"Benchmark completed! ClickHouse speedup factor: **{bench_res['speedup_factor']}x**.")

        b1, b2, b3 = st.columns(3)
        b1.metric("ClickHouse Latency", f"{bench_res['clickhouse']['latency_ms']} ms", delta="Vectorized OLAP")
        b2.metric("Parquet Scan Latency", f"{bench_res['parquet_batch']['latency_ms']} ms", delta=f"{bench_res['parquet_batch']['rows_scanned']} rows scanned")
        b3.metric("Relative Performance", f"{bench_res['speedup_factor']}x Faster", delta="Hardware Native")

        st.json(bench_res)
