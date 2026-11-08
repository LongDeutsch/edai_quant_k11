-- =============================================================================
-- ClickHouse DDL: Quant Analytics & Serving Layer
-- Engine: ReplacingMergeTree for deduplication and fast OLAP queries
-- =============================================================================

CREATE DATABASE IF NOT EXISTS quant_dw;

-- 1. Market Daily OHLCV Table
CREATE TABLE IF NOT EXISTS quant_dw.market_daily_ohlcv (
    ticker_symbol LowCardinality(String),
    trade_date Date,
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume UInt64,
    vwap Float64,
    trade_count UInt32,
    daily_return Float64,
    created DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created)
PARTITION BY toYYYYMM(trade_date)
PRIMARY KEY (ticker_symbol, trade_date)
ORDER BY (ticker_symbol, trade_date);

-- 2. Dimension Companies Table
CREATE TABLE IF NOT EXISTS quant_dw.dim_companies (
    company_key UInt64,
    ticker_symbol LowCardinality(String),
    company_name String,
    sector LowCardinality(String),
    industry LowCardinality(String),
    market_cap_category LowCardinality(String),
    valid_from_ts DateTime,
    valid_to_ts DateTime,
    is_current UInt8
) ENGINE = ReplacingMergeTree(valid_from_ts)
PRIMARY KEY (company_key)
ORDER BY (company_key, ticker_symbol);

-- 3. Technical Indicators & Features Serving Table
CREATE TABLE IF NOT EXISTS quant_dw.feat_technical_indicators (
    ticker_symbol LowCardinality(String),
    event_timestamp DateTime,
    created DateTime DEFAULT now(),
    close_price Float64,
    sma_20 Float64,
    sma_50 Float64,
    sma_ratio_20_50 Float64,
    rsi_14d Float64,
    macd_12_26 Float64,
    macd_signal_9 Float64,
    macd_histogram Float64,
    rolling_volatility_30d Float64,
    intraday_return_1d Float64
) ENGINE = ReplacingMergeTree(created)
PARTITION BY toYYYYMM(event_timestamp)
PRIMARY KEY (ticker_symbol, event_timestamp)
ORDER BY (ticker_symbol, event_timestamp);
