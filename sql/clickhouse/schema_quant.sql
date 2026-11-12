-- =============================================================================
-- ClickHouse DDL: Quant Analytics & Serving Layer
-- Engine: ReplacingMergeTree for deduplication and fast OLAP queries
-- =============================================================================

CREATE DATABASE IF NOT EXISTS quant_dw;

-- 1. Market Daily OHLCV Table
CREATE TABLE IF NOT EXISTS quant_dw.market_daily_ohlcv (
    dataset_id LowCardinality(String),
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
    version UInt64 DEFAULT 1,
    created DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (dataset_id, ticker_symbol, trade_date);

-- 2. Dimension Companies Table
CREATE TABLE IF NOT EXISTS quant_dw.dim_companies (
    dataset_id LowCardinality(String),
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
ORDER BY (dataset_id, company_key, ticker_symbol);

-- 3. Technical Indicators & Features Serving Table
CREATE TABLE IF NOT EXISTS quant_dw.feat_technical_indicators (
    dataset_id LowCardinality(String),
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
ORDER BY (dataset_id, ticker_symbol, event_timestamp);

-- 4. Market Intraday Bars Table (1-minute / sub-second intraday serving, M1-19 aligned)
CREATE TABLE IF NOT EXISTS quant_dw.market_intraday_bars (
    dataset_id LowCardinality(String),
    ticker_symbol LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    event_timestamp DateTime64(3, 'UTC'),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume UInt64,
    vwap Float64,
    trade_count UInt32 DEFAULT 1,
    exchange LowCardinality(String) DEFAULT 'NASDAQ',
    version UInt64 DEFAULT 1,
    is_deleted UInt8 DEFAULT 0,
    created DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (dataset_id, ticker_symbol, window_start);

-- 5. Dataset lineage (P1.2): every serving row records the immutable dataset_id it was synced from.
--    Idempotent ALTERs so pre-existing tables are upgraded in place.
ALTER TABLE quant_dw.market_daily_ohlcv ADD COLUMN IF NOT EXISTS dataset_id LowCardinality(String) DEFAULT '';
ALTER TABLE quant_dw.dim_companies ADD COLUMN IF NOT EXISTS dataset_id LowCardinality(String) DEFAULT '';
ALTER TABLE quant_dw.feat_technical_indicators ADD COLUMN IF NOT EXISTS dataset_id LowCardinality(String) DEFAULT '';
ALTER TABLE quant_dw.market_intraday_bars ADD COLUMN IF NOT EXISTS dataset_id LowCardinality(String) DEFAULT '';

-- 6. Canonical 4Y Split Provenance (Phase 2): IS, OOS, TRADING split isolation
ALTER TABLE quant_dw.market_daily_ohlcv ADD COLUMN IF NOT EXISTS split_id LowCardinality(String) DEFAULT '';
ALTER TABLE quant_dw.market_intraday_bars ADD COLUMN IF NOT EXISTS split_id LowCardinality(String) DEFAULT '';
ALTER TABLE quant_dw.feat_technical_indicators ADD COLUMN IF NOT EXISTS split_id LowCardinality(String) DEFAULT '';

CREATE OR REPLACE VIEW quant_dw.v_market_daily_ohlcv_splits AS
SELECT
    *,
    if(split_id != '', split_id,
       if(trade_date <= '2025-04-30', 'IS',
          if(trade_date <= '2026-04-30', 'OOS', 'TRADING')
       )
    ) AS effective_split_id
FROM quant_dw.market_daily_ohlcv;

CREATE OR REPLACE VIEW quant_dw.v_market_intraday_bars_splits AS
SELECT
    *,
    if(split_id != '', split_id,
       if(toDate(window_start) <= '2025-04-30', 'IS',
          if(toDate(window_start) <= '2026-04-30', 'OOS', 'TRADING')
       )
    ) AS effective_split_id
FROM quant_dw.market_intraday_bars;
