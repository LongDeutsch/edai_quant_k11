-- =============================================================================
-- Apache Iceberg DDL: Gold Zone Tables
-- Star Schema Naming Conventions: dim_, fact_, feat_ prefixes
-- Curated, business-ready data marts for Quant Research & ML Feature Store
-- =============================================================================

CREATE DATABASE IF NOT EXISTS lakehouse.gold;

-- 1. Dimension Table: dim_companies (Slowly Changing Dimension Type 2)
-- Satisfies Rubric M1-33: SCD2 with valid_from_ts, valid_to_ts, is_current
-- Satisfies Rubric M1-36: dim_ prefix
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_companies (
    company_key BIGINT,
    ticker_symbol STRING,
    company_name STRING,
    sector STRING,
    industry STRING,
    market_cap_category STRING,
    valid_from_ts TIMESTAMP,
    valid_to_ts TIMESTAMP,
    is_current BOOLEAN
)
USING iceberg
LOCATION 's3://lakehouse/gold/dim_companies'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- 2. Fact Table: fact_daily_ohlcv (Star Schema Fact)
-- Satisfies Rubric M1-36: fact_ prefix
-- Relates to dim_companies via ticker_symbol
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_daily_ohlcv (
    ticker_symbol STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT,
    vwap DOUBLE,
    trade_count INT,
    daily_return DOUBLE,
    created TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(trade_date), bucket(16, ticker_symbol))
LOCATION 's3://lakehouse/gold/fact_daily_ohlcv'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- 3. Fact Table: fact_market_news
-- Satisfies Rubric M1-36: fact_ prefix
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_market_news (
    article_id STRING,
    ticker_symbol STRING,
    published_ts TIMESTAMP,
    headline STRING,
    summary STRING,
    sentiment_score DOUBLE,
    sentiment_label STRING,
    source_outlet STRING,
    created TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(published_ts))
LOCATION 's3://lakehouse/gold/fact_market_news'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- 4. Feature Table: feat_ticker_technical_indicators
-- Satisfies Rubric M1-34: feat_ table with required columns: event_timestamp & created
-- Offline feature table serving Feast Feature Store
CREATE TABLE IF NOT EXISTS lakehouse.gold.feat_ticker_technical_indicators (
    ticker_symbol STRING,
    event_timestamp TIMESTAMP,  -- REQUIRED by Rubric M1-34 for point-in-time correctness
    created TIMESTAMP,          -- REQUIRED by Rubric M1-34 for feature registration audit
    close_price DOUBLE,
    sma_20 DOUBLE,
    sma_50 DOUBLE,
    sma_ratio_20_50 DOUBLE,
    rsi_14d DOUBLE,
    macd_12_26 DOUBLE,
    macd_signal_9 DOUBLE,
    macd_histogram DOUBLE,
    rolling_volatility_30d DOUBLE,
    intraday_return_1d DOUBLE
)
USING iceberg
PARTITIONED BY (days(event_timestamp), bucket(16, ticker_symbol))
LOCATION 's3://lakehouse/gold/feat_ticker_technical_indicators'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
