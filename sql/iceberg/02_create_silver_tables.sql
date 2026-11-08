-- =============================================================================
-- Apache Iceberg DDL: Silver Zone Tables
-- Naming convention: stg_ prefix
-- Cleaned, deduplicated, schema-evolved intermediate zone
-- =============================================================================

CREATE DATABASE IF NOT EXISTS lakehouse.silver;

-- 1. Staged Market OHLCV Table (With Iceberg Schema Evolution Support)
CREATE TABLE IF NOT EXISTS lakehouse.silver.stg_market_ohlcv (
    ticker_symbol STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT,
    vwap DOUBLE,           -- Mid-stream evolved column (2024-06-01+)
    trade_count INT,       -- Mid-stream evolved column (2024-06-01+)
    exchange STRING,
    market_regime STRING,
    processed_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(trade_date), bucket(16, ticker_symbol))
LOCATION 's3://lakehouse/silver/stg_market_ohlcv'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);

-- 2. Staged Financial News Table
CREATE TABLE IF NOT EXISTS lakehouse.silver.stg_financial_news (
    article_id STRING,
    ticker_symbol STRING,
    publish_date DATE,
    published_ts TIMESTAMP,
    headline STRING,
    summary STRING,
    sentiment_score DOUBLE, -- Evolved sentiment column
    source_outlet STRING,
    processed_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(publish_date))
LOCATION 's3://lakehouse/silver/stg_financial_news'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
