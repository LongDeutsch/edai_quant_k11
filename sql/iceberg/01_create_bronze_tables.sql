-- =============================================================================
-- Apache Iceberg DDL: Bronze Zone Tables
-- Naming convention: raw_ prefix
-- Immutable, append-only raw ingest from simulated external sources
-- =============================================================================

CREATE DATABASE IF NOT EXISTS lakehouse.bronze;

-- 1. Raw Market OHLCV Table (Daily & Intraday Bars)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.raw_market_ohlcv (
    ticker_symbol STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT,
    exchange STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING iceberg
PARTITIONED BY (trade_date)
LOCATION 's3://lakehouse/bronze/raw_market_ohlcv'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- 2. Raw Unstructured Financial News Table
CREATE TABLE IF NOT EXISTS lakehouse.bronze.raw_financial_news (
    article_id STRING,
    ticker_symbol STRING,
    publish_date DATE,
    published_ts TIMESTAMP,
    headline STRING,
    summary STRING,
    source_outlet STRING,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (publish_date)
LOCATION 's3://lakehouse/bronze/raw_financial_news'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
