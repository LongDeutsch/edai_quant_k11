-- ==============================================================================
-- Schema: 02_paper_trading_schema.sql
-- Description: ACID Paper Trading Engine Persistence & Idempotency Schema (M1-38)
-- Idempotency key: UNIQUE (session_id, ticker_symbol, bar_timestamp, direction, order_type)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS paper_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL,
    ticker_symbol VARCHAR(16) NOT NULL,
    order_type VARCHAR(16) NOT NULL,    -- 'MARKET', 'STOP_LOSS', 'TAKE_PROFIT', 'EOD_FLAT', 'CIRCUIT_BREAKER'
    direction VARCHAR(8) NOT NULL,      -- 'BUY', 'SELL'
    quantity NUMERIC(14, 4) NOT NULL,
    requested_price NUMERIC(14, 4) NOT NULL,
    status VARCHAR(16) NOT NULL,        -- 'FILLED', 'REJECTED', 'CANCELLED'
    bar_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_order_idempotency UNIQUE (session_id, ticker_symbol, bar_timestamp, direction, order_type)
);

CREATE INDEX IF NOT EXISTS idx_paper_orders_session ON paper_orders(session_id);
CREATE INDEX IF NOT EXISTS idx_paper_orders_ticker ON paper_orders(ticker_symbol);

CREATE TABLE IF NOT EXISTS paper_trades (
    trade_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) REFERENCES paper_orders(order_id),
    session_id VARCHAR(64) NOT NULL,
    ticker_symbol VARCHAR(16) NOT NULL,
    direction VARCHAR(8) NOT NULL,
    fill_price NUMERIC(14, 4) NOT NULL,
    fill_quantity NUMERIC(14, 4) NOT NULL,
    slippage_bps NUMERIC(8, 2) NOT NULL DEFAULT 5.0,
    commission NUMERIC(10, 4) NOT NULL DEFAULT 0.0,
    realized_pnl NUMERIC(14, 4) NOT NULL DEFAULT 0.0,
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_trades_ticker ON paper_trades(ticker_symbol);
CREATE INDEX IF NOT EXISTS idx_paper_trades_executed_at ON paper_trades(executed_at);

CREATE TABLE IF NOT EXISTS paper_positions (
    session_id VARCHAR(64) NOT NULL,
    ticker_symbol VARCHAR(16) NOT NULL,
    side VARCHAR(8) NOT NULL,
    quantity NUMERIC(14, 4) NOT NULL,
    average_entry_price NUMERIC(14, 4) NOT NULL,
    current_mark_price NUMERIC(14, 4) NOT NULL,
    unrealized_pnl NUMERIC(14, 4) NOT NULL DEFAULT 0.0,
    unrealized_pnl_pct NUMERIC(8, 4) NOT NULL DEFAULT 0.0,
    stop_loss_price NUMERIC(14, 4),
    take_profit_price NUMERIC(14, 4),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (session_id, ticker_symbol)
);

CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL,
    cash_balance NUMERIC(16, 4) NOT NULL,
    invested_equity NUMERIC(16, 4) NOT NULL,
    total_portfolio_value NUMERIC(16, 4) NOT NULL,
    realized_pnl_cumulative NUMERIC(16, 4) NOT NULL,
    unrealized_pnl_current NUMERIC(16, 4) NOT NULL,
    rolling_sharpe_ratio NUMERIC(8, 4) NOT NULL DEFAULT 0.0,
    max_drawdown_pct NUMERIC(8, 4) NOT NULL DEFAULT 0.0,
    active_positions_count INT NOT NULL DEFAULT 0,
    latency_ms NUMERIC(8, 3) NOT NULL DEFAULT 0.0,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_snapshots_time ON paper_portfolio_snapshots(recorded_at);

-- Pick-5 alpha ensemble config (Page 4 / AlphaSignalServer filter)
CREATE TABLE IF NOT EXISTS alpha_ensemble_config (
    ensemble_id VARCHAR(64) NOT NULL,
    horizon VARCHAR(16) NOT NULL,
    selected_alpha_ids TEXT NOT NULL,
    booksize NUMERIC(16, 2) NOT NULL DEFAULT 100000.0,
    top_k_tickers INT NOT NULL DEFAULT 5,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by VARCHAR(64),
    PRIMARY KEY (ensemble_id, horizon)
);
