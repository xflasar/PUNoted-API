-- Production Database Performance Optimization SQL Script for CX Queries
-- Execute this SQL script directly on your PostgreSQL database!

-- 1. Index for fast ticker history queries by ticker and timestamp
CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_ticker_snap 
ON cx_brokers_history (ticker, snapshot_at DESC);

-- 2. Index for case-insensitive ticker queries
CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_upper_ticker 
ON cx_brokers_history (UPPER(ticker));

-- 3. Composite Index for fast timeframe stability matrix aggregation queries
CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_snap_ticker 
ON cx_brokers_history (snapshot_at DESC, ticker);

-- 4. Index on snapshot_at alone for fast max snapshot timestamp resolution
CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_snapshot_at 
ON cx_brokers_history (snapshot_at DESC);

-- Analyze tables to update PostgreSQL query planner statistics immediately
ANALYZE cx_brokers_history;
