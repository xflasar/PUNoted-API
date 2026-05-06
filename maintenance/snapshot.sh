#!/bin/bash
set -e

# Define your source table names here to avoid typos
MARKET_TABLE="cx_brokers"
CURRENCY_TABLE="user_currency_accounts" 

echo "[$(date)] Starting Daily Snapshots..."

# ==============================================================================
# 1. MARKETPLACE HISTORY
# ==============================================================================
echo "Processing Marketplace History..."

psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "
DO \$\$ 
BEGIN 
    -- Create History Table if it doesn't exist
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'cx_brokers_history') THEN 
        CREATE TABLE cx_brokers_history (LIKE $MARKET_TABLE);
        -- Remove constraints that would block duplicate historical rows
        ALTER TABLE cx_brokers_history DROP CONSTRAINT IF EXISTS marketplace_data_pkey; 
        ALTER TABLE cx_brokers_history ADD COLUMN snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_DATE - INTERVAL '1 second');
        CREATE INDEX idx_cx_brokers_history_date ON cx_brokers_history(snapshot_at);
    END IF; 
END \$\$;"

# Execute Snapshot
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "
    INSERT INTO cx_brokers_history 
    SELECT *, (CURRENT_DATE - INTERVAL '1 second')
    FROM $MARKET_TABLE;
"

# ==============================================================================
# 2. USER CURRENCY HISTORY (Money at End of Day)
# ==============================================================================
echo "Processing User Currency History..."

psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "
DO \$\$ 
BEGIN 
    -- Create History Table if it doesn't exist
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'user_currency_accounts_history') THEN 
        CREATE TABLE user_currency_accounts_history (LIKE $CURRENCY_TABLE);
        
        -- Remove Primary Key 
        -- We must allow the same user/currency combination to appear every day
        ALTER TABLE user_currency_accounts_history DROP CONSTRAINT IF EXISTS user_currency_accounts_pkey;
        
        -- Add Timestamp
        ALTER TABLE user_currency_accounts_history ADD COLUMN snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
        
        -- Indexes for performance (Filtering by Date or by User)
        CREATE INDEX idx_currency_history_date ON user_currency_accounts_history(snapshot_at);
        CREATE INDEX idx_currency_history_user ON user_currency_accounts_history(userid); 
    END IF; 
END \$\$;"

# Execute Snapshot
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "
    INSERT INTO user_currency_accounts_history 
    SELECT *, NOW() 
    FROM $CURRENCY_TABLE;
"

echo "[$(date)] All Snapshots completed successfully."