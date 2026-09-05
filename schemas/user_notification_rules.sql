-- ============================================================================
-- PostgreSQL Table & Index Schema: user_notification_rules
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_notification_rules (
    accountid TEXT NOT NULL,
    fleet_enabled BOOLEAN DEFAULT true,
    health_threshold INTEGER DEFAULT 70,
    storage_enabled BOOLEAN DEFAULT true,
    storage_threshold INTEGER DEFAULT 90,
    production_enabled BOOLEAN DEFAULT true,
    supply_days_threshold DOUBLE PRECISION DEFAULT 1.0,
    contracts_enabled BOOLEAN DEFAULT true,
    cx_enabled BOOLEAN DEFAULT true,
    cx_market_watchers JSONB DEFAULT '[]'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid)
);
