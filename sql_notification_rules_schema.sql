-- SQL Schema for Per-User Notification Rules & CX Market Watchers

CREATE TABLE IF NOT EXISTS user_notification_rules (
    accountid TEXT PRIMARY KEY,
    fleet_enabled BOOLEAN DEFAULT TRUE,
    health_threshold INT DEFAULT 70,
    storage_enabled BOOLEAN DEFAULT TRUE,
    storage_threshold INT DEFAULT 90,
    production_enabled BOOLEAN DEFAULT TRUE,
    supply_days_threshold DOUBLE PRECISION DEFAULT 1.0,
    contracts_enabled BOOLEAN DEFAULT TRUE,
    cx_enabled BOOLEAN DEFAULT TRUE,
    cx_market_watchers JSONB DEFAULT '[]'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
