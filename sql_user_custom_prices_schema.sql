-- SQL Schema for User Custom Pricing Sheets
-- Enables custom material price overrides per user account

CREATE TABLE IF NOT EXISTS user_custom_prices (
    accountid TEXT NOT NULL,
    ticker VARCHAR(32) NOT NULL,
    price NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid, ticker)
);

CREATE INDEX IF NOT EXISTS idx_user_custom_prices_acc ON user_custom_prices (accountid);
