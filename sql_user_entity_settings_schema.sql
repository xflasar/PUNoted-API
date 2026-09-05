-- SQL Schema for Universal Entity & Page Settings Engine

CREATE TABLE IF NOT EXISTS user_entity_settings (
    accountid TEXT NOT NULL,
    domain TEXT NOT NULL,      -- 'site', 'ship', 'cx', 'contract', 'storage', 'page'
    entity_id TEXT NOT NULL,   -- e.g. siteid, shipid, 'RAT_ICA', contractid, or 'GLOBAL'
    settings JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid, domain, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_user_entity_settings_acc_domain ON user_entity_settings (accountid, domain);
