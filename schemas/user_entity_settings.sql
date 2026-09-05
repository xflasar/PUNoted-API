-- ============================================================================
-- PostgreSQL Table & Index Schema: user_entity_settings
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_entity_settings (
    accountid TEXT NOT NULL,
    domain TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    settings JSONB DEFAULT '{}'::jsonb NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid, domain, entity_id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_user_entity_settings_acc_domain ON public.user_entity_settings USING btree (accountid, domain);
