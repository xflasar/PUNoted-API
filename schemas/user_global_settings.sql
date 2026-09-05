-- ============================================================================
-- PostgreSQL Table & Index Schema: user_global_settings
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_global_settings (
    userid TEXT NOT NULL,
    default_cx_code TEXT DEFAULT 'IC1'::text,
    default_currency TEXT DEFAULT 'ICA'::text,
    internal_excluded_sites JSONB DEFAULT '[]'::jsonb,
    internal_leased_sites JSONB DEFAULT '[]'::jsonb,
    privacy_settings JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (userid)
);
