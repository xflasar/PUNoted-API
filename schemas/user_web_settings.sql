-- ============================================================================
-- PostgreSQL Table & Index Schema: user_web_settings
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_web_settings (
    user_id UUID NOT NULL,
    page_context TEXT NOT NULL,
    preferences JSONB DEFAULT '{}'::jsonb NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (user_id, page_context)
);
