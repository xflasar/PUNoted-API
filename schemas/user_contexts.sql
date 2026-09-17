-- ============================================================================
-- PostgreSQL Table & Index Schema: user_contexts
-- Stores available entity contexts for users (COMPANY, GOVERNMENT, etc.)
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_contexts (
    userid TEXT NOT NULL,
    contextid TEXT NOT NULL,
    type TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    action_roles JSONB,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (userid, contextid)
);

CREATE INDEX IF NOT EXISTS idx_user_contexts_userid ON public.user_contexts USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_user_contexts_type ON public.user_contexts USING btree (type);
