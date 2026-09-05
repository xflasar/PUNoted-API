-- ============================================================================
-- PostgreSQL Table & Index Schema: user_api_tokens
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_api_tokens (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    user_id UUID NOT NULL,
    token_hash TEXT NOT NULL,
    token_prefix TEXT NOT NULL,
    label TEXT NOT NULL,
    description TEXT,
    permissions JSONB DEFAULT '[]'::jsonb NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    last_used_at TIMESTAMP WITH TIME ZONE,
    group_id UUID,
    allow_group_access BOOLEAN DEFAULT false,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS user_api_tokens_token_hash_key ON public.user_api_tokens USING btree (token_hash);
CREATE INDEX IF NOT EXISTS idx_api_tokens_hash ON public.user_api_tokens USING btree (token_hash);
CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON public.user_api_tokens USING btree (user_id);
