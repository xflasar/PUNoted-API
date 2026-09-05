-- ============================================================================
-- PostgreSQL Table & Index Schema: production_groups
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_groups (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    name VARCHAR(255) NOT NULL,
    owner_id UUID NOT NULL,
    chain_data JSONB DEFAULT '{}'::jsonb NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_groups_owner_id ON public.production_groups USING btree (owner_id);
CREATE INDEX IF NOT EXISTS idx_groups_updated_at ON public.production_groups USING btree (updated_at);
