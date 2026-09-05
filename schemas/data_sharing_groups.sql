-- ============================================================================
-- PostgreSQL Table & Index Schema: data_sharing_groups
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_sharing_groups (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    owner_id UUID NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    access_key TEXT,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS data_sharing_groups_access_key_key ON public.data_sharing_groups USING btree (access_key);
CREATE INDEX IF NOT EXISTS idx_group_access_key ON public.data_sharing_groups USING btree (access_key);
