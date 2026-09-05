-- ============================================================================
-- PostgreSQL Table & Index Schema: data_group_members
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_group_members (
    group_id UUID NOT NULL,
    user_id UUID NOT NULL,
    status TEXT DEFAULT 'INVITED'::text,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    personal_suffix TEXT,
    can_read_data BOOLEAN DEFAULT false,
    granted_permissions JSONB DEFAULT '[]'::jsonb,
    PRIMARY KEY (group_id, user_id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_group_members_suffix ON public.data_group_members USING btree (group_id, personal_suffix);
