-- ============================================================================
-- PostgreSQL Table & Index Schema: group_members
-- ============================================================================

CREATE TABLE IF NOT EXISTS group_members (
    group_id UUID NOT NULL,
    user_id UUID NOT NULL,
    role VARCHAR(20) NOT NULL,
    joined_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_group_members_user ON public.group_members USING btree (user_id);
CREATE INDEX IF NOT EXISTS idx_members_user_id ON public.group_members USING btree (user_id);
