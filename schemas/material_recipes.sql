-- ============================================================================
-- PostgreSQL Table & Index Schema: material_recipes
-- ============================================================================

CREATE TABLE IF NOT EXISTS material_recipes (
    id VARCHAR(32) NOT NULL,
    reactor_id VARCHAR(64) NOT NULL,
    duration_ms BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_mat_recipes_reactor ON public.material_recipes USING btree (reactor_id);
