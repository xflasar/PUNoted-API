-- ============================================================================
-- PostgreSQL Table & Index Schema: platform_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS platform_materials (
    amount INTEGER,
    materialid TEXT,
    materialtype TEXT,
    platformid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS platform_materials_id ON public.platform_materials USING btree (materialid, platformid, materialtype);
CREATE INDEX IF NOT EXISTS idx_pm_platformid ON public.platform_materials USING btree (platformid);
