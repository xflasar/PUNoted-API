-- ============================================================================
-- PostgreSQL Table & Index Schema: building_build_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS building_build_materials (
    amount INTEGER,
    buildingid TEXT,
    materialid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS building_build_materials_id ON public.building_build_materials USING btree (buildingid, materialid);
CREATE INDEX IF NOT EXISTS idx_bbm_buildingid ON public.building_build_materials USING btree (buildingid);
