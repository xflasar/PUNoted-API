-- ============================================================================
-- PostgreSQL Table & Index Schema: materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS materials (
    category TEXT,
    materialid TEXT NOT NULL,
    name TEXT,
    resource BOOLEAN,
    ticker TEXT,
    volume DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (materialid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_materials_materialid ON public.materials USING btree (materialid);
CREATE INDEX IF NOT EXISTS idx_materials_matid ON public.materials USING btree (materialid);
CREATE INDEX IF NOT EXISTS idx_materials_ticker ON public.materials USING btree (ticker);
