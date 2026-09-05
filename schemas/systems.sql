-- ============================================================================
-- PostgreSQL Table & Index Schema: systems
-- ============================================================================

CREATE TABLE IF NOT EXISTS systems (
    mass DOUBLE PRECISION DEFAULT '0'::double precision,
    masssol DOUBLE PRECISION DEFAULT '0'::double precision,
    microasteroidcount DOUBLE PRECISION,
    name TEXT,
    naturalid TEXT,
    positionx DOUBLE PRECISION,
    positiony DOUBLE PRECISION,
    positionz DOUBLE PRECISION,
    sectorid TEXT,
    subsectorid TEXT,
    systemid TEXT NOT NULL,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (systemid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_systems_name_trgm ON public.systems USING gin (name gin_trgm_ops);
