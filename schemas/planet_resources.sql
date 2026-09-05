-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_resources
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_resources (
    factor DOUBLE PRECISION,
    id INTEGER DEFAULT nextval('planet_resources_id_seq'::regclass) NOT NULL,
    materialid TEXT NOT NULL,
    planetid TEXT NOT NULL,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (planetid, materialid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_planet_resources_planetid ON public.planet_resources USING btree (planetid);
