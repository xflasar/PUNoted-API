-- ============================================================================
-- PostgreSQL Table & Index Schema: planets
-- ============================================================================

CREATE TABLE IF NOT EXISTS planets (
    admincenterid TEXT,
    countrycode TEXT,
    countryname TEXT,
    fertility DOUBLE PRECISION,
    mass DOUBLE PRECISION DEFAULT '0'::double precision,
    name TEXT,
    nameable BOOLEAN,
    namer TEXT,
    namingdate TIMESTAMP WITHOUT TIME ZONE,
    naturalid TEXT,
    planetid TEXT NOT NULL,
    plots INTEGER,
    populationid TEXT,
    sunlight DOUBLE PRECISION,
    surface BOOLEAN,
    systemid TEXT,
    temperature DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    cogc TEXT,
    PRIMARY KEY (planetid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_planets_name_trgm ON public.planets USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_planets_naturalid_trgm ON public.planets USING gin (naturalid gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_planets_planetid ON public.planets USING btree (planetid);
CREATE INDEX IF NOT EXISTS idx_planets_populationid ON public.planets USING btree (populationid);
