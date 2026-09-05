-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_physical_data
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_physical_data (
    gravity DOUBLE PRECISION,
    id INTEGER DEFAULT nextval('planet_physical_data_id_seq'::regclass) NOT NULL,
    magneticfield DOUBLE PRECISION,
    mass DOUBLE PRECISION,
    massearth DOUBLE PRECISION,
    planetid TEXT,
    pressure DOUBLE PRECISION,
    radiation DOUBLE PRECISION,
    radius DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    fertility DOUBLE PRECISION,
    surface BOOLEAN,
    sunlight DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_planet_physical_data_planetid ON public.planet_physical_data USING btree (planetid);
