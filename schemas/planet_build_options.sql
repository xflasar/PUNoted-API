-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_build_options
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_build_options (
    billofmaterial TEXT,
    id INTEGER DEFAULT nextval('planet_build_options_id_seq'::regclass) NOT NULL,
    planetid TEXT NOT NULL,
    sitetype TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (planetid, sitetype)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_planet_build_options_planetid ON public.planet_build_options USING btree (planetid);
