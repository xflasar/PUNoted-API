-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_production_fees
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_production_fees (
    category TEXT,
    feeamount DOUBLE PRECISION,
    feecurrency TEXT,
    id INTEGER DEFAULT nextval('planet_production_fees_id_seq'::regclass) NOT NULL,
    planetid TEXT,
    workforcelevel TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS unique_ppf ON public.planet_production_fees USING btree (category, planetid, workforcelevel);
CREATE INDEX IF NOT EXISTS idx_planet_production_fees_planetid ON public.planet_production_fees USING btree (planetid);
