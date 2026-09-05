-- ============================================================================
-- PostgreSQL Table & Index Schema: company_data
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_data (
    companycode TEXT,
    companyid TEXT NOT NULL,
    companyname TEXT,
    countryid TEXT,
    headquartersid TEXT,
    ratingreportid TEXT,
    representationid TEXT,
    startinglocationplanetid TEXT,
    startinglocationsystemid TEXT,
    startingprofile TEXT,
    userdataid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (companyid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_company_companycode_trgm ON public.company_data USING gin (companycode gin_trgm_ops);
