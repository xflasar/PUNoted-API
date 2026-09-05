-- ============================================================================
-- PostgreSQL Table & Index Schema: site_platforms
-- ============================================================================

CREATE TABLE IF NOT EXISTS site_platforms (
    area INTEGER,
    bookvalueamount DOUBLE PRECISION,
    bookvaluecurrency TEXT,
    buildingid TEXT,
    condition DOUBLE PRECISION,
    creationtime TIMESTAMP WITHOUT TIME ZONE,
    lastrepair TIMESTAMP WITHOUT TIME ZONE,
    platformid TEXT NOT NULL,
    siteid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (platformid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_site_platforms_buildingid ON public.site_platforms USING btree (buildingid);
CREATE INDEX IF NOT EXISTS idx_site_platforms_site_created ON public.site_platforms USING btree (siteid, creationtime DESC);
