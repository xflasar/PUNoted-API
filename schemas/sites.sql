-- ============================================================================
-- PostgreSQL Table & Index Schema: sites
-- ============================================================================

CREATE TABLE IF NOT EXISTS sites (
    addressplanetid TEXT,
    addresssystemid TEXT,
    area INTEGER,
    buildingoptions TEXT[],
    foundedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    investedpermits INTEGER,
    maximumpermits INTEGER,
    siteid TEXT NOT NULL,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (siteid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_sites_addressplanetid ON public.sites USING btree (addressplanetid);
CREATE INDEX IF NOT EXISTS idx_sites_user ON public.sites USING btree (userid, siteid);
CREATE INDEX IF NOT EXISTS idx_sites_userid ON public.sites USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_sites_userid_siteid ON public.sites USING btree (userid, siteid);
