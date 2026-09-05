-- ============================================================================
-- PostgreSQL Table & Index Schema: site_available_reserve_populations
-- ============================================================================

CREATE TABLE IF NOT EXISTS site_available_reserve_populations (
    engineer INTEGER DEFAULT 0 NOT NULL,
    pioneer INTEGER DEFAULT 0 NOT NULL,
    planetid TEXT NOT NULL,
    scientist INTEGER DEFAULT 0 NOT NULL,
    settler INTEGER DEFAULT 0 NOT NULL,
    siteid TEXT NOT NULL,
    technician INTEGER DEFAULT 0 NOT NULL
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS site_available_reserve_populations_id ON public.site_available_reserve_populations USING btree (planetid, siteid);
