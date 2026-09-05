-- ============================================================================
-- PostgreSQL Table & Index Schema: site_production_lines
-- ============================================================================

CREATE TABLE IF NOT EXISTS site_production_lines (
    capacity INTEGER,
    condition DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    productionlineid TEXT NOT NULL,
    siteid TEXT,
    slots INTEGER,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (productionlineid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_prod_lines_siteid ON public.site_production_lines USING btree (siteid);
CREATE INDEX IF NOT EXISTS idx_site_prod_lines_siteid_productionlineid ON public.site_production_lines USING btree (siteid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_spl_site ON public.site_production_lines USING btree (siteid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_spl_site_line ON public.site_production_lines USING btree (siteid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_spl_siteid ON public.site_production_lines USING btree (siteid);
