-- ============================================================================
-- PostgreSQL Table & Index Schema: site_production_line_orders
-- ============================================================================

CREATE TABLE IF NOT EXISTS site_production_line_orders (
    completed BOOLEAN,
    completion TIMESTAMP WITHOUT TIME ZONE,
    created TIMESTAMP WITHOUT TIME ZONE,
    duration BIGINT,
    halted BOOLEAN,
    lastupdated TIMESTAMP WITHOUT TIME ZONE,
    orderid TEXT NOT NULL,
    productionfeeamount DOUBLE PRECISION,
    productionfeecurrency TEXT,
    productionlineid TEXT,
    recipeid TEXT,
    recurring BOOLEAN,
    started TIMESTAMP WITHOUT TIME ZONE,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (orderid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_orders_line_completion ON public.site_production_line_orders USING btree (productionlineid, completion);
CREATE INDEX IF NOT EXISTS idx_site_prod_line_orders_productionlineid_started_orderid ON public.site_production_line_orders USING btree (productionlineid, started, orderid);
CREATE INDEX IF NOT EXISTS idx_splo_active ON public.site_production_line_orders USING btree (productionlineid, orderid) WHERE (started IS NULL);
CREATE INDEX IF NOT EXISTS idx_splo_lineid_started ON public.site_production_line_orders USING btree (productionlineid) WHERE (started IS NULL);
