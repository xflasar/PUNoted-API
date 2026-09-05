-- ============================================================================
-- PostgreSQL Table & Index Schema: production_recipes
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_recipes (
    duration BIGINT,
    efficiency DOUBLE PRECISION,
    effortfactor DOUBLE PRECISION,
    experience INTEGER,
    name TEXT NOT NULL,
    productionfee DOUBLE PRECISION,
    productionfeecurrency TEXT,
    productiontemplateid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    materialid TEXT,
    productionlineid TEXT NOT NULL,
    PRIMARY KEY (productionlineid, productiontemplateid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_pr_valid ON public.production_recipes USING btree (productiontemplateid, productionlineid) WHERE (duration > 0);
CREATE INDEX IF NOT EXISTS idx_production_recipes_templateid_lineid ON public.production_recipes USING btree (productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_recipes_composite ON public.production_recipes USING btree (productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_recipes_template_id_text ON public.production_recipes USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_recipes_templateid ON public.production_recipes USING btree (productiontemplateid);
