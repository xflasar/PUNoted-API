-- ============================================================================
-- PostgreSQL Table & Index Schema: production_recipe_input_factors
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_recipe_input_factors (
    factor DOUBLE PRECISION NOT NULL,
    id INTEGER DEFAULT nextval('production_recipe_input_factors_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    productiontemplateid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    productionlineid TEXT NOT NULL,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS production_recipe_input_factors_id ON public.production_recipe_input_factors USING btree (materialid, productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_input_factors_template_text ON public.production_recipe_input_factors USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_inputs_composite ON public.production_recipe_input_factors USING btree (productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_inputs_templateid ON public.production_recipe_input_factors USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_prif_group ON public.production_recipe_input_factors USING btree (productiontemplateid, productionlineid, materialid);
CREATE INDEX IF NOT EXISTS idx_prif_templateid_lineid_materialid ON public.production_recipe_input_factors USING btree (productiontemplateid, productionlineid, materialid);
CREATE INDEX IF NOT EXISTS idx_recipe_inputs_template ON public.production_recipe_input_factors USING btree (productiontemplateid);
