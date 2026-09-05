-- ============================================================================
-- PostgreSQL Table & Index Schema: material_recipe_ingredients
-- ============================================================================

CREATE TABLE IF NOT EXISTS material_recipe_ingredients (
    recipe_id VARCHAR(32) NOT NULL,
    material_id VARCHAR(64) NOT NULL,
    material_ticker VARCHAR(10),
    amount NUMERIC NOT NULL,
    type VARCHAR(6) NOT NULL,
    PRIMARY KEY (recipe_id, material_id, type)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_mat_recipe_ing_reverse_lookup ON public.material_recipe_ingredients USING btree (material_ticker, type);
