-- ============================================================================
-- PostgreSQL Table & Index Schema: recipes
-- ============================================================================

CREATE TABLE IF NOT EXISTS recipes (
    materialid TEXT NOT NULL,
    input_recipe_ids JSONB DEFAULT '[]'::jsonb,
    output_recipe_ids JSONB DEFAULT '[]'::jsonb,
    PRIMARY KEY (materialid)
);
