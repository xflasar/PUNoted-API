-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_blueprints_component_options
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_blueprints_component_options (
    id TEXT NOT NULL,
    materialname TEXT,
    option TEXT,
    type TEXT,
    PRIMARY KEY (id)
);
