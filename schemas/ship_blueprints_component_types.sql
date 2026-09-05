-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_blueprints_component_types
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_blueprints_component_types (
    cardinality TEXT,
    id TEXT NOT NULL,
    selectable BOOLEAN,
    type TEXT,
    PRIMARY KEY (id)
);
