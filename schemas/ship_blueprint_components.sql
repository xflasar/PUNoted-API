-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_blueprint_components
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_blueprint_components (
    amount INTEGER,
    blueprintid TEXT,
    cardinality TEXT,
    id TEXT NOT NULL,
    option TEXT,
    optionmaterialid TEXT,
    type TEXT,
    user_id TEXT,
    PRIMARY KEY (id)
);
