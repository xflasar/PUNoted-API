-- ============================================================================
-- PostgreSQL Table & Index Schema: blueprint_components_modifiers
-- ============================================================================

CREATE TABLE IF NOT EXISTS blueprint_components_modifiers (
    componentid TEXT,
    id INTEGER DEFAULT nextval('blueprint_components_modifiers_id_seq'::regclass) NOT NULL,
    type TEXT,
    value DOUBLE PRECISION,
    PRIMARY KEY (id)
);
