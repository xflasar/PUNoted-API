-- ============================================================================
-- PostgreSQL Table & Index Schema: planetbuildoptionmaterials
-- ============================================================================

CREATE TABLE IF NOT EXISTS planetbuildoptionmaterials (
    amount INTEGER,
    id INTEGER DEFAULT nextval('planetbuildoptionmaterials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    planetid TEXT,
    sitetype TEXT,
    PRIMARY KEY (id)
);
