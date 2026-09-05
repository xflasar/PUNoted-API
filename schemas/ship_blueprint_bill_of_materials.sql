-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_blueprint_bill_of_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_blueprint_bill_of_materials (
    amount INTEGER,
    blueprintid TEXT,
    id INTEGER DEFAULT nextval('ship_blueprint_bill_of_materials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    user_id TEXT,
    PRIMARY KEY (id)
);
