-- ============================================================================
-- PostgreSQL Table & Index Schema: corporation_project_bill_of_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporation_project_bill_of_materials (
    amount INTEGER,
    currentamount INTEGER,
    id INTEGER DEFAULT nextval('corporation_project_bill_of_materials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    projectid TEXT,
    PRIMARY KEY (id)
);
