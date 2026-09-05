-- ============================================================================
-- PostgreSQL Table & Index Schema: shipyard_project_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS shipyard_project_materials (
    amount INTEGER,
    amountlimit INTEGER,
    id INTEGER DEFAULT nextval('shipyard_project_materials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    projectid TEXT,
    PRIMARY KEY (id)
);
