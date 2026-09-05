-- ============================================================================
-- PostgreSQL Table & Index Schema: corporation_project_bill_contributions
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporation_project_bill_contributions (
    amount INTEGER,
    id INTEGER DEFAULT nextval('corporation_project_bill_contributions_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    projectid TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    userid TEXT,
    PRIMARY KEY (id)
);
