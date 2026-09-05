-- ============================================================================
-- PostgreSQL Table & Index Schema: corporation_projects
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporation_projects (
    completiondate TIMESTAMP WITHOUT TIME ZONE,
    corporationid TEXT,
    id TEXT NOT NULL,
    naturalid TEXT,
    planetid TEXT,
    systemid TEXT,
    type TEXT,
    PRIMARY KEY (id)
);
