-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_projects
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_projects (
    entityid TEXT,
    planetid TEXT,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);
