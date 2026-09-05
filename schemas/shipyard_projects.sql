-- ============================================================================
-- PostgreSQL Table & Index Schema: shipyard_projects
-- ============================================================================

CREATE TABLE IF NOT EXISTS shipyard_projects (
    blueprintnaturalid TEXT,
    canstart BOOLEAN,
    creationtimestamp TIMESTAMP WITHOUT TIME ZONE,
    endtimestamp TIMESTAMP WITHOUT TIME ZONE,
    id TEXT NOT NULL,
    originblueprintnaturalid TEXT,
    shipid TEXT,
    shipyardid TEXT,
    starttimestamp TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    PRIMARY KEY (id)
);
