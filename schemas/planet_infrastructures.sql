-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_infrastructures
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_infrastructures (
    activelevel INTEGER,
    currentlevel INTEGER,
    level INTEGER,
    populationid TEXT NOT NULL,
    projectid TEXT NOT NULL,
    projectname TEXT,
    ticker TEXT,
    type TEXT NOT NULL,
    upgradestatus INTEGER,
    upkeepstatus INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (populationid, type, projectid)
);
