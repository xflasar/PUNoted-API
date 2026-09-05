-- ============================================================================
-- PostgreSQL Table & Index Schema: sectors
-- ============================================================================

CREATE TABLE IF NOT EXISTS sectors (
    externalsectorid TEXT NOT NULL,
    hexq INTEGER,
    hexr INTEGER,
    hexs INTEGER,
    name TEXT,
    size INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (externalsectorid)
);
