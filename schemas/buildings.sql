-- ============================================================================
-- PostgreSQL Table & Index Schema: buildings
-- ============================================================================

CREATE TABLE IF NOT EXISTS buildings (
    area INTEGER,
    buildingid TEXT NOT NULL,
    expertisecategory TEXT,
    name TEXT,
    needsfertilesoil BOOLEAN,
    ticker TEXT,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (buildingid)
);
