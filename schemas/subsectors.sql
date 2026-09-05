-- ============================================================================
-- PostgreSQL Table & Index Schema: subsectors
-- ============================================================================

CREATE TABLE IF NOT EXISTS subsectors (
    externalsectorid TEXT NOT NULL,
    externalsubsectorid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (externalsubsectorid)
);
