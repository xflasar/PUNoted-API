-- ============================================================================
-- PostgreSQL Table & Index Schema: representation_contributors
-- ============================================================================

CREATE TABLE IF NOT EXISTS representation_contributors (
    amountcontributed INTEGER,
    representationid TEXT,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);
