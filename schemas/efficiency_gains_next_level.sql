-- ============================================================================
-- PostgreSQL Table & Index Schema: efficiency_gains_next_level
-- ============================================================================

CREATE TABLE IF NOT EXISTS efficiency_gains_next_level (
    category TEXT,
    gain DOUBLE PRECISION,
    headquartersid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);
