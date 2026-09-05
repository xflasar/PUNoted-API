-- ============================================================================
-- PostgreSQL Table & Index Schema: headquarters_upgrade_items
-- ============================================================================

CREATE TABLE IF NOT EXISTS headquarters_upgrade_items (
    amount INTEGER,
    amountlimit INTEGER,
    headquartersid TEXT,
    materialid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);
