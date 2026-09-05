-- ============================================================================
-- PostgreSQL Table & Index Schema: material_categories
-- ============================================================================

CREATE TABLE IF NOT EXISTS material_categories (
    id TEXT NOT NULL,
    name TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);
