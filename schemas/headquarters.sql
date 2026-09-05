-- ============================================================================
-- PostgreSQL Table & Index Schema: headquarters
-- ============================================================================

CREATE TABLE IF NOT EXISTS headquarters (
    additionalbasepermits INTEGER,
    additionalproductionqueueslots INTEGER,
    addressplanetid TEXT,
    addresssystemid TEXT,
    basepermits INTEGER,
    headquarterslevel INTEGER,
    headquartersnextupgradeid TEXT,
    nextrelocationtime TIMESTAMP WITHOUT TIME ZONE,
    relocationlocked BOOLEAN,
    usedbasepermits INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    headquartersid UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (headquartersid)
);
