-- ============================================================================
-- PostgreSQL Table & Index Schema: user_vendors
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_vendors (
    companycode TEXT,
    companyname TEXT NOT NULL,
    corpname TEXT,
    cx TEXT,
    gamename TEXT,
    isactive BOOLEAN DEFAULT true,
    userid TEXT NOT NULL,
    vendorid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (vendorid)
);
