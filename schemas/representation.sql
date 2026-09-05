-- ============================================================================
-- PostgreSQL Table & Index Schema: representation
-- ============================================================================

CREATE TABLE IF NOT EXISTS representation (
    contributednextlevelamount INTEGER,
    contributednextlevelcurrency TEXT,
    contributedtotalamount INTEGER,
    contributedtotalcurrency TEXT,
    costnextlevelamount INTEGER,
    costnextlevelcurrency TEXT,
    currentlevel INTEGER,
    leftnextlevelamount INTEGER,
    leftnextlevelcurrency TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    representationid TEXT NOT NULL,
    PRIMARY KEY (representationid)
);
