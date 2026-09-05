-- ============================================================================
-- PostgreSQL Table & Index Schema: stations
-- ============================================================================

CREATE TABLE IF NOT EXISTS stations (
    comexid TEXT,
    commissioningtime TIMESTAMP WITHOUT TIME ZONE,
    countryid TEXT,
    governingentityid TEXT,
    localmarketid TEXT,
    name TEXT,
    naturalid TEXT,
    stationid TEXT NOT NULL,
    systemid TEXT,
    warehouseid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    orbit JSONB,
    PRIMARY KEY (stationid)
);
