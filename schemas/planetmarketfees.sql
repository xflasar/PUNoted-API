-- ============================================================================
-- PostgreSQL Table & Index Schema: planetmarketfees
-- ============================================================================

CREATE TABLE IF NOT EXISTS planetmarketfees (
    id INTEGER DEFAULT nextval('planetmarketfees_id_seq'::regclass) NOT NULL,
    localmarketfeebase INTEGER,
    localmarketfeetimefactor INTEGER,
    planetid TEXT,
    productionfeelimitfactors TEXT,
    siteestablishmentfee INTEGER,
    warehousefee INTEGER,
    PRIMARY KEY (id)
);
