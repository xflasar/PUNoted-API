-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_production
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_production (
    completed BOOLEAN,
    corpmember BOOLEAN,
    notes TEXT,
    ordercompleted TIMESTAMP WITHOUT TIME ZONE,
    orderdate TIMESTAMP WITHOUT TIME ZONE,
    orderid INTEGER DEFAULT nextval('ship_production_orderid_seq'::regclass) NOT NULL,
    orderwaittime INTEGER,
    price INTEGER,
    shiptype TEXT,
    username TEXT,
    position INTEGER,
    PRIMARY KEY (orderid)
);
