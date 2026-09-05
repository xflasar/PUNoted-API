-- ============================================================================
-- PostgreSQL Table & Index Schema: production_line_orders
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_line_orders (
    completed DOUBLE PRECISION,
    completiontimestamp TIMESTAMP WITHOUT TIME ZONE,
    createdtimestamp TIMESTAMP WITHOUT TIME ZONE,
    durationmillis INTEGER,
    halted BOOLEAN,
    id TEXT NOT NULL,
    lastupdatedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    productionfeeamount DOUBLE PRECISION,
    productionfeecurrency TEXT,
    productionlineid TEXT,
    recipeid TEXT,
    recurring BOOLEAN,
    startedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);
