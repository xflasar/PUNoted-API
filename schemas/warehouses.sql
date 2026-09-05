-- ============================================================================
-- PostgreSQL Table & Index Schema: warehouses
-- ============================================================================

CREATE TABLE IF NOT EXISTS warehouses (
    addressplanet TEXT,
    addresssystem TEXT,
    feeamount INTEGER,
    feecurrency TEXT,
    nextpayment TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    storeid TEXT NOT NULL,
    units INTEGER,
    userid TEXT,
    volumecapacity INTEGER,
    warehouseid TEXT NOT NULL,
    weightcapacity INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (warehouseid, storeid)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS warehouses_warehouseid_storeid_key ON public.warehouses USING btree (warehouseid, storeid);
