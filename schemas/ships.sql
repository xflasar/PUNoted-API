-- ============================================================================
-- PostgreSQL Table & Index Schema: ships
-- ============================================================================

CREATE TABLE IF NOT EXISTS ships (
    acceleration DOUBLE PRECISION,
    addressplanetid TEXT,
    addressstationid TEXT,
    addresssystemid TEXT,
    blueprintnaturalid TEXT,
    commissioningtime TIMESTAMP WITHOUT TIME ZONE,
    condition DOUBLE PRECISION,
    emitterpower INTEGER,
    flightid TEXT,
    idftlfuelstore TEXT,
    idshipstore TEXT,
    idstlfuelstore TEXT,
    lastrepair TIMESTAMP WITHOUT TIME ZONE,
    mass DOUBLE PRECISION,
    name TEXT,
    operatingemptymass DOUBLE PRECISION,
    operatingtimeftl BIGINT,
    operatingtimestl BIGINT,
    reactorpower INTEGER,
    registration TEXT,
    shipid TEXT NOT NULL,
    status TEXT,
    stlfuelflowrate DOUBLE PRECISION,
    thrust INTEGER,
    userid TEXT,
    volume INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    type TEXT,
    PRIMARY KEY (shipid)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS ships_id ON public.ships USING btree (shipid);
CREATE INDEX IF NOT EXISTS idx_ships_addressplanetid ON public.ships USING btree (addressplanetid);
CREATE INDEX IF NOT EXISTS idx_ships_addressstationid ON public.ships USING btree (addressstationid);
CREATE INDEX IF NOT EXISTS idx_ships_addresssystemid ON public.ships USING btree (addresssystemid);
CREATE INDEX IF NOT EXISTS idx_ships_shipid_userid ON public.ships USING btree (shipid, userid);
