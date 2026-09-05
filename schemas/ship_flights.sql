-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_flights
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_flights (
    aborted BOOLEAN,
    arrivaltimestamp TIMESTAMP WITHOUT TIME ZONE,
    departuretimestamp TIMESTAMP WITHOUT TIME ZONE,
    destinationplanetid TEXT,
    destinationstationid TEXT,
    destinationsystemid TEXT,
    ftldistance DOUBLE PRECISION,
    ftltotalconsumption INTEGER,
    id TEXT NOT NULL,
    originplanetid TEXT,
    originstationid TEXT,
    originsystemid TEXT,
    shipid TEXT NOT NULL,
    stldistance DOUBLE PRECISION,
    stltotalconsumption INTEGER,
    damage DOUBLE PRECISION,
    currentsegmentindex INTEGER,
    userid TEXT NOT NULL,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS ship_flights_unique ON public.ship_flights USING btree (id, shipid, userid);
