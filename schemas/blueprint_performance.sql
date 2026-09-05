-- ============================================================================
-- PostgreSQL Table & Index Schema: blueprint_performance
-- ============================================================================

CREATE TABLE IF NOT EXISTS blueprint_performance (
    acceleration DOUBLE PRECISION,
    accelerationmax DOUBLE PRECISION,
    blueprintid TEXT,
    emitterchargetime DOUBLE PRECISION,
    fltfuelcapacity DOUBLE PRECISION,
    fltmaxspeed DOUBLE PRECISION,
    id TEXT NOT NULL,
    maxgfactor INTEGER,
    maxoverchargetime DOUBLE PRECISION,
    minreactorusage DOUBLE PRECISION,
    operatingemptymass DOUBLE PRECISION,
    stlfuelcapacity DOUBLE PRECISION,
    storecapacitymass DOUBLE PRECISION,
    storecapacityvolume DOUBLE PRECISION,
    totalvolume DOUBLE PRECISION,
    PRIMARY KEY (id)
);
