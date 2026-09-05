-- ============================================================================
-- PostgreSQL Table & Index Schema: workforce_needs
-- ============================================================================

CREATE TABLE IF NOT EXISTS workforce_needs (
    category TEXT,
    essential BOOLEAN,
    materialid TEXT,
    satisfaction DOUBLE PRECISION,
    unitsper100 DOUBLE PRECISION,
    unitsperinterval DOUBLE PRECISION,
    workforceid TEXT NOT NULL,
    workforceneedid TEXT NOT NULL,
    PRIMARY KEY (workforceneedid)
);
