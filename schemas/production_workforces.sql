-- ============================================================================
-- PostgreSQL Table & Index Schema: production_workforces
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_workforces (
    efficiency DOUBLE PRECISION,
    productionlineid TEXT,
    workforcelevel TEXT
);
