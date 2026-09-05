-- ============================================================================
-- PostgreSQL Table & Index Schema: production_lines
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_lines (
    addressplanetid TEXT,
    addresssystemid TEXT,
    capacity INTEGER,
    condition DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    id TEXT NOT NULL,
    siteid TEXT,
    slots INTEGER,
    type TEXT,
    PRIMARY KEY (id)
);
