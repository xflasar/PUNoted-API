-- ============================================================================
-- PostgreSQL Table & Index Schema: site_experts
-- ============================================================================

CREATE TABLE IF NOT EXISTS site_experts (
    available INTEGER,
    category TEXT,
    current INTEGER,
    efficiencygain DOUBLE PRECISION,
    elimit INTEGER,
    id TEXT NOT NULL,
    progress DOUBLE PRECISION,
    siteid TEXT,
    PRIMARY KEY (id)
);
