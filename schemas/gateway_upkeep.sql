-- ============================================================================
-- PostgreSQL Table & Index Schema: gateway_upkeep
-- ============================================================================

CREATE TABLE IF NOT EXISTS gateway_upkeep (
    gateway_id TEXT NOT NULL,
    average_uptime DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (gateway_id)
);
