-- ============================================================================
-- PostgreSQL Table & Index Schema: gateway_upkeep_phases
-- ============================================================================

CREATE TABLE IF NOT EXISTS gateway_upkeep_phases (
    id TEXT NOT NULL,
    gateway_id TEXT,
    natural_id INTEGER,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    service_level DOUBLE PRECISION,
    materials_json JSONB,
    PRIMARY KEY (id)
);
