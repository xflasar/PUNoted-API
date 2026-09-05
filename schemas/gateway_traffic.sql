-- ============================================================================
-- PostgreSQL Table & Index Schema: gateway_traffic
-- ============================================================================

CREATE TABLE IF NOT EXISTS gateway_traffic (
    gateway_id TEXT NOT NULL,
    total_jumps INTEGER,
    current_phase_jumps INTEGER,
    current_phase_inbound INTEGER,
    current_phase_start TIMESTAMP WITHOUT TIME ZONE,
    current_phase_end TIMESTAMP WITHOUT TIME ZONE,
    avg_jumps DOUBLE PRECISION,
    avg_inbound DOUBLE PRECISION,
    raw_current_phase JSONB,
    raw_last_phase JSONB,
    raw_averages JSONB,
    PRIMARY KEY (gateway_id)
);
