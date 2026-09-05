-- ============================================================================
-- PostgreSQL Table & Index Schema: gateway_upkeep_requirements
-- ============================================================================

CREATE TABLE IF NOT EXISTS gateway_upkeep_requirements (
    gateway_id TEXT NOT NULL,
    material_id TEXT NOT NULL,
    material_ticker TEXT,
    material_name TEXT,
    amount_current DOUBLE PRECISION,
    amount_required DOUBLE PRECISION,
    PRIMARY KEY (gateway_id, material_id)
);
