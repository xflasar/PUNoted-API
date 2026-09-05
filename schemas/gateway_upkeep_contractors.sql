-- ============================================================================
-- PostgreSQL Table & Index Schema: gateway_upkeep_contractors
-- ============================================================================

CREATE TABLE IF NOT EXISTS gateway_upkeep_contractors (
    gateway_id TEXT NOT NULL,
    phase_index INTEGER NOT NULL,
    contractor_id TEXT,
    contractor_code TEXT,
    contractor_name TEXT,
    contract_id TEXT NOT NULL,
    PRIMARY KEY (gateway_id, phase_index, contract_id)
);
