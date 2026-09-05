-- ============================================================================
-- PostgreSQL Table & Index Schema: population_available_reserve_workforce
-- ============================================================================

CREATE TABLE IF NOT EXISTS population_available_reserve_workforce (
    siteid TEXT NOT NULL,
    workforceamountengineer INTEGER,
    workforceamountpioneer INTEGER,
    workforceamountscientist INTEGER,
    workforceamountsettler INTEGER,
    workforceamounttechnician INTEGER,
    PRIMARY KEY (siteid)
);
