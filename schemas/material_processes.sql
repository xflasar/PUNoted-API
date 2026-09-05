-- ============================================================================
-- PostgreSQL Table & Index Schema: material_processes
-- ============================================================================

CREATE TABLE IF NOT EXISTS material_processes (
    processid UUID DEFAULT gen_random_uuid() NOT NULL,
    reactorid TEXT NOT NULL,
    durationmillis BIGINT,
    processtype TEXT,
    PRIMARY KEY (processid)
);
