-- ============================================================================
-- PostgreSQL Table & Index Schema: process_material_io
-- ============================================================================

CREATE TABLE IF NOT EXISTS process_material_io (
    processid UUID NOT NULL,
    materialid TEXT NOT NULL,
    iotype TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    PRIMARY KEY (processid, materialid, iotype)
);
