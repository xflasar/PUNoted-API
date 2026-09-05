-- ============================================================================
-- PostgreSQL Table & Index Schema: rating_reports
-- ============================================================================

CREATE TABLE IF NOT EXISTS rating_reports (
    contractcount INTEGER,
    earliestcontract TIMESTAMP WITHOUT TIME ZONE,
    overallrating TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    ratingreportid UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (ratingreportid)
);
