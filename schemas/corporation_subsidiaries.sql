-- ============================================================================
-- PostgreSQL Table & Index Schema: corporation_subsidiaries
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporation_subsidiaries (
    corporationmainid TEXT NOT NULL,
    corporationsubid TEXT NOT NULL,
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    linkedat TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);
