-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_infrastructure_contributions
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_infrastructure_contributions (
    contributorid TEXT NOT NULL,
    contributorname TEXT NOT NULL,
    contributorcode TEXT NOT NULL,
    amount INTEGER NOT NULL,
    materialid TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    projectid TEXT NOT NULL,
    id TEXT DEFAULT gen_random_uuid() NOT NULL,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS planet_infrastructure_contributions_unq ON public.planet_infrastructure_contributions USING btree (projectid, contributorid, materialid, "timestamp");
