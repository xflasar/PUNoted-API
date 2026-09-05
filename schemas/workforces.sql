-- ============================================================================
-- PostgreSQL Table & Index Schema: workforces
-- ============================================================================

CREATE TABLE IF NOT EXISTS workforces (
    capacity INTEGER,
    population INTEGER,
    required INTEGER,
    reserve INTEGER,
    satisfaction DOUBLE PRECISION,
    siteid TEXT,
    level TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    userid TEXT,
    workforceid TEXT NOT NULL,
    PRIMARY KEY (workforceid)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS workforces_unique_key ON public.workforces USING btree (siteid, level);
