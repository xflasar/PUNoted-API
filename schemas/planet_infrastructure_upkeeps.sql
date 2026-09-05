-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_infrastructure_upkeeps
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_infrastructure_upkeeps (
    projectid TEXT NOT NULL,
    id TEXT DEFAULT gen_random_uuid() NOT NULL,
    amount INTEGER,
    currentamount INTEGER,
    duration BIGINT,
    materialid TEXT,
    storecapacity INTEGER,
    stored INTEGER,
    nexttick BIGINT,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS planet_infrastructure_upkeeps_unq ON public.planet_infrastructure_upkeeps USING btree (projectid, materialid);
