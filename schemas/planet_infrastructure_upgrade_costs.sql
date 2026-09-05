-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_infrastructure_upgrade_costs
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_infrastructure_upgrade_costs (
    projectid TEXT NOT NULL,
    id TEXT DEFAULT gen_random_uuid() NOT NULL,
    amount INTEGER NOT NULL,
    currentamount INTEGER NOT NULL,
    materialid TEXT NOT NULL,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS planet_infrastructure_upgrade_costs_unq ON public.planet_infrastructure_upgrade_costs USING btree (projectid, materialid);
