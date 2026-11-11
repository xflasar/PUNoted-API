-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_routes
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_routes (
    routeid TEXT NOT NULL,
    naturalid TEXT NOT NULL,
    name TEXT,
    userid TEXT NOT NULL,
    repeats BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (routeid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_ship_routes_userid ON public.ship_routes USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_ship_routes_naturalid ON public.ship_routes USING btree (naturalid);

