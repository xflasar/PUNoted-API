-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_blueprints
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_blueprints (
    buildtime INTEGER,
    createdtimestamp TIMESTAMP WITHOUT TIME ZONE,
    id TEXT NOT NULL,
    name TEXT,
    naturalid TEXT,
    status TEXT,
    natural_id TEXT,
    user_id TEXT,
    bill_of_material JSONB,
    selections JSONB,
    performance JSONB,
    build_time INTEGER,
    xata_updatedat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    type TEXT,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_user_id ON public.ship_blueprints USING btree (user_id);
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_natural_id ON public.ship_blueprints USING btree (natural_id);
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_status ON public.ship_blueprints USING btree (status);
