-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_repair_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_repair_materials (
    amount INTEGER,
    materialid TEXT,
    shipid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS ships_repair_materials_id ON public.ship_repair_materials USING btree (shipid, materialid);
CREATE INDEX IF NOT EXISTS idx_ship_repair_materials_shipid ON public.ship_repair_materials USING btree (shipid);
