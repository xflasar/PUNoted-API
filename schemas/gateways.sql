-- ============================================================================
-- PostgreSQL Table & Index Schema: gateways
-- ============================================================================

CREATE TABLE IF NOT EXISTS gateways (
    id TEXT NOT NULL,
    natural_id TEXT,
    name TEXT,
    type TEXT,
    system_id TEXT,
    planet_id TEXT,
    owner_admin_center_id TEXT,
    currency_code TEXT,
    established TIMESTAMP WITHOUT TIME ZONE,
    operational_state TEXT,
    link_status TEXT,
    outgoing_link_id TEXT,
    incoming_links TEXT[],
    is_linked BOOLEAN,
    max_ship_volume DOUBLE PRECISION,
    linking_radius DOUBLE PRECISION,
    jumps_per_day DOUBLE PRECISION,
    fuel_available DOUBLE PRECISION,
    fuel_max DOUBLE PRECISION,
    fuel_per_jump DOUBLE PRECISION,
    fuel_usage_fee DOUBLE PRECISION,
    fuel_usage_currency TEXT,
    avg_fuel_availability DOUBLE PRECISION,
    capacity_upgrades INTEGER,
    volume_upgrades INTEGER,
    distance_upgrades INTEGER,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    satellite_id TEXT,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_gateways_system ON public.gateways USING btree (system_id);
