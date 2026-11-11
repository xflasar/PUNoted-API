-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_route_steps
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_route_steps (
    stepid TEXT NOT NULL,
    waypointid TEXT NOT NULL REFERENCES ship_route_waypoints(waypointid) ON DELETE CASCADE,
    routeid TEXT NOT NULL REFERENCES ship_routes(routeid) ON DELETE CASCADE,
    step_index INTEGER NOT NULL,
    type TEXT NOT NULL,            -- 'LOAD' | 'UNLOAD' | 'REFUEL' | 'WAIT' | 'LOAD_SHIPMENT' | 'UNLOAD_SHIPMENT'
    
    -- Parameters for LOAD / UNLOAD:
    materialid TEXT,
    material_ticker TEXT,
    min_type TEXT,                 -- 'ABSOLUTE' | 'CAPACITY'
    min_units INTEGER,
    max_type TEXT,
    max_units INTEGER,
    retain INTEGER DEFAULT 0,
    
    -- Parameters for REFUEL:
    fuel_type TEXT,                -- 'STL' | 'FTL'
    fuel_source TEXT,              -- 'LOCAL_STORE'
    
    -- Parameters for WAIT:
    wait_amount INTEGER,
    wait_unit TEXT,                -- 'MINUTES' | 'HOURS'
    
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (stepid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_route_steps_waypoint ON public.ship_route_steps USING btree (waypointid, step_index);
CREATE INDEX IF NOT EXISTS idx_route_steps_route ON public.ship_route_steps USING btree (routeid);
CREATE INDEX IF NOT EXISTS idx_route_steps_material ON public.ship_route_steps USING btree (material_ticker);
CREATE INDEX IF NOT EXISTS idx_route_steps_type ON public.ship_route_steps USING btree (type);

