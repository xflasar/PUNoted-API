-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_route_waypoints
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_route_waypoints (
    waypointid TEXT NOT NULL,
    routeid TEXT NOT NULL REFERENCES ship_routes(routeid) ON DELETE CASCADE,
    waypoint_index INTEGER NOT NULL,
    system_id TEXT,
    location_id TEXT,
    location_type TEXT,            -- 'PLANET' | 'STATION'
    landing BOOLEAN DEFAULT TRUE,
    fuel_usage_factor DOUBLE PRECISION DEFAULT 1.0,
    reactor_usage_factor DOUBLE PRECISION DEFAULT 1.0,
    ftl_preference TEXT DEFAULT 'LEAST_JUMPS',
    use_gateways BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (waypointid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_route_waypoints_route ON public.ship_route_waypoints USING btree (routeid, waypoint_index);
CREATE INDEX IF NOT EXISTS idx_route_waypoints_location ON public.ship_route_waypoints USING btree (location_id);
CREATE INDEX IF NOT EXISTS idx_route_waypoints_system ON public.ship_route_waypoints USING btree (system_id);

