-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_flight_segments
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_flight_segments (
    segment_id BIGINT DEFAULT nextval('ship_flight_segments_segment_id_seq'::regclass) NOT NULL,
    flight_id TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    segment_type TEXT NOT NULL,
    departure BIGINT,
    arrival BIGINT,
    duration BIGINT,
    origin_system_id TEXT,
    origin_location_id TEXT,
    origin_location_type TEXT,
    origin_orbit_data JSONB,
    destination_system_id TEXT,
    destination_location_id TEXT,
    destination_location_type TEXT,
    destination_orbit_data JSONB,
    stl_distance DOUBLE PRECISION,
    stl_fuel INTEGER,
    ftl_distance DOUBLE PRECISION,
    ftl_fuel INTEGER,
    damage DOUBLE PRECISION,
    transferellipse JSONB,
    PRIMARY KEY (segment_id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS ship_flight_segments_flight_id_segment_index_key ON public.ship_flight_segments USING btree (flight_id, segment_index);
