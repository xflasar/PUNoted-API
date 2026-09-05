-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_celestial_bodies
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_celestial_bodies (
    id TEXT NOT NULL,
    planetid TEXT,
    systemid TEXT,
    satelliteid TEXT,
    name TEXT,
    naturalid TEXT,
    semimajoraxis DOUBLE PRECISION,
    eccentricity DOUBLE PRECISION,
    inclination DOUBLE PRECISION,
    rightascension DOUBLE PRECISION,
    periapsis DOUBLE PRECISION,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_planet_celestial_bodies_id_planetid ON public.planet_celestial_bodies USING btree (id, planetid);
