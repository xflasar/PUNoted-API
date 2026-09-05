-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_orbit
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_orbit (
    eccentricity DOUBLE PRECISION,
    id INTEGER DEFAULT nextval('planet_orbit_id_seq'::regclass) NOT NULL,
    inclination DOUBLE PRECISION,
    orbitindex INTEGER,
    periapsis DOUBLE PRECISION,
    planetid TEXT NOT NULL,
    rightascension DOUBLE PRECISION,
    semimajoraxis DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (planetid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_planet_orbit_planetid ON public.planet_orbit USING btree (planetid);
