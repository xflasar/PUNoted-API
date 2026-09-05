-- ============================================================================
-- PostgreSQL Table & Index Schema: planet_populations
-- ============================================================================

CREATE TABLE IF NOT EXISTS planet_populations (
    averagehappinessengineer DOUBLE PRECISION,
    averagehappinesspioneer DOUBLE PRECISION,
    averagehappinessscientist DOUBLE PRECISION,
    averagehappinesssettler DOUBLE PRECISION,
    averagehappinesstechnician DOUBLE PRECISION,
    explorersgraceenabled BOOLEAN,
    governmentprogramtype TEXT,
    nextpopulationengineer INTEGER,
    nextpopulationpioneer INTEGER,
    nextpopulationscientist INTEGER,
    nextpopulationsettler INTEGER,
    nextpopulationtechnician INTEGER,
    openjobsengineer INTEGER,
    openjobspioneer INTEGER,
    openjobsscientist INTEGER,
    openjobssettler INTEGER,
    openjobstechnician INTEGER,
    populationdifferenceengineer INTEGER,
    populationdifferencepioneer INTEGER,
    populationdifferencescientist INTEGER,
    populationdifferencesettler INTEGER,
    populationdifferencetechnician INTEGER,
    populationid TEXT NOT NULL,
    simulationperiod INTEGER,
    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    unemploymentrateengineer DOUBLE PRECISION,
    unemploymentratepioneer DOUBLE PRECISION,
    unemploymentratescientist DOUBLE PRECISION,
    unemploymentratesettler DOUBLE PRECISION,
    unemploymentratetechnician DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (populationid, time)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_planet_populations_latest ON public.planet_populations USING btree (populationid, "time" DESC);
