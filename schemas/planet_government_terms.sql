-- PostgreSQL Table Schema: planet_government_terms & planet_government_candidates

CREATE TABLE IF NOT EXISTS planet_government_terms (
    termid VARCHAR(64) PRIMARY KEY,
    admincenterid VARCHAR(64) NOT NULL,
    planet_natural_id INT,
    election_start TIMESTAMP WITH TIME ZONE,
    election_end TIMESTAMP WITH TIME ZONE,
    term_start TIMESTAMP WITH TIME ZONE,
    term_end TIMESTAMP WITH TIME ZONE,
    parliament_size INT,
    election_ongoing BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_planet_gov_terms_admincenter 
    ON planet_government_terms (admincenterid);

CREATE TABLE IF NOT EXISTS planet_government_candidates (
    id SERIAL PRIMARY KEY,
    termid VARCHAR(64) NOT NULL,
    userid VARCHAR(64),
    username TEXT,
    corporation_id VARCHAR(64),
    corporation_name TEXT,
    corporation_code TEXT,
    country_id VARCHAR(64),
    country_code TEXT,
    country_name TEXT,
    votes INT,
    votes_percentage DOUBLE PRECISION,
    is_winner BOOLEAN DEFAULT FALSE,
    start_of_run TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_planet_gov_candidates_term 
        FOREIGN KEY (termid) REFERENCES planet_government_terms(termid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_planet_gov_candidates_term 
    ON planet_government_candidates (termid);
CREATE INDEX IF NOT EXISTS idx_planet_gov_candidates_user 
    ON planet_government_candidates (userid);
