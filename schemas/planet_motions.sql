-- PostgreSQL Table Schema: planet_motions, planet_motion_votes & planet_motion_components

CREATE TABLE IF NOT EXISTS planet_motions (
    motionid VARCHAR(64) PRIMARY KEY,
    admincenterid VARCHAR(64),
    naturalid VARCHAR(64),
    name TEXT,
    creator_id VARCHAR(64),
    creator_username TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(32),
    voting_start TIMESTAMP WITH TIME ZONE,
    voting_end TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_planet_motions_admincenter 
    ON planet_motions (admincenterid);
CREATE INDEX IF NOT EXISTS idx_planet_motions_naturalid 
    ON planet_motions (naturalid);

CREATE TABLE IF NOT EXISTS planet_motion_votes (
    id SERIAL PRIMARY KEY,
    motionid VARCHAR(64) NOT NULL,
    voter_id VARCHAR(64),
    voter_username TEXT,
    role VARCHAR(64),
    status VARCHAR(32),
    voted_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_planet_motion_votes_motion 
    ON planet_motion_votes (motionid);
CREATE INDEX IF NOT EXISTS idx_planet_motion_votes_voter 
    ON planet_motion_votes (voter_id);

CREATE TABLE IF NOT EXISTS planet_motion_components (
    componentid VARCHAR(64) PRIMARY KEY,
    motionid VARCHAR(64) NOT NULL,
    type VARCHAR(64),
    contributor_id VARCHAR(64),
    contributor_username TEXT,
    recipient_id VARCHAR(64),
    recipient_username TEXT,
    amount DOUBLE PRECISION,
    currency VARCHAR(16),
    program VARCHAR(64),
    category VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_planet_motion_components_motion 
    ON planet_motion_components (motionid);
