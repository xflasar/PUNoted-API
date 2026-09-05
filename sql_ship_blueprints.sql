-- SQL Schema & Optimization Script for ship_blueprints table
CREATE TABLE IF NOT EXISTS ship_blueprints (
    id TEXT PRIMARY KEY,
    natural_id TEXT,
    user_id TEXT,
    name TEXT,
    type TEXT,
    status TEXT,
    createdtimestamp TIMESTAMP,
    bill_of_material JSONB,
    selections JSONB,
    performance JSONB,
    buildtime INT,
    xata_updatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_user_id ON ship_blueprints (user_id);
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_natural_id ON ship_blueprints (natural_id);
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_status ON ship_blueprints (status);
