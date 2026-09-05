-- ============================================================================
-- PUNoted Comprehensive PostgreSQL Database Performance & Index Optimization
-- Execute this SQL script directly on your production PostgreSQL database!
-- ============================================================================

-- 1. CX BROKERS & HISTORY TABLES
CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_ticker_snap 
ON cx_brokers_history (ticker, snapshot_at DESC);

CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_upper_ticker 
ON cx_brokers_history (UPPER(ticker));

CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_snap_ticker 
ON cx_brokers_history (snapshot_at DESC, ticker);

CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_snapshot_at 
ON cx_brokers_history (snapshot_at DESC);

-- 2. USER & AUTH TABLES
CREATE INDEX IF NOT EXISTS idx_users_username 
ON users (LOWER(username));

CREATE INDEX IF NOT EXISTS idx_users_company_code 
ON users (company_code);

CREATE INDEX IF NOT EXISTS idx_user_tokens_user_id 
ON user_tokens (user_id);

CREATE INDEX IF NOT EXISTS idx_user_tokens_token 
ON user_tokens (token);

-- 3. SHIPS & TELEMETRY TABLES
CREATE INDEX IF NOT EXISTS idx_ships_user_id 
ON user_ships (user_id);

CREATE INDEX IF NOT EXISTS idx_ships_registration 
ON user_ships (registration);

CREATE INDEX IF NOT EXISTS idx_shipments_user_id 
ON shipments (user_id);

CREATE INDEX IF NOT EXISTS idx_shipments_status 
ON shipments (status);

-- 4. STORAGE & INVENTORY TABLES
CREATE INDEX IF NOT EXISTS idx_storages_user_id 
ON storages (user_id);

CREATE INDEX IF NOT EXISTS idx_storages_station_code 
ON storages (station_code);

CREATE INDEX IF NOT EXISTS idx_storage_items_storage_id 
ON storage_items (storage_id);

CREATE INDEX IF NOT EXISTS idx_storage_items_ticker 
ON storage_items (ticker);

-- 5. CONTRACTS & GOVERNANCE TABLES
CREATE INDEX IF NOT EXISTS idx_contracts_user_id 
ON contracts (user_id);

CREATE INDEX IF NOT EXISTS idx_contracts_status 
ON contracts (status);

CREATE INDEX IF NOT EXISTS idx_contract_conditions_contract_id 
ON contract_conditions (contract_id);

-- 6. MAP & SYSTEM TABLES
CREATE INDEX IF NOT EXISTS idx_map_systems_system_id 
ON map_systems (system_id);

CREATE INDEX IF NOT EXISTS idx_map_planets_planet_id 
ON map_planets (planet_id);

CREATE INDEX IF NOT EXISTS idx_map_stations_station_id 
ON map_stations (station_id);

-- UPDATE POSTGRES STATISTICAL PLANNER DATA
ANALYZE cx_brokers_history;
ANALYZE users;
ANALYZE user_tokens;
ANALYZE user_ships;
ANALYZE shipments;
ANALYZE storages;
ANALYZE storage_items;
ANALYZE contracts;
ANALYZE contract_conditions;
ANALYZE map_systems;
ANALYZE map_planets;
ANALYZE map_stations;
