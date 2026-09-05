-- ============================================================================
-- PostgreSQL Table & Index Schema: contract_conditions
-- ============================================================================

CREATE TABLE IF NOT EXISTS contract_conditions (
    id TEXT NOT NULL,
    contractid TEXT NOT NULL,
    deadline TIMESTAMP WITH TIME ZONE,
    deadlineduration_millis BIGINT,
    amountmoney NUMERIC,
    currencymoney TEXT,
    dependencies TEXT[],
    addresssystemid TEXT,
    addressplanetid TEXT,
    addressstationid TEXT,
    destinationsystemid TEXT,
    destinationplanetid TEXT,
    destinationstationid TEXT,
    index INTEGER,
    type TEXT NOT NULL,
    party TEXT,
    status TEXT NOT NULL,
    autoprovisionstoreid TEXT,
    reputationchange NUMERIC,
    blockid TEXT,
    shipmentitemid TEXT,
    contractparty TEXT NOT NULL,
    PRIMARY KEY (id, contractparty)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_conditions_address_planet ON public.contract_conditions USING btree (addressplanetid);
CREATE INDEX IF NOT EXISTS idx_conditions_address_station ON public.contract_conditions USING btree (addressstationid);
CREATE INDEX IF NOT EXISTS idx_conditions_contractid_index ON public.contract_conditions USING btree (contractid, index);
CREATE INDEX IF NOT EXISTS idx_conditions_contractid_type ON public.contract_conditions USING btree (contractid, type);
CREATE INDEX IF NOT EXISTS idx_conditions_dest_planet ON public.contract_conditions USING btree (destinationplanetid);
CREATE INDEX IF NOT EXISTS idx_conditions_dest_station ON public.contract_conditions USING btree (destinationstationid);
