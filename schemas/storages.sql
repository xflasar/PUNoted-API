-- ============================================================================
-- PostgreSQL Table & Index Schema: storages
-- ============================================================================

CREATE TABLE IF NOT EXISTS storages (
    addressableid TEXT,
    fixed BOOLEAN,
    locked BOOLEAN,
    name TEXT,
    rank INTEGER,
    storageid TEXT NOT NULL,
    tradestore BOOLEAN,
    type TEXT,
    userid TEXT NOT NULL,
    volumecapacity INTEGER,
    volumeload DOUBLE PRECISION,
    weightcapacity INTEGER,
    weightload DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (storageid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_storage_user ON public.storages USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_storages_capacities ON public.storages USING btree (weightcapacity, volumecapacity);
