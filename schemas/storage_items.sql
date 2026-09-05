-- ============================================================================
-- PostgreSQL Table & Index Schema: storage_items
-- ============================================================================

CREATE TABLE IF NOT EXISTS storage_items (
    compositekey TEXT NOT NULL,
    currencyamount DOUBLE PRECISION,
    currencytype TEXT,
    materialid TEXT,
    quantity INTEGER,
    storageid TEXT,
    totalvolume DOUBLE PRECISION,
    totalweight DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    type TEXT,
    PRIMARY KEY (compositekey)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_storage_items_storageid ON public.storage_items USING btree (storageid) INCLUDE (quantity);
