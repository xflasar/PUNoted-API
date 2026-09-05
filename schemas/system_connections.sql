-- ============================================================================
-- PostgreSQL Table & Index Schema: system_connections
-- ============================================================================

CREATE TABLE IF NOT EXISTS system_connections (
    systemiddestination TEXT,
    systemidorigin TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS system_connections_id ON public.system_connections USING btree (systemidorigin, systemiddestination);
