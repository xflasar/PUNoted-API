-- ============================================================================
-- PostgreSQL Table & Index Schema: subsector_vertices
-- ============================================================================

CREATE TABLE IF NOT EXISTS subsector_vertices (
    externalsubsectorid TEXT NOT NULL,
    index INTEGER,
    x DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    y DOUBLE PRECISION,
    z DOUBLE PRECISION
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS subsector_vertices_id ON public.subsector_vertices USING btree (index, externalsubsectorid);
