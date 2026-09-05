-- ============================================================================
-- PostgreSQL Table & Index Schema: building_workforce_capacities
-- ============================================================================

CREATE TABLE IF NOT EXISTS building_workforce_capacities (
    buildingid TEXT NOT NULL,
    capacity INTEGER,
    workforcelevel TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    ishabitation BOOLEAN DEFAULT false
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS building_workforce_capacities_id ON public.building_workforce_capacities USING btree (buildingid, workforcelevel);
