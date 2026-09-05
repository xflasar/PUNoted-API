-- ============================================================================
-- PostgreSQL Table & Index Schema: contract_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS contract_materials (
    contractconditionid TEXT NOT NULL,
    materialid TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    pickedupamount NUMERIC DEFAULT 0,
    contractparty TEXT NOT NULL,
    PRIMARY KEY (contractconditionid, materialid, contractparty)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_materials_conditionid ON public.contract_materials USING btree (contractconditionid);
