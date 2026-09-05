-- ============================================================================
-- PostgreSQL Table & Index Schema: corporation_shareholders_history
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporation_shareholders_history (
    corporationid TEXT,
    companyid TEXT NOT NULL,
    relativeshare INTEGER,
    shares INTEGER,
    userid TEXT,
    companycode TEXT,
    companyname TEXT,
    id UUID NOT NULL,
    snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_corp_history_company ON public.corporation_shareholders_history USING btree (companycode);
CREATE INDEX IF NOT EXISTS idx_corp_history_date ON public.corporation_shareholders_history USING btree (snapshot_at);
