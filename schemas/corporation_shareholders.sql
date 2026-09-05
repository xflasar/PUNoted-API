-- ============================================================================
-- PostgreSQL Table & Index Schema: corporation_shareholders
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporation_shareholders (
    corporationid TEXT,
    companyid TEXT NOT NULL,
    relativeshare INTEGER,
    shares INTEGER,
    userid TEXT,
    companycode TEXT,
    companyname TEXT,
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS corporation_shareholders_id ON public.corporation_shareholders USING btree (corporationid, companyid);
CREATE INDEX IF NOT EXISTS idx_corp_shareholders_corporationid_userid ON public.corporation_shareholders USING btree (corporationid, userid);
CREATE INDEX IF NOT EXISTS idx_corp_shareholders_userid_corporationid ON public.corporation_shareholders USING btree (userid, corporationid);
CREATE INDEX IF NOT EXISTS idx_cs_corp_user ON public.corporation_shareholders USING btree (corporationid, userid);
