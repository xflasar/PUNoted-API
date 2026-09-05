-- ============================================================================
-- PostgreSQL Table & Index Schema: users_data
-- ============================================================================

CREATE TABLE IF NOT EXISTS users_data (
    activedaysperweek INTEGER,
    companyid TEXT,
    created TIMESTAMP WITHOUT TIME ZONE,
    displayname TEXT,
    highesttier TEXT,
    ismuted BOOLEAN,
    ispayinguser BOOLEAN,
    owncurrencyid TEXT,
    preferredlocale TEXT,
    subscriptionexpiry TIMESTAMP WITHOUT TIME ZONE,
    subscriptionlevel TEXT,
    userid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    corporationid TEXT,
    PRIMARY KEY (userid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_users_data_userid ON public.users_data USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_users_displayname_trgm ON public.users_data USING gin (displayname gin_trgm_ops);
