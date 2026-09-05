-- ============================================================================
-- PostgreSQL Table & Index Schema: users
-- ============================================================================

CREATE TABLE IF NOT EXISTS users (
    email TEXT,
    fioapikey TEXT,
    isverified BOOLEAN DEFAULT false,
    password_hash TEXT,
    type TEXT DEFAULT 'user'::character varying,
    userdataid TEXT,
    username TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    accountid UUID DEFAULT gen_random_uuid() NOT NULL,
    is_synchronized BOOLEAN DEFAULT false NOT NULL,
    dataapikeys TEXT[],
    displayname TEXT,
    PRIMARY KEY (accountid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_users_accountid ON public.users USING btree (accountid);
CREATE INDEX IF NOT EXISTS idx_users_userdataid ON public.users USING btree (userdataid);
CREATE INDEX IF NOT EXISTS idx_users_xata ON public.users USING btree (xata_updatedat);
CREATE INDEX IF NOT EXISTS idx_users_xata_updatedat ON public.users USING btree (xata_updatedat);
