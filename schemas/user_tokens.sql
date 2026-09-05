-- ============================================================================
-- PostgreSQL Table & Index Schema: user_tokens
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_tokens (
    expiresat TIMESTAMP WITHOUT TIME ZONE,
    id INTEGER DEFAULT nextval('user_tokens_id_seq'::regclass) NOT NULL,
    refreshtoken TEXT,
    token TEXT,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    type TEXT,
    user_agent TEXT,
    last_ip TEXT,
    iat_id TEXT,
    PRIMARY KEY (id)
);
