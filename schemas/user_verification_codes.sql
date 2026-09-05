-- ============================================================================
-- PostgreSQL Table & Index Schema: user_verification_codes
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_verification_codes (
    code TEXT,
    email TEXT,
    expiresat TIMESTAMP WITHOUT TIME ZONE,
    id INTEGER DEFAULT nextval('user_verification_codes_id_seq'::regclass) NOT NULL,
    servercode TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);
