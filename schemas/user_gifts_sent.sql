-- ============================================================================
-- PostgreSQL Table & Index Schema: user_gifts_sent
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_gifts_sent (
    giftid TEXT,
    id INTEGER DEFAULT nextval('user_gifts_sent_id_seq'::regclass) NOT NULL,
    userid TEXT,
    PRIMARY KEY (id)
);
