-- ============================================================================
-- PostgreSQL Table & Index Schema: user_gifts_received
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_gifts_received (
    giftid TEXT,
    id INTEGER DEFAULT nextval('user_gifts_received_id_seq'::regclass) NOT NULL,
    userid TEXT,
    PRIMARY KEY (id)
);
