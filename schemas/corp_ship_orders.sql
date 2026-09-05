-- ============================================================================
-- PostgreSQL Table & Index Schema: corp_ship_orders
-- ============================================================================

CREATE TABLE IF NOT EXISTS corp_ship_orders (
    id INTEGER DEFAULT nextval('corp_ship_orders_id_seq'::regclass) NOT NULL,
    corporation_id VARCHAR(100),
    customer_username VARCHAR(255),
    customer_company_code VARCHAR(100),
    owner_type VARCHAR(50),
    owner_id VARCHAR(255),
    guest_pin VARCHAR(255),
    ship_config JSONB,
    price NUMERIC,
    wait_time_days INTEGER,
    status VARCHAR(50) DEFAULT 'QUEUED'::character varying,
    notes TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (id)
);
