-- ============================================================================
-- PostgreSQL Table & Index Schema: ship_build_presets
-- ============================================================================

CREATE TABLE IF NOT EXISTS ship_build_presets (
    id INTEGER DEFAULT nextval('ship_build_presets_id_seq'::regclass) NOT NULL,
    corporation_id VARCHAR(100),
    name VARCHAR(255) NOT NULL,
    price NUMERIC NOT NULL,
    price_corp NUMERIC NOT NULL,
    parts JSONB NOT NULL,
    is_admin_preset BOOLEAN DEFAULT false,
    created_by VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);
