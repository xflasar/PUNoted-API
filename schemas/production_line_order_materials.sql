-- ============================================================================
-- PostgreSQL Table & Index Schema: production_line_order_materials
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_line_order_materials (
    materialid TEXT,
    poroductionlineorderid TEXT,
    quantity INTEGER,
    type TEXT,
    valueamount DOUBLE PRECISION,
    valuecurrency TEXT
);
