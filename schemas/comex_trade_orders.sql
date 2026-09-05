-- ============================================================================
-- PostgreSQL Table & Index Schema: comex_trade_orders
-- ============================================================================

CREATE TABLE IF NOT EXISTS comex_trade_orders (
    amount INTEGER,
    brokerid TEXT,
    created TIMESTAMP WITHOUT TIME ZONE,
    exchangeid TEXT,
    initialamount INTEGER,
    limitamount NUMERIC,
    limitcurrency TEXT,
    materialid TEXT,
    orderid TEXT NOT NULL,
    status TEXT,
    type TEXT,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (orderid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_cx_orders_user_created ON public.comex_trade_orders USING btree (userid, created DESC);
CREATE INDEX IF NOT EXISTS idx_cx_orders_user_status ON public.comex_trade_orders USING btree (userid, status);
