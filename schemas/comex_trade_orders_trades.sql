-- ============================================================================
-- PostgreSQL Table & Index Schema: comex_trade_orders_trades
-- ============================================================================

CREATE TABLE IF NOT EXISTS comex_trade_orders_trades (
    amount INTEGER,
    orderid TEXT,
    partnercode TEXT,
    partnerid TEXT,
    partnername TEXT,
    priceamount NUMERIC,
    pricecurrency TEXT,
    tradeid TEXT NOT NULL,
    tradetime TIMESTAMP WITHOUT TIME ZONE,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (tradeid)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_cx_trades_orderid_amount ON public.comex_trade_orders_trades USING btree (orderid) INCLUDE (amount);
