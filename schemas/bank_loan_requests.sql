-- ============================================================================
-- PostgreSQL Table & Index Schema: bank_loan_requests
-- ============================================================================

CREATE TABLE IF NOT EXISTS bank_loan_requests (
    id INTEGER DEFAULT nextval('bank_loan_requests_id_seq'::regclass) NOT NULL,
    bank_id INTEGER,
    requester_username VARCHAR(100) NOT NULL,
    amount NUMERIC NOT NULL,
    interest_rate NUMERIC NOT NULL,
    term_days INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING'::character varying,
    contract_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_bank_loans_bank ON public.bank_loan_requests USING btree (bank_id);
CREATE INDEX IF NOT EXISTS idx_bank_loans_requester ON public.bank_loan_requests USING btree (requester_username);
