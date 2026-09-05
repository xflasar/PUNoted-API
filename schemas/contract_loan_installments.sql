-- ============================================================================
-- PostgreSQL Table & Index Schema: contract_loan_installments
-- ============================================================================

CREATE TABLE IF NOT EXISTS contract_loan_installments (
    conditionid TEXT NOT NULL,
    interestamount NUMERIC,
    repaymentamount NUMERIC,
    totalamount NUMERIC NOT NULL,
    currency TEXT NOT NULL,
    contractparty TEXT NOT NULL,
    PRIMARY KEY (conditionid, contractparty)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_loans_conditionid ON public.contract_loan_installments USING btree (conditionid);
