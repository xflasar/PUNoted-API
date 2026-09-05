-- ============================================================================
-- PostgreSQL Table & Index Schema: contracts
-- ============================================================================

CREATE TABLE IF NOT EXISTS contracts (
    id TEXT NOT NULL,
    localid TEXT,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    party TEXT NOT NULL,
    partnerid TEXT,
    partnername TEXT,
    partnercode TEXT,
    status TEXT NOT NULL,
    duedate TIMESTAMP WITH TIME ZONE,
    name TEXT,
    preamble TEXT,
    extensiondeadline TIMESTAMP WITH TIME ZONE,
    relatedcontracts TEXT[],
    contracttype TEXT,
    userid TEXT,
    terminationreceived BOOLEAN,
    terminationsent BOOLEAN,
    agentcontract BOOLEAN,
    canextend BOOLEAN,
    canrequesttermination BOOLEAN,
    PRIMARY KEY (id, party)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_contracts_filtering ON public.contracts USING btree (userid, status, party, partnercode);
CREATE INDEX IF NOT EXISTS idx_contracts_userid_date_desc ON public.contracts USING btree (userid, date DESC);
