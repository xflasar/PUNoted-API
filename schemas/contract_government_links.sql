-- PostgreSQL Table & Index Schema: contract_government_links
-- Links contracts to specific government / admin center entity IDs

CREATE TABLE IF NOT EXISTS contract_government_links (
    contractid TEXT NOT NULL,
    admincenterid TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (contractid, admincenterid)
);

CREATE INDEX IF NOT EXISTS idx_cgl_contractid ON public.contract_government_links USING btree (contractid);
CREATE INDEX IF NOT EXISTS idx_cgl_admincenterid ON public.contract_government_links USING btree (admincenterid);
