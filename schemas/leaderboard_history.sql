-- ============================================================================
-- PostgreSQL Table & Index Schema: leaderboard_history
-- ============================================================================

CREATE TABLE IF NOT EXISTS leaderboard_history (
    record_date DATE DEFAULT CURRENT_DATE NOT NULL,
    category TEXT NOT NULL,
    time_range TEXT NOT NULL,
    material_ticker TEXT DEFAULT 'NONE'::text NOT NULL,
    company_id TEXT NOT NULL,
    rank INTEGER,
    score NUMERIC
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS uq_leaderboard_daily ON public.leaderboard_history USING btree (record_date, category, time_range, material_ticker, company_id);
CREATE INDEX IF NOT EXISTS idx_leaderboard_history_company ON public.leaderboard_history USING btree (company_id, category, record_date);
CREATE INDEX IF NOT EXISTS idx_leaderboard_history_daily ON public.leaderboard_history USING btree (record_date DESC, category, time_range, material_ticker);
