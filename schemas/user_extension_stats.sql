-- PostgreSQL Table Schema: user_extension_stats
-- Stores cumulative sent message count and telemetry for each user

CREATE TABLE IF NOT EXISTS user_extension_stats (
    userid TEXT NOT NULL PRIMARY KEY,
    total_messages_sent BIGINT NOT NULL DEFAULT 0,
    last_batch_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_ext_stats_user ON public.user_extension_stats USING btree (userid);
