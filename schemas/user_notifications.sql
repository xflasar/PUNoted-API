-- ============================================================================
-- PostgreSQL Table & Index Schema: user_notifications
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_notifications (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    accountid VARCHAR(255) NOT NULL,
    type VARCHAR(50) DEFAULT 'info'::character varying,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    is_read BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    is_deleted BOOLEAN DEFAULT false,
    category TEXT,
    dedup_key TEXT,
    data JSONB,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_user_notifications_userid ON public.user_notifications USING btree (accountid) WHERE (is_read = false);
CREATE INDEX IF NOT EXISTS idx_user_notif_acc_active ON public.user_notifications USING btree (accountid, is_deleted, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS user_notifications_dedup_key_key ON public.user_notifications USING btree (dedup_key);
CREATE INDEX IF NOT EXISTS idx_user_notif_acc_created ON public.user_notifications USING btree (accountid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_notif_unread ON public.user_notifications USING btree (accountid, is_read);
