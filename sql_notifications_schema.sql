-- SQL Schema Migration for App-Wide User Notifications & Public Announcements

CREATE TABLE IF NOT EXISTS user_notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    accountid TEXT NOT NULL,
    category TEXT NOT NULL,
    type TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    dedup_key TEXT UNIQUE,
    data JSONB,
    is_read BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Ensure all columns exist on pre-existing user_notifications table
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS accountid TEXT;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS type TEXT;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS title TEXT;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS message TEXT;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS dedup_key TEXT;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS data JSONB;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS is_read BOOLEAN DEFAULT FALSE;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_user_notif_acc_created ON user_notifications (accountid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_notif_unread ON user_notifications (accountid, is_read);
CREATE INDEX IF NOT EXISTS idx_user_notif_acc_active ON user_notifications (accountid, is_deleted, created_at DESC);

CREATE TABLE IF NOT EXISTS public_announcements (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    severity TEXT DEFAULT 'info',
    link TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_public_announcements_active ON public_announcements (is_active, created_at DESC);
