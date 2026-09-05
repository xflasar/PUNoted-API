-- ============================================================================
-- PostgreSQL Table & Index Schema: public_announcements
-- ============================================================================

CREATE TABLE IF NOT EXISTS public_announcements (
    id INTEGER DEFAULT nextval('public_announcements_id_seq'::regclass) NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    severity TEXT DEFAULT 'info'::text,
    link TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_public_announcements_active ON public.public_announcements USING btree (is_active, created_at DESC);
