-- ============================================================================
-- PostgreSQL Table & Index Schema: scheduled_tasks
-- ============================================================================

CREATE TABLE IF NOT EXISTS scheduled_tasks (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    accountid VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    reference_id VARCHAR(255),
    trigger_time TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB DEFAULT '{}'::jsonb,
    is_processed BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_pending ON public.scheduled_tasks USING btree (trigger_time) WHERE (is_processed = false);
CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_reference ON public.scheduled_tasks USING btree (reference_id, event_type) WHERE (is_processed = false);
