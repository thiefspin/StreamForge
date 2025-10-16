-- Migration: Create events_normalized table
-- Description: Initial schema for storing normalized events from Kafka

-- Create events_normalized table
CREATE TABLE IF NOT EXISTS events_normalized (
    -- Primary key
    event_id UUID PRIMARY KEY,

    -- Event data
    event_type TEXT NOT NULL CHECK (event_type IN ('CLICK', 'VIEW', 'PURCHASE')),
    occurred_at TIMESTAMPTZ NOT NULL,
    user_id UUID NOT NULL,

    -- Optional fields
    amount_cents BIGINT CHECK (amount_cents IS NULL OR amount_cents >= 0),
    path TEXT,
    referrer TEXT,

    -- Metadata
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    src_partition INT NOT NULL,
    src_offset BIGINT NOT NULL,

    -- Ensure we don't process the same Kafka message twice
    CONSTRAINT unique_kafka_message UNIQUE (src_partition, src_offset)
);

-- Create indexes for common query patterns
CREATE INDEX idx_events_occurred_at ON events_normalized(occurred_at DESC);
CREATE INDEX idx_events_user_id ON events_normalized(user_id);
CREATE INDEX idx_events_event_type ON events_normalized(event_type);
CREATE INDEX idx_events_partition_offset ON events_normalized(src_partition, src_offset);

-- Create composite index for time-based queries with user filtering
CREATE INDEX idx_events_user_time ON events_normalized(user_id, occurred_at DESC);

-- Add comments for documentation
COMMENT ON TABLE events_normalized IS 'Normalized events consumed from Kafka and validated by StreamForge';
COMMENT ON COLUMN events_normalized.event_id IS 'Unique identifier for the event (UUID v4)';
COMMENT ON COLUMN events_normalized.event_type IS 'Type of event: CLICK, VIEW, or PURCHASE';
COMMENT ON COLUMN events_normalized.occurred_at IS 'When the event occurred (client timestamp)';
COMMENT ON COLUMN events_normalized.user_id IS 'User who generated the event';
COMMENT ON COLUMN events_normalized.amount_cents IS 'Transaction amount in cents (for PURCHASE events)';
COMMENT ON COLUMN events_normalized.path IS 'URL path where the event occurred';
COMMENT ON COLUMN events_normalized.referrer IS 'Referrer URL if available';
COMMENT ON COLUMN events_normalized.processed_at IS 'When StreamForge processed this event';
COMMENT ON COLUMN events_normalized.src_partition IS 'Source Kafka partition';
COMMENT ON COLUMN events_normalized.src_offset IS 'Source Kafka offset within partition';
