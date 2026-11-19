CREATE TABLE events (
                        id UUID PRIMARY KEY,
                        event_type VARCHAR(100) NOT NULL,
                        payload JSONB NOT NULL,
                        status VARCHAR(30) NOT NULL DEFAULT 'RECEIVED',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        processed_at TIMESTAMPTZ
);

CREATE INDEX idx_events_status
    ON events(status);
