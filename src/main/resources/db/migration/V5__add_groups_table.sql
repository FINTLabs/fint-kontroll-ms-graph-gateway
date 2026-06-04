CREATE TABLE groups (
                        object_id UUID PRIMARY KEY,
                        checksum BYTEA NOT NULL,
                        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        not_seen_count INTEGER NOT NULL DEFAULT 0
);
