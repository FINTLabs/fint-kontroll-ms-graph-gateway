CREATE TABLE groups (
                        object_id UUID PRIMARY KEY,
                        resource_group_id BIGINT NOT NULL,
                        checksum BYTEA NOT NULL,
                        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        not_seen_count INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX idx_groups_resource_group_id
    ON groups(resource_group_id)
