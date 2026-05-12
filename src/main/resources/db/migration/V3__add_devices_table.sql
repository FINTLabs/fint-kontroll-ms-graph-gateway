CREATE TABLE devices
(
    id             BIGSERIAL PRIMARY KEY,
    object_id      UUID        NOT NULL UNIQUE,
    checksum       BYTEA       NOT NULL,
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    not_seen_count INT         NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX uq_entra_device_object_id ON devices (object_id);
CREATE UNIQUE INDEX uq_entra_device_id ON devices (id);
CREATE INDEX idx_entra_device_checkpoint_last_seen
    ON devices (last_seen_at);