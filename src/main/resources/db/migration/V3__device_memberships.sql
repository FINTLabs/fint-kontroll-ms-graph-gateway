CREATE TABLE device_memberships
(
    device_ref       UUID NOT NULL,
    resource_ref     UUID NOT NULL,
    status           VARCHAR(50) NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (device_ref, resource_ref)
);
CREATE INDEX idx_device_memberships_resource_ref
    ON device_memberships (resource_ref);

CREATE INDEX idx_device_memberships_status
    ON device_memberships (status);
