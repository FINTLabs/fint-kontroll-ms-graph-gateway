CREATE TABLE user_memberships
(
    user_ref        UUID        NOT NULL,
    group_ref       UUID        NOT NULL,
    status          VARCHAR(50) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_ref, group_ref)
);

CREATE INDEX idx_user_memberships_status
    ON user_memberships (status);
