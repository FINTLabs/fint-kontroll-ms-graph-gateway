ALTER TABLE user_memberships
    ALTER COLUMN status DROP NOT NULL,
    ADD COLUMN desired_present BOOLEAN,
    ADD COLUMN observed_present BOOLEAN,
    ADD COLUMN observed_run_id UUID,
    ADD COLUMN last_observed_at TIMESTAMPTZ;

ALTER TABLE device_memberships
    ALTER COLUMN status DROP NOT NULL,
    ADD COLUMN desired_present BOOLEAN,
    ADD COLUMN observed_present BOOLEAN,
    ADD COLUMN observed_run_id UUID,
    ADD COLUMN last_observed_at TIMESTAMPTZ;

UPDATE user_memberships
SET desired_present = CASE
    WHEN status = 'ADDED' THEN TRUE
    WHEN status = 'REMOVED' THEN FALSE
    ELSE NULL
END;

UPDATE device_memberships
SET desired_present = CASE
    WHEN status = 'ADDED' THEN TRUE
    WHEN status = 'REMOVED' THEN FALSE
    ELSE NULL
END;

CREATE INDEX idx_user_memberships_observed_group
    ON user_memberships (group_ref)
    WHERE observed_present IS TRUE;

CREATE INDEX idx_device_memberships_observed_group
    ON device_memberships (group_ref)
    WHERE observed_present IS TRUE;
