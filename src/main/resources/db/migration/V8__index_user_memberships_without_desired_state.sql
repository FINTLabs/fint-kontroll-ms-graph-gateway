CREATE INDEX idx_user_memberships_without_desired_state
    ON user_memberships (user_ref, group_ref)
    WHERE desired_present IS NULL
      AND observed_present IS TRUE;
