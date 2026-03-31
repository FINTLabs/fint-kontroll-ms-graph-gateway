DROP INDEX IF EXISTS uq_entra_user_ext_object_id;
DROP INDEX IF EXISTS uq_entra_user_ext_id;

CREATE UNIQUE INDEX uq_entra_user_ext_object_id ON users_external (object_id);
CREATE UNIQUE INDEX uq_entra_user_ext_id ON users_external (id);