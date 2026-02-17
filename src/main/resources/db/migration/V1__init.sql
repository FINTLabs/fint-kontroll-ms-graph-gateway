CREATE TABLE users
(
    id           BIGSERIAL PRIMARY KEY,
    object_id    UUID        NOT NULL UNIQUE,
    checksum     BYTEA       NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    not_seen_count INT DEFAULT 0
);
CREATE UNIQUE INDEX uq_entra_user_object_id ON users (object_id);
CREATE UNIQUE INDEX uq_entra_user_id ON users (id);
CREATE INDEX IF NOT EXISTS idx_entra_user_checkpoint_last_seen
    ON users (last_seen_at);

CREATE TABLE users_external
(
    id           BIGSERIAL PRIMARY KEY,
    object_id    UUID        NOT NULL UNIQUE,
    checksum     BYTEA       NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    not_seen_count INT DEFAULT 0
);
CREATE UNIQUE INDEX uq_entra_user_ext_object_id ON users (object_id);
CREATE UNIQUE INDEX uq_entra_user_ext_id ON users (id);
CREATE INDEX IF NOT EXISTS idx_entra_user_ext_checkpoint_last_seen
    ON users_external (last_seen_at);



CREATE TABLE IF NOT EXISTS delta_state
(
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT UNIQUE,
    delta_link TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);