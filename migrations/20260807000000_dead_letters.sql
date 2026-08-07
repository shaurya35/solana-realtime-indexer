CREATE TABLE dead_letters (
  id            BIGSERIAL PRIMARY KEY,
  signature     TEXT   NOT NULL,
  absolute_path BYTEA  NOT NULL,
  event_ordinal INT    NOT NULL,
  slot          BIGINT NOT NULL,
  payload       JSONB  NOT NULL,
  error         TEXT   NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX dead_letters_signature_idx ON dead_letters (signature);