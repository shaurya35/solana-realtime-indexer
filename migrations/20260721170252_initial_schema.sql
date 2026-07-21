CREATE TABLE events (
  signature      TEXT   NOT NULL,
  absolute_path  BYTEA  NOT NULL,
  event_ordinal  INT    NOT NULL,
  slot           BIGINT NOT NULL,
  block_time     BIGINT,
  program        TEXT   NOT NULL,
  event_type     TEXT   NOT NULL,
  payload        JSONB  NOT NULL,
  parser_version INT    NOT NULL DEFAULT 1,
  received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (signature, absolute_path, event_ordinal)
);

CREATE INDEX events_slot_idx ON events (slot);

CREATE TABLE trades (
  signature      TEXT   NOT NULL,
  absolute_path  BYTEA  NOT NULL,
  event_ordinal  INT    NOT NULL,
  slot           BIGINT NOT NULL,
  block_time     BIGINT,
  program        TEXT   NOT NULL,
  pool           TEXT,
  token_mint     TEXT   NOT NULL,
  side           TEXT   NOT NULL,
  sol_amount     BIGINT NOT NULL,
  token_amount   BIGINT NOT NULL,
  trader         TEXT   NOT NULL,
  fee            BIGINT,
  PRIMARY KEY (signature, absolute_path, event_ordinal),
  FOREIGN KEY (signature, absolute_path, event_ordinal)
    REFERENCES events (signature, absolute_path, event_ordinal),
  CONSTRAINT trades_side_check CHECK (side IN ('buy', 'sell'))
);

CREATE INDEX trades_mint_slot_idx ON trades (token_mint, slot DESC);
CREATE INDEX trades_slot_idx      ON trades (slot DESC);

CREATE TABLE pools (
  pool           TEXT PRIMARY KEY,
  base_mint      TEXT NOT NULL,
  quote_mint     TEXT NOT NULL,
  base_decimals  INT  NOT NULL,
  quote_decimals INT  NOT NULL,
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE ingestion_checkpoints (
  id                       INT PRIMARY KEY DEFAULT 1,
  last_completed_slot      BIGINT NOT NULL,
  last_completed_signature TEXT,
  updated_at               TIMESTAMPTZ DEFAULT now(),
  CONSTRAINT single_row CHECK (id = 1)
);