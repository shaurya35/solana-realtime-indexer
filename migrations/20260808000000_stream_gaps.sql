CREATE TABLE stream_gaps (
  gap_id          BIGSERIAL PRIMARY KEY,
  start_slot      BIGINT NOT NULL,
  end_slot        BIGINT NOT NULL,
  missed_slots    BIGINT NOT NULL,
  detected_at     TIMESTAMPTZ NOT NULL,
  recovered_at    TIMESTAMPTZ,
  recovery_method TEXT,
  status          TEXT NOT NULL DEFAULT 'open',
  CONSTRAINT stream_gaps_status_check CHECK (status IN ('open', 'recovering', 'closed'))
);

CREATE INDEX stream_gaps_status_idx ON stream_gaps (status);