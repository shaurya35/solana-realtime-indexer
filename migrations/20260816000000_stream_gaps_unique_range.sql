DELETE FROM stream_gaps a
USING stream_gaps b
WHERE a.gap_id > b.gap_id
  AND a.start_slot = b.start_slot
  AND a.end_slot = b.end_slot;

CREATE UNIQUE INDEX stream_gaps_range_idx ON stream_gaps (start_slot, end_slot);