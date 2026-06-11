-- Events table: persists every published message for durable delivery and replay.
-- The "offset" column (BIGSERIAL) gives each event a monotonically increasing position
-- in the stream, enabling subscribers to resume from their last-seen offset.

CREATE TABLE IF NOT EXISTS events (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id UUID        NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    "offset"   BIGSERIAL   NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Primary replay query: SELECT ... WHERE channel_id = $1 AND "offset" > $2 ORDER BY "offset"
-- This composite index makes that query an index-only scan.
CREATE INDEX IF NOT EXISTS events_channel_offset_idx ON events (channel_id, "offset");
