package model

import "time"

// Event is a single message within a channel. The Offset field is a monotonically
// increasing sequence (BIGSERIAL) used for replay — subscribers reconnect at their
// last-seen offset to receive missed events without duplicates.
type Event struct {
	ID        string    `json:"id"`
	ChannelID string    `json:"channel_id"`
	Offset    int64     `json:"offset"`
	Payload   []byte    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}
