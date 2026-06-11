// Package repository provides data access methods for domain entities.
// Separates SQL logic from HTTP handlers, keeping both layers independently testable.
package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anshum77/StreamBridge/internal/model"
)

// EventRepo handles all database operations for events.
type EventRepo struct {
	db *pgxpool.Pool
}

// NewEventRepo creates an EventRepo backed by the given connection pool.
func NewEventRepo(db *pgxpool.Pool) *EventRepo {
	return &EventRepo{db: db}
}

// Insert persists a new event and returns it with the database-generated offset and ID.
// The offset is assigned by PostgreSQL's BIGSERIAL, guaranteeing global ordering.
func (r *EventRepo) Insert(ctx context.Context, channelID string, payload []byte) (model.Event, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var event model.Event
	err := r.db.QueryRow(ctx, `
		INSERT INTO events (channel_id, payload)
		VALUES ($1, $2)
		RETURNING id, channel_id, "offset", payload, created_at
	`, channelID, payload).Scan(
		&event.ID,
		&event.ChannelID,
		&event.Offset,
		&event.Payload,
		&event.CreatedAt,
	)

	return event, err
}

// ListAfterOffset returns events for a channel with offset greater than afterOffset,
// ordered by offset ascending. Used for replay on client reconnect.
func (r *EventRepo) ListAfterOffset(ctx context.Context, channelID string, afterOffset int64, limit int) ([]model.Event, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if limit <= 0 || limit > 100 {
		limit = 50
	}

	rows, err := r.db.Query(ctx, `
		SELECT id, channel_id, "offset", payload, created_at
		FROM events
		WHERE channel_id = $1 AND "offset" > $2
		ORDER BY "offset" ASC
		LIMIT $3
	`, channelID, afterOffset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]model.Event, 0, limit)
	for rows.Next() {
		var e model.Event
		if err := rows.Scan(&e.ID, &e.ChannelID, &e.Offset, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	return events, rows.Err()
}
