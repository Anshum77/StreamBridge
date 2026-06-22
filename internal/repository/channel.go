package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anshum77/StreamBridge/internal/model"
)

// ChannelRepo manages data access for channels, strictly enforcing tenant isolation.
type ChannelRepo struct {
	db *pgxpool.Pool
}

// NewChannelRepo initializes a repository for channel management.
func NewChannelRepo(db *pgxpool.Pool) *ChannelRepo {
	return &ChannelRepo{db: db}
}

// List returns all channels belonging to a specific tenant, ordered newest first.
func (r *ChannelRepo) List(ctx context.Context, tenantID string) ([]model.Channel, error) {
	query := `
		SELECT id, tenant_id, name, created_at, updated_at
		FROM channels
		WHERE tenant_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.db.Query(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	channels := make([]model.Channel, 0)
	for rows.Next() {
		var c model.Channel
		if err := rows.Scan(&c.ID, &c.TenantID, &c.Name, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		channels = append(channels, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return channels, nil
}

// Create provisions a new channel under the given tenant's namespace.
func (r *ChannelRepo) Create(ctx context.Context, tenantID, name string) (model.Channel, error) {
	query := `
		INSERT INTO channels (tenant_id, name)
		VALUES ($1, $2)
		RETURNING id, tenant_id, name, created_at, updated_at
	`
	var c model.Channel
	err := r.db.QueryRow(ctx, query, tenantID, name).Scan(&c.ID, &c.TenantID, &c.Name, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

// Get looks up a single channel, enforcing tenant ownership.
func (r *ChannelRepo) Get(ctx context.Context, tenantID, channelID string) (model.Channel, error) {
	query := `
		SELECT id, tenant_id, name, created_at, updated_at
		FROM channels
		WHERE id = $1 AND tenant_id = $2
	`
	var c model.Channel
	err := r.db.QueryRow(ctx, query, channelID, tenantID).Scan(&c.ID, &c.TenantID, &c.Name, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

// Update replaces the channel name, enforcing tenant ownership.
func (r *ChannelRepo) Update(ctx context.Context, tenantID, channelID, name string) (model.Channel, error) {
	query := `
		UPDATE channels
		SET name = $3, updated_at = now()
		WHERE id = $1 AND tenant_id = $2
		RETURNING id, tenant_id, name, created_at, updated_at
	`
	var c model.Channel
	err := r.db.QueryRow(ctx, query, channelID, tenantID, name).Scan(&c.ID, &c.TenantID, &c.Name, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

// Delete removes a channel, enforcing tenant ownership. Returns rows affected.
func (r *ChannelRepo) Delete(ctx context.Context, tenantID, channelID string) (int64, error) {
	query := "DELETE FROM channels WHERE id = $1 AND tenant_id = $2"
	result, err := r.db.Exec(ctx, query, channelID, tenantID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
