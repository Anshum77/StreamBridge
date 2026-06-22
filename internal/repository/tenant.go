package repository

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anshum77/StreamBridge/internal/model"
)

// TenantRepo manages data access for tenants and API keys.
type TenantRepo struct {
	db *pgxpool.Pool
}

// NewTenantRepo initializes a repository for tenant management.
func NewTenantRepo(db *pgxpool.Pool) *TenantRepo {
	return &TenantRepo{db: db}
}

// CreateTenant provisions a new isolated tenant with specific resource limits.
func (r *TenantRepo) CreateTenant(ctx context.Context, name string, channelLimit, wsLimit, rateLimit, rateWindow int) (*model.Tenant, error) {
	query := `
		INSERT INTO tenants (name, channel_limit, ws_limit, rate_limit, rate_window)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, name, channel_limit, ws_limit, rate_limit, rate_window, created_at, updated_at
	`

	var t model.Tenant
	err := r.db.QueryRow(ctx, query, name, channelLimit, wsLimit, rateLimit, rateWindow).Scan(
		&t.ID, &t.Name, &t.ChannelLimit, &t.WSLimit, &t.RateLimit, &t.RateWindow, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// CreateAPIKey generates a cryptographically secure key, stores its SHA-256 hash, and returns the plaintext key.
func (r *TenantRepo) CreateAPIKey(ctx context.Context, tenantID string) (string, error) {
	rawKey, err := generateSecureKey()
	if err != nil {
		return "", err
	}

	hash := hashKey(rawKey)

	query := `
		INSERT INTO api_keys (key_hash, tenant_id)
		VALUES ($1, $2)
	`
	if _, err := r.db.Exec(ctx, query, hash, tenantID); err != nil {
		return "", err
	}

	return rawKey, nil
}

// GetTenantByAPIKey resolves a plaintext API key to a Tenant by comparing hashes.
func (r *TenantRepo) GetTenantByAPIKey(ctx context.Context, rawKey string) (*model.Tenant, error) {
	hash := hashKey(rawKey)

	query := `
		SELECT t.id, t.name, t.channel_limit, t.ws_limit, t.rate_limit, t.rate_window, t.created_at, t.updated_at
		FROM tenants t
		INNER JOIN api_keys k ON t.id = k.tenant_id
		WHERE k.key_hash = $1
	`

	var t model.Tenant
	err := r.db.QueryRow(ctx, query, hash).Scan(
		&t.ID, &t.Name, &t.ChannelLimit, &t.WSLimit, &t.RateLimit, &t.RateWindow, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// generateSecureKey creates a 32-byte cryptographically random string with a prefix.
func generateSecureKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "sb_" + hex.EncodeToString(b), nil
}

// hashKey returns the SHA-256 checksum of the input string as a hex string.
func hashKey(rawKey string) string {
	hash := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(hash[:])
}
