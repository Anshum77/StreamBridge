package model

import "time"

// Tenant represents a customer account with isolated resources and enforced quotas.
type Tenant struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	ChannelLimit int       `json:"channel_limit"`
	WSLimit      int       `json:"ws_limit"`
	RateLimit    int       `json:"rate_limit"`
	RateWindow   int       `json:"rate_window"` // Rate window duration in seconds
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// APIKey associates an authentication token hash with a specific tenant.
type APIKey struct {
	KeyHash   string    `json:"-"` // Omitted from JSON responses for security
	TenantID  string    `json:"tenant_id"`
	CreatedAt time.Time `json:"created_at"`
}
