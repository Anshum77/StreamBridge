// Package model defines the core domain types shared across handlers, broker, and persistence.
package model

import "time"

// Channel is a named topic that publishers send events to and subscribers listen on.
type Channel struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
