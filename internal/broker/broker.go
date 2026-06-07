// Package broker implements the in-memory pub/sub event broker.
// It receives published events and fans them out to WebSocket subscribers.
package broker

// Event represents a message published to a specific channel.
type Event struct {
	ChannelID string
	Payload   []byte
}
