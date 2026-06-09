package hub

import "github.com/rs/zerolog"

// Message carries an event payload to all subscribers of a specific channel.
type Message struct {
	ChannelID string
	Data      []byte
}

// Hub is the central coordinator for all WebSocket connections.
// It runs a single event loop goroutine that owns the client map — all
// register/unregister/broadcast operations go through channels, so no
// mutex is needed. This is the idiomatic Go concurrency pattern.
type Hub struct {
	// Channel-scoped client registry: channelID → set of connected clients.
	clients map[string]map[*Client]bool

	// Inbound channels for the event loop.
	register   chan *Client
	unregister chan *Client
	broadcast  chan Message

	logger zerolog.Logger
}

// NewHub creates a Hub instance. Called once at server startup.
func NewHub(logger zerolog.Logger) *Hub {
	return &Hub{
		clients:    make(map[string]map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan Message),
		logger:     logger,
	}
}

// Run starts the Hub's event loop. Must be called in its own goroutine (go hub.Run()).
// It blocks forever, processing register/unregister/broadcast events sequentially.
// Sequential processing on a single goroutine eliminates race conditions on the client map.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			// Create the channel's client set if this is the first subscriber.
			if h.clients[client.channelID] == nil {
				h.clients[client.channelID] = make(map[*Client]bool)
			}
			h.clients[client.channelID][client] = true

			h.logger.Info().
				Str("channel", client.channelID).
				Int("subscribers", len(h.clients[client.channelID])).
				Msg("client registered")

		case client := <-h.unregister:
			if subscribers, ok := h.clients[client.channelID]; ok {
				if _, exists := subscribers[client]; exists {
					delete(subscribers, client)
					close(client.send)

					// Clean up empty channel entries to prevent map growth over time.
					if len(subscribers) == 0 {
						delete(h.clients, client.channelID)
					}

					h.logger.Info().
						Str("channel", client.channelID).
						Int("subscribers", len(h.clients[client.channelID])).
						Msg("client unregistered")
				}
			}

		case msg := <-h.broadcast:
			subscribers := h.clients[msg.ChannelID]
			for client := range subscribers {
				select {
				case client.send <- msg.Data:
					// Message queued for delivery via writePump.
				default:
					// Send buffer full — client is too slow. Drop it to protect
					// the Hub from blocking on one slow consumer.
					close(client.send)
					delete(subscribers, client)
					h.logger.Warn().
						Str("channel", client.channelID).
						Msg("dropped slow client")
				}
			}
		}
	}
}
