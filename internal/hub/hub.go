package hub

import "github.com/rs/zerolog"

// Message carries an event payload to all subscribers of a specific channel.
type Message struct {
	ChannelID string
	Data      []byte
}

// Hub is the central coordinator for all WebSocket connections.
// A single-goroutine event loop owns the client map; all mutations
// flow through channels, eliminating the need for a mutex.
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

// Broadcast fans out a message to all subscribers of the given channel.
// Goroutine-safe; dispatched through the Hub's internal event loop.
func (h *Hub) Broadcast(channelID string, data []byte) {
	h.broadcast <- Message{ChannelID: channelID, Data: data}
}

// Run starts the Hub's event loop in a dedicated goroutine.
// Processes register/unregister/broadcast events sequentially to avoid races.
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
					// Queued for delivery.
				default:
					// Send buffer full — evict slow consumer to protect throughput.
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
