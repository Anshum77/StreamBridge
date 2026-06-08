// Package hub manages WebSocket connections and real-time event fan-out.
// Each client gets a persistent, full-duplex connection to receive events
// the moment they are published — no polling required.
package hub

import (
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

// Client wraps a single WebSocket connection and the channel it subscribed to.
// The send channel buffers outgoing messages — writePump (Step 2) will drain it.
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	channelID string
	send      chan []byte
	logger    zerolog.Logger
}
