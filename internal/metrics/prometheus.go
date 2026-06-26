package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ActiveConnections tracks the number of currently open WebSocket connections across all channels.
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streambridge_ws_connections_active",
		Help: "Currently open WebSocket connections",
	})

	// EventsPublished tracks the total number of events successfully persisted and published.
	EventsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streambridge_events_published_total",
		Help: "Total number of events published",
	})

	// RateLimitHits tracks how many times API requests were rejected with 429 Too Many Requests.
	RateLimitHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streambridge_rate_limit_hits_total",
		Help: "Total number of 429 Too Many Requests responses",
	})

	// HTTPRequestDuration tracks the latency of HTTP API requests, labeled by method, path, and status code.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "streambridge_http_request_duration_seconds",
		Help:    "Latency of HTTP API requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path", "status"})
)
