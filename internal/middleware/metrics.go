package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	
	"github.com/Anshum77/StreamBridge/internal/metrics"
)

// MetricsRecords tracks the duration of all HTTP requests.
// It intercepts the request, records the start time, waits for the handler to finish,
// and then logs the total duration to Prometheus, partitioned by method, path, and status code.
func MetricsRecorder() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Process request
		c.Next()

		// Record duration
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		
		metrics.HTTPRequestDuration.WithLabelValues(c.Request.Method, c.FullPath(), status).Observe(duration)
	}
}
