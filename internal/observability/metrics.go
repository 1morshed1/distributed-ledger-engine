package observability

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus metrics for the API. Registered once at package init.
var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_http_requests_total",
		Help: "Total HTTP requests by method, route template, and status.",
	}, []string{"method", "path", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ledger_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets, // .005 .. 10s
	}, []string{"method", "path"})

	// Business metric: transfers by outcome.
	TransfersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_transfers_total",
		Help: "Total transfer attempts by outcome (completed|insufficient|duplicate|error).",
	}, []string{"outcome"})
)

// GinMetrics records request count + latency. Uses the route TEMPLATE
// (c.FullPath, e.g. /api/v1/accounts/:id) as the label so per-id URLs don't
// explode cardinality.
func GinMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		status := strconv.Itoa(c.Writer.Status())
		httpRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		httpRequestDuration.WithLabelValues(c.Request.Method, path).Observe(time.Since(start).Seconds())
	}
}

// MetricsHandler serves the Prometheus exposition format at /metrics.
func MetricsHandler() gin.HandlerFunc {
	return gin.WrapH(promhttp.Handler())
}
