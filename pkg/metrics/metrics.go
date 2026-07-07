// Package metrics exposes Prometheus metrics on /metrics per CLAUDE.md §5:
// CPU, resident memory (process collector), throughput, latency percentiles,
// published/dropped counters, and circuit-breaker state.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the custom collectors for a service. Process CPU/memory come
// from the default process collector, registered alongside.
type Metrics struct {
	reg *prometheus.Registry

	MessagesConsumed prometheus.Counter
	MessagesDropped  prometheus.Counter // malformed / dead-lettered
	Duplicates       prometheus.Counter
	AlertsPublished  *prometheus.CounterVec // by level
	EvalDuration     prometheus.Histogram   // seconds; percentiles via histogram_quantile
	BreakerState     *prometheus.GaugeVec   // 0 closed / 1 open / 2 half_open, by target

	// AlertDeliveryLatency measures the controllable span: threshold crossed
	// (Alert.EvaluatedAt) -> FCM accepts the multicast (HTTP 200 + message ID).
	// Fine buckets since this path is expected to complete in low seconds.
	AlertDeliveryLatency prometheus.Histogram

	// HTTP: same metric name as data-management/project-management/
	// user-management (http_request_duration_seconds, http_requests_total) so
	// the Grafana A-vs-B dashboard can aggregate every Go service the same way.
	httpReqTotal    *prometheus.CounterVec
	httpReqDuration *prometheus.HistogramVec
}

// New builds and registers all metrics for a service. The service name is a
// constant label so multiple instances are distinguishable when scraped.
func New(service string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	cl := prometheus.Labels{"service": service}

	m := &Metrics{
		reg: reg,
		MessagesConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ew_messages_consumed_total", Help: "Sensor messages consumed.", ConstLabels: cl,
		}),
		MessagesDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ew_messages_dropped_total", Help: "Messages dropped (malformed or dead-lettered).", ConstLabels: cl,
		}),
		Duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ew_duplicates_skipped_total", Help: "Duplicate messages skipped by idempotency.", ConstLabels: cl,
		}),
		AlertsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ew_alerts_published_total", Help: "Alerts published, by level.", ConstLabels: cl,
		}, []string{"level"}),
		EvalDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ew_eval_duration_seconds", Help: "Message evaluation latency.", ConstLabels: cl,
			NativeHistogramBucketFactor: 1.1,
		}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ew_circuit_breaker_state", Help: "0 closed, 1 open, 2 half_open.", ConstLabels: cl,
		}, []string{"target"}),
		AlertDeliveryLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "ew_alert_delivery_latency_seconds",
			Help:        "Threshold crossed -> FCM multicast accepted (message ID assigned).",
			ConstLabels: cl,
			Buckets:     []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		httpReqTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total de requests HTTP por metodo, ruta y codigo.",
		}, []string{"method", "route", "status"}),
		httpReqDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Latencia de requests HTTP. Buckets default cubren p50..p99.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
	}
	reg.MustRegister(m.MessagesConsumed, m.MessagesDropped, m.Duplicates,
		m.AlertsPublished, m.EvalDuration, m.BreakerState, m.AlertDeliveryLatency,
		m.httpReqTotal, m.httpReqDuration)
	return m
}

// Middleware measures every request that reaches the mux: increments the
// counter and observes latency. Wraps the whole mux (not per-handler
// instrumentation) so a new route cannot slip through unmeasured. route is
// resolved by the caller (net/http has no route pattern of its own).
func (m *Metrics) Middleware(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(sw, r)

		status := strconv.Itoa(sw.status)
		m.httpReqTotal.WithLabelValues(r.Method, route, status).Inc()
		m.httpReqDuration.WithLabelValues(r.Method, route, status).Observe(time.Since(start).Seconds())
	}
}

// statusWriter captures the status code written by the handler; net/http's
// ResponseWriter does not expose it after the fact.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
