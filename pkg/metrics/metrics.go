// Package metrics exposes Prometheus metrics on /metrics per CLAUDE.md §5:
// CPU, resident memory (process collector), throughput, latency percentiles,
// published/dropped counters, and circuit-breaker state.
package metrics

import (
	"net/http"

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
	HTTPDuration     *prometheus.HistogramVec
	BreakerState     *prometheus.GaugeVec // 0 closed / 1 open / 2 half_open, by target
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
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ew_http_request_duration_seconds", Help: "HTTP handler latency.", ConstLabels: cl,
			NativeHistogramBucketFactor: 1.1,
		}, []string{"route", "method"}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ew_circuit_breaker_state", Help: "0 closed, 1 open, 2 half_open.", ConstLabels: cl,
		}, []string{"target"}),
	}
	reg.MustRegister(m.MessagesConsumed, m.MessagesDropped, m.Duplicates,
		m.AlertsPublished, m.EvalDuration, m.HTTPDuration, m.BreakerState)
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
