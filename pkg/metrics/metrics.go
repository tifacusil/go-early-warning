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

// httpBuckets are the HTTP latency histogram cutpoints, shared by the 4 Go
// services and aligned to the SLO declared in application.yml for the Spring
// services (Sistema A): 5ms,10ms,25ms,50ms,100ms,250ms,500ms,1s, plus 2.5s/5s
// to keep resolution under load. prometheus.DefBuckets would make percentiles
// non-comparable against Sistema A via histogram_quantile.
var httpBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

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

	// Evaluations counts what the threshold engine concluded, by outcome:
	// "alert", "none", or the skip reason / note it returned.
	//
	// Consumed was the only visible number, and it says nothing about how many
	// messages actually reached a threshold comparison. Under load most do
	// not: a reading that arrives out of order is discarded to protect the
	// baseline, and one that lands more than MaxDTSeconds after it resets the
	// baseline without evaluating. On the 2026-07-22 ladder, alerts per
	// consumed message came out 8x below what the corpus produces per
	// evaluation, and telling those two apart took a simulation because
	// nothing here counted evaluations.
	Evaluations *prometheus.CounterVec

	// OutboundCalls counts synchronous gRPC calls leaving this service, by
	// target and outcome ("ok", "timeout", "circuit_open", "error").
	//
	// Needed to interpret a breaker that does NOT open. The gauge alone cannot
	// tell "the delay was not enough to time the calls out" from "the alert
	// path never ran, so nothing was called" — and the second is easy to hit,
	// because user-management is only called when an alert fires, which needs
	// sensors that already have a baseline. With this counter, "breaker stayed
	// closed" comes with the number of invocations and how many timed out,
	// which is a finding instead of a blank.
	OutboundCalls *prometheus.CounterVec

	// AlertDeliveryLatency measures the controllable span: threshold crossed
	// (Alert.EvaluatedAt) -> FCM accepts the multicast (HTTP 200 + message ID).
	// Fine buckets since this path is expected to complete in low seconds.
	AlertDeliveryLatency prometheus.Histogram

	// AlertDispatchDuration measures threshold crossed (Alert.EvaluatedAt) ->
	// alert durably recorded in MySQL, by push_path (see PushPath*).
	//
	// AlertDeliveryLatency cannot measure the degraded mode: it only observes
	// when FCM accepts a push, which is exactly what does not happen while
	// user-management is down. Yet that is the window where the latency cost
	// shows up — the subscriber lookup sits BEFORE the MySQL write, so with
	// user-management timing out and the breaker still closed every alert
	// waits out timeout x attempts + backoff (~21s with the defaults) before it
	// is recorded. Once the breaker opens, the lookup fails fast and the
	// latency should drop back. push_path="resolve_failed" isolates that span;
	// its _count is also the number of alerts emitted in degraded mode.
	AlertDispatchDuration *prometheus.HistogramVec

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
		// Classic buckets are required: Prometheus runs without the
		// native-histograms feature, so a native-only histogram reached it as a
		// single +Inf bucket and histogram_quantile returned NaN. Buckets start
		// at 1µs because Evaluate is pure in-memory math (~9µs mean on the
		// 2026-09-18 run); DefBuckets starts at 5ms and would put every sample
		// in the first bucket.
		EvalDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ew_eval_duration_seconds", Help: "Threshold engine Evaluate() latency (in-memory, excludes Redis/RabbitMQ).", ConstLabels: cl,
			Buckets:                     prometheus.ExponentialBuckets(1e-6, 2.5, 12), // 1µs .. ~24ms
			NativeHistogramBucketFactor: 1.1,
		}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ew_circuit_breaker_state", Help: "0 closed, 1 open, 2 half_open.", ConstLabels: cl,
		}, []string{"target"}),
		Evaluations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "ew_evaluations_total",
			Help:        "Threshold engine outcomes: alert, none, or the skip reason.",
			ConstLabels: cl,
		}, []string{"outcome"}),
		OutboundCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "ew_outbound_calls_total",
			Help:        "Outbound sync gRPC calls by target and outcome (ok/timeout/circuit_open/error).",
			ConstLabels: cl,
		}, []string{"target", "outcome"}),
		AlertDeliveryLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "ew_alert_delivery_latency_seconds",
			Help:        "Threshold crossed -> FCM multicast accepted (message ID assigned).",
			ConstLabels: cl,
			Buckets:     []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		// Up to 120s: a degraded dispatch with the breaker still closed spends
		// ~21s in the subscriber lookup alone, and several can queue on the
		// same sensor's worker. Classic buckets for the same reason as
		// EvalDuration (Prometheus has no native histograms enabled).
		AlertDispatchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "ew_alert_dispatch_duration_seconds",
			Help:        "Threshold crossed -> alert recorded in MySQL, by push_path (delivered/no_tokens/push_failed/resolve_failed).",
			ConstLabels: cl,
			Buckets:     []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60, 120},
		}, []string{"push_path"}),
		httpReqTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total de requests HTTP por metodo, ruta y codigo.",
		}, []string{"method", "route", "status"}),
		httpReqDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Latencia de requests HTTP, buckets alineados al SLO de Sistema A -> percentiles via histogram_quantile.",
			Buckets: httpBuckets,
		}, []string{"method", "route", "status"}),
	}
	reg.MustRegister(m.MessagesConsumed, m.MessagesDropped, m.Duplicates,
		m.AlertsPublished, m.EvalDuration, m.BreakerState, m.Evaluations,
		m.OutboundCalls, m.AlertDeliveryLatency, m.AlertDispatchDuration,
		m.httpReqTotal, m.httpReqDuration)
	return m
}

// push_path values for ew_alert_dispatch_duration_seconds. A closed set, so
// the label cardinality is fixed regardless of what the alert carries.
const (
	PushPathDelivered     = "delivered"      // FCM accepted the multicast
	PushPathNoTokens      = "no_tokens"      // lookup OK, nobody eligible
	PushPathPushFailed    = "push_failed"    // lookup OK, FCM did not accept
	PushPathResolveFailed = "resolve_failed" // user-management lookup failed: degraded mode
)

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
