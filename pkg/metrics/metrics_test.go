package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerExposesCustomAndProcessMetrics(t *testing.T) {
	m := New("early-warning")
	m.MessagesConsumed.Inc()
	m.AlertsPublished.WithLabelValues("red").Inc()
	m.Duplicates.Inc()
	m.BreakerState.WithLabelValues("data-analysis").Set(1)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	out := string(body)

	for _, want := range []string{
		"ew_messages_consumed_total",
		`ew_alerts_published_total{level="red",service="early-warning"} 1`,
		"ew_duplicates_skipped_total",
		`ew_circuit_breaker_state{service="early-warning",target="data-analysis"} 1`,
		"process_resident_memory_bytes",
		"go_goroutines",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestMiddlewareRecordsStatusAndLatency(t *testing.T) {
	m := New("early-warning")
	handler := m.Middleware("/alerts/recent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/alerts/recent", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("handler status = %d, want 404 (middleware must not swallow it)", rec.Code)
	}

	out := scrapeBody(t, m)
	for _, want := range []string{
		`http_requests_total{method="GET",route="/alerts/recent",status="404"} 1`,
		`http_request_duration_seconds_count{method="GET",route="/alerts/recent",status="404"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\n---\n%s", want, out)
		}
	}
}

// Guards against going back to a native-only histogram: without classic
// buckets Prometheus only sees le="+Inf" and percentiles come out NaN.
func TestEvalDurationHasClassicBuckets(t *testing.T) {
	m := New("early-warning")
	m.EvalDuration.Observe(9e-6)

	out := scrapeBody(t, m)
	for _, want := range []string{
		`ew_eval_duration_seconds_bucket{service="early-warning",le="1e-06"} 0`,
		`ew_eval_duration_seconds_bucket{service="early-warning",le="1.5625e-05"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

// A degraded dispatch with the breaker still closed takes ~21s (3 x 5s timeout
// + 1s + 5s backoff). It must land in a finite bucket, or the O6 degradation
// latency reads as +Inf and histogram_quantile cannot place it.
func TestAlertDispatchDurationResolvesDegradedLatency(t *testing.T) {
	m := New("early-warning")
	m.AlertDispatchDuration.WithLabelValues(PushPathResolveFailed).Observe(21)

	out := scrapeBody(t, m)
	for _, want := range []string{
		`ew_alert_dispatch_duration_seconds_bucket{push_path="resolve_failed",service="early-warning",le="20"} 0`,
		`ew_alert_dispatch_duration_seconds_bucket{push_path="resolve_failed",service="early-warning",le="30"} 1`,
		`ew_alert_dispatch_duration_seconds_count{push_path="resolve_failed",service="early-warning"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func scrapeBody(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	return string(body)
}
