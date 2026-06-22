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
