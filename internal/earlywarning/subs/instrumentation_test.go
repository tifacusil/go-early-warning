package subs

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/AngelloFD/go-early-warning/pkg/grpcx"
	"github.com/AngelloFD/go-early-warning/pkg/metrics"
)

// newResolverForMetrics builds a Resolver wired only for the instrumentation
// paths (no gRPC connection): these tests exercise publishBreakerState /
// recordOutcome, not the RPC itself.
func newResolverForMetrics() (*Resolver, *metrics.Metrics) {
	mx := metrics.New("early-warning-test")
	return &Resolver{caller: grpcx.New(grpcx.Options{}), mx: mx}, mx
}

// The gauge must EXIST for target="user-management". The bug this guards
// against is silent: the series was simply absent, so a chaos run could not
// tell "breaker stayed closed" from "breaker was never measured".
func TestPublishBreakerStateEmitsUserManagementSeries(t *testing.T) {
	r, mx := newResolverForMetrics()

	if got := testutil.CollectAndCount(mx.BreakerState); got != 0 {
		t.Fatalf("gauge no debe tener series antes de la primera llamada, hay %d", got)
	}

	r.publishBreakerState()

	if got := testutil.CollectAndCount(mx.BreakerState); got != 1 {
		t.Fatalf("se esperaba 1 serie en ew_circuit_breaker_state, hay %d", got)
	}
	if got := testutil.ToFloat64(mx.BreakerState.WithLabelValues("user-management")); got != 0 {
		t.Errorf("breaker recien creado debe reportar 0 (closed), reporta %v", got)
	}
}

// A nil Metrics must be a no-op, not a panic: tests and any caller that does
// not collect metrics pass nil.
func TestInstrumentationNilMetricsIsNoop(t *testing.T) {
	r := &Resolver{caller: grpcx.New(grpcx.Options{}), mx: nil}
	r.publishBreakerState()
	r.recordOutcome(errors.New("boom"))
}

func TestRecordOutcomeClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"exito", nil, "ok"},
		{"circuito abierto", grpcx.ErrCircuitOpen, "circuit_open"},
		{"deadline gRPC", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), "timeout"},
		{"deadline de context", context.DeadlineExceeded, "timeout"},
		{"otro error", status.Error(codes.Unavailable, "down"), "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, mx := newResolverForMetrics()
			r.recordOutcome(tc.err)

			if got := testutil.ToFloat64(mx.OutboundCalls.WithLabelValues("user-management", tc.want)); got != 1 {
				t.Fatalf("outcome %q: se esperaba 1, hay %v", tc.want, got)
			}
			if got := testutil.CollectAndCount(mx.OutboundCalls); got != 1 {
				t.Errorf("solo debe incrementarse un outcome, hay %d series", got)
			}
		})
	}
}

// The counter is what tells "the delay was too weak" (calls landed as "ok")
// apart from "the alert path never called out" (no series at all). Both
// readings must survive a breaker that never opens.
func TestOutboundCallsDistinguishesWeakInjectionFromNoTraffic(t *testing.T) {
	r, mx := newResolverForMetrics()

	if got := testutil.CollectAndCount(mx.OutboundCalls); got != 0 {
		t.Fatalf("sin llamadas no debe haber series, hay %d", got)
	}

	for i := 0; i < 3; i++ {
		r.recordOutcome(nil)
	}
	r.recordOutcome(status.Error(codes.DeadlineExceeded, "slow"))

	if got := testutil.ToFloat64(mx.OutboundCalls.WithLabelValues("user-management", "ok")); got != 3 {
		t.Errorf("se esperaban 3 llamadas ok, hay %v", got)
	}
	if got := testutil.ToFloat64(mx.OutboundCalls.WithLabelValues("user-management", "timeout")); got != 1 {
		t.Errorf("se esperaba 1 timeout, hay %v", got)
	}
}
