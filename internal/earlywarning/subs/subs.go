// Package subs resolves FCM device tokens from user-management over gRPC,
// applying the push_enabled / min_alert_level / quiet-hours rules ported from
// redis_cache.py. The gRPC call is guarded by timeout + retry + circuit breaker
// (CLAUDE.md §1.5 / §4).
package subs

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	umv1 "github.com/AngelloFD/go-early-warning/internal/earlywarning/grpc/usermanagement/v1"
	"github.com/AngelloFD/go-early-warning/pkg/grpcx"
	"github.com/AngelloFD/go-early-warning/pkg/metrics"
)

// breakerTarget labels this resolver's breaker in ew_circuit_breaker_state.
// user-management is the only synchronous dependency on the ALERT path, so it
// is the only breaker whose opening can be exercised by delaying a dependency
// of that path — data-analysis sits on the report path instead.
const breakerTarget = "user-management"

var levelValue = map[string]int{"blue": 1, "orange": 2, "red": 3}

// Subscription is the resolved view used by the filter logic.
type Subscription struct {
	ID              string
	ProjectID       string
	PushEnabled     bool
	MinAlertLevel   string
	QuietHoursStart string
	QuietHoursEnd   string
	FCMTokens       []string
}

// Resolver fetches and filters subscriptions via the user-management gRPC API.
type Resolver struct {
	client umv1.SubscriptionServiceClient
	caller *grpcx.Caller
	now    func() time.Time
	// mx publishes the breaker state to /metrics. Optional: nil disables the
	// gauge (tests, and any caller that does not collect metrics).
	mx *metrics.Metrics
}

// New builds a Resolver over an existing gRPC connection. caller carries the
// timeout/retry/breaker policy. mx may be nil to skip metrics.
func New(conn grpc.ClientConnInterface, caller *grpcx.Caller, mx *metrics.Metrics) *Resolver {
	return &Resolver{
		client: umv1.NewSubscriptionServiceClient(conn),
		caller: caller,
		now:    time.Now,
		mx:     mx,
	}
}

// BreakerState exposes the breaker state as a string, for /health.
func (r *Resolver) BreakerState() string { return r.caller.State().String() }

// publishBreakerState mirrors the breaker into ew_circuit_breaker_state
// (0 closed / 1 open / 2 half_open), the same gauge the data-analysis caller
// feeds from api.enrich. Without this the series simply does not exist for
// user-management and a chaos run cannot tell "breaker stayed closed" from
// "breaker was never measured" — which is what invalidated the earlier
// data-analysis scenario.
func (r *Resolver) publishBreakerState() {
	if r.mx == nil {
		return
	}
	r.mx.BreakerState.WithLabelValues(breakerTarget).Set(float64(r.caller.State()))
}

// recordOutcome counts one outbound call. "timeout" is the outcome that makes
// the breaker advance, so it is kept apart from a generic error: a delay
// injection that does not push calls past USER_MANAGEMENT_TIMEOUT_MS produces
// "ok", and that is the difference between "the injection was too weak" and
// "the alert path never called out at all".
func (r *Resolver) recordOutcome(err error) {
	if r.mx == nil {
		return
	}
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, grpcx.ErrCircuitOpen):
		outcome = "circuit_open"
	case errors.Is(err, context.DeadlineExceeded),
		status.Code(err) == codes.DeadlineExceeded:
		outcome = "timeout"
	default:
		outcome = "error"
	}
	r.mx.OutboundCalls.WithLabelValues(breakerTarget, outcome).Inc()
}

func (r *Resolver) fetch(ctx context.Context, projectID string) ([]Subscription, error) {
	var resp *umv1.GetSubscriptionsResponse
	err := r.caller.Invoke(ctx, func(c context.Context) error {
		var e error
		resp, e = r.client.GetSubscriptions(c, &umv1.GetSubscriptionsRequest{ProjectId: projectID})
		return e
	})
	// Sampled after every call, success or failure, so the gauge tracks the
	// open -> half_open -> closed transitions the recovery time is read from.
	r.publishBreakerState()
	r.recordOutcome(err)
	if err != nil {
		return nil, err
	}
	out := make([]Subscription, 0, len(resp.GetSubscriptions()))
	for _, s := range resp.GetSubscriptions() {
		out = append(out, Subscription{
			ID:              s.GetId(),
			ProjectID:       s.GetProjectId(),
			PushEnabled:     s.GetPushEnabled(),
			MinAlertLevel:   s.GetMinAlertLevel(),
			QuietHoursStart: s.GetQuietHoursStart(),
			QuietHoursEnd:   s.GetQuietHoursEnd(),
			FCMTokens:       s.GetFcmTokens(),
		})
	}
	return out, nil
}

// TokensFor returns deduped FCM tokens eligible for an alert at alertLevel,
// applying push_enabled, min_alert_level and quiet-hours rules. red overrides
// quiet hours (matches Python).
func (r *Resolver) TokensFor(ctx context.Context, projectID, alertLevel string) ([]string, error) {
	if projectID == "" || levelValue[alertLevel] == 0 {
		return nil, nil
	}
	subs, err := r.fetch(ctx, projectID)
	if err != nil {
		return nil, err
	}
	now := r.now().UTC()
	seen := map[string]struct{}{}
	var tokens []string
	for _, s := range subs {
		if !s.PushEnabled {
			continue
		}
		if levelValue[s.MinAlertLevel] == 0 || levelValue[s.MinAlertLevel] > levelValue[alertLevel] {
			continue
		}
		if alertLevel != "red" && inQuietHours(s.QuietHoursStart, s.QuietHoursEnd, now) {
			continue
		}
		for _, t := range s.FCMTokens {
			if t == "" {
				continue
			}
			if _, dup := seen[t]; dup {
				continue
			}
			seen[t] = struct{}{}
			tokens = append(tokens, t)
		}
	}
	return tokens, nil
}

// inQuietHours reports whether now (UTC) falls in [start,end], supporting wrap
// past midnight. Empty/invalid bounds mean "not quiet".
func inQuietHours(start, end string, now time.Time) bool {
	s, okS := parseClock(start)
	e, okE := parseClock(end)
	if !okS || !okE {
		return false
	}
	cur := now.Hour()*3600 + now.Minute()*60 + now.Second()
	if s <= e {
		return cur >= s && cur <= e
	}
	return cur >= s || cur <= e
}

func parseClock(v string) (int, bool) {
	if v == "" {
		return 0, false
	}
	t, err := time.Parse("15:04:05", v)
	if err != nil {
		if t, err = time.Parse("15:04", v); err != nil {
			return 0, false
		}
	}
	return t.Hour()*3600 + t.Minute()*60 + t.Second(), true
}
