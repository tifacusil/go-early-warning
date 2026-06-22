// Package subs resolves FCM device tokens from user-management over gRPC,
// applying the push_enabled / min_alert_level / quiet-hours rules ported from
// redis_cache.py. The gRPC call is guarded by timeout + retry + circuit breaker
// (CLAUDE.md §1.5 / §4).
package subs

import (
	"context"
	"time"

	"google.golang.org/grpc"

	umv1 "github.com/AngelloFD/go-early-warning/internal/earlywarning/grpc/usermanagement/v1"
	"github.com/AngelloFD/go-early-warning/pkg/grpcx"
)

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
}

// New builds a Resolver over an existing gRPC connection. caller carries the
// timeout/retry/breaker policy.
func New(conn grpc.ClientConnInterface, caller *grpcx.Caller) *Resolver {
	return &Resolver{
		client: umv1.NewSubscriptionServiceClient(conn),
		caller: caller,
		now:    time.Now,
	}
}

// BreakerState exposes the breaker state for /metrics.
func (r *Resolver) BreakerState() string { return r.caller.State().String() }

func (r *Resolver) fetch(ctx context.Context, projectID string) ([]Subscription, error) {
	var resp *umv1.GetSubscriptionsResponse
	err := r.caller.Invoke(ctx, func(c context.Context) error {
		var e error
		resp, e = r.client.GetSubscriptions(c, &umv1.GetSubscriptionsRequest{ProjectId: projectID})
		return e
	})
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
