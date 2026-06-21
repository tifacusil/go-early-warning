// Package subs resolves FCM device tokens from user-management, ported from the
// subscription logic of redis_cache.py (push_enabled, min_alert_level, quiet
// hours). HTTP call uses an explicit timeout (CLAUDE.md §1.5); the circuit
// breaker is layered in Hito 3.
package subs

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

var levelValue = map[string]int{"blue": 1, "orange": 2, "red": 3}

// Config holds user-management connection settings.
type Config struct {
	BaseURL string
	APIKey  string
	Timeout time.Duration
}

// Subscription mirrors contracts/openapi/user-management.yaml.
type Subscription struct {
	ID              string   `json:"id"`
	ProjectID       string   `json:"project_id"`
	PushEnabled     bool     `json:"push_enabled"`
	MinAlertLevel   string   `json:"min_alert_level"`
	QuietHoursStart string   `json:"quiet_hours_start"`
	QuietHoursEnd   string   `json:"quiet_hours_end"`
	FCMTokens       []string `json:"fcm_tokens"`
}

// Resolver fetches and filters subscriptions.
type Resolver struct {
	cfg  Config
	http *http.Client
	now  func() time.Time
}

// New builds a Resolver.
func New(cfg Config) *Resolver {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &Resolver{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}, now: time.Now}
}

func (r *Resolver) fetch(ctx context.Context, projectID string) ([]Subscription, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.BaseURL+"/api/v1/users/subscriptions", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Api-Key", r.cfg.APIKey)
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var all []Subscription
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		return nil, err
	}
	out := all[:0]
	for _, s := range all {
		if s.ProjectID == projectID {
			out = append(out, s)
		}
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
