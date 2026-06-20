// Package httpx provides an outbound HTTP client wrapping every inter-service
// call with an explicit timeout, retry+exponential backoff, and a circuit
// breaker, per CLAUDE.md §1.5 / §4. Never use the default http.Client.
package httpx

import (
	"errors"
	"net/http"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when the breaker is open and rejects a call.
var ErrCircuitOpen = errors.New("httpx: circuit open")

// State is the circuit-breaker state, exposed via /metrics (CLAUDE.md §2.3).
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Options configures a resilient client.
type Options struct {
	Timeout      time.Duration // explicit per-request timeout (required)
	MaxRetries   int           // additional attempts after the first
	Backoff      []time.Duration
	FailThreshold int          // consecutive failures before the breaker opens
	OpenFor      time.Duration // cool-down before half-open probe
}

// DefaultBackoff matches the CLAUDE.md §4 schedule: 1s / 5s / 30s.
var DefaultBackoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

// Client is a resilient HTTP client. Zero value is not usable; use New.
type Client struct {
	opts Options
	http *http.Client

	mu          sync.Mutex
	state       State
	failures    int
	openedAt    time.Time
}

// New builds a Client; unset Backoff/Timeout/thresholds get sane defaults.
func New(o Options) *Client {
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if len(o.Backoff) == 0 {
		o.Backoff = DefaultBackoff
	}
	if o.FailThreshold <= 0 {
		o.FailThreshold = 5
	}
	if o.OpenFor <= 0 {
		o.OpenFor = 30 * time.Second
	}
	return &Client{opts: o, http: &http.Client{Timeout: o.Timeout}}
}

// State returns the current breaker state (for /metrics and /health).
func (c *Client) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// TODO(SE-practice): implement Do(req) with retry+backoff and breaker transitions
// (closed → open after FailThreshold; open → half-open after OpenFor; half-open
// success → closed). Skeleton only; filled in Hito 3.
