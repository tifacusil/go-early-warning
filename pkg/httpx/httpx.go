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

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time

	now   func() time.Time   // injectable for tests
	sleep func(time.Duration) // injectable for tests
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
	return &Client{
		opts:  o,
		http:  &http.Client{Timeout: o.Timeout},
		now:   time.Now,
		sleep: time.Sleep,
	}
}

// State returns the current breaker state (for /metrics and /health).
func (c *Client) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gate()
}

// gate returns the effective state, promoting open → half-open once OpenFor has
// elapsed. Caller must hold the lock.
func (c *Client) gate() State {
	if c.state == StateOpen && c.now().Sub(c.openedAt) >= c.opts.OpenFor {
		c.state = StateHalfOpen
	}
	return c.state
}

// allow reports whether a call may proceed given the breaker state.
func (c *Client) allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gate() != StateOpen
}

// onResult records a call outcome and transitions the breaker.
func (c *Client) onResult(ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok {
		c.failures = 0
		c.state = StateClosed
		return
	}
	c.failures++
	// A failure while half-open, or hitting the threshold while closed, opens it.
	if c.state == StateHalfOpen || c.failures >= c.opts.FailThreshold {
		c.state = StateOpen
		c.openedAt = c.now()
	}
}

// Do executes req with retry + exponential backoff, guarded by the circuit
// breaker. Returns ErrCircuitOpen immediately when the breaker is open. A 5xx or
// transport error counts as a failure (and is retried); a 2xx–4xx is a success
// (4xx is the caller's problem, not retried).
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if !c.allow() {
		return nil, ErrCircuitOpen
	}

	var resp *http.Response
	var err error
	attempts := c.opts.MaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			c.sleep(c.backoffFor(attempt - 1))
			if resp != nil {
				resp.Body.Close() // drop the previous failed response before retry
			}
		}
		resp, err = c.http.Do(req)
		if err == nil && resp.StatusCode < 500 {
			c.onResult(true)
			return resp, nil
		}
	}
	c.onResult(false)
	if err != nil {
		return nil, err
	}
	return resp, nil // last 5xx surfaced to caller (body still open)
}

func (c *Client) backoffFor(i int) time.Duration {
	if i >= len(c.opts.Backoff) {
		return c.opts.Backoff[len(c.opts.Backoff)-1]
	}
	return c.opts.Backoff[i]
}
