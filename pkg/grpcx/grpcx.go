// Package grpcx wraps outbound gRPC calls with an explicit per-call timeout,
// retry + exponential backoff, and a circuit breaker, mirroring pkg/httpx for
// synchronous gRPC (CLAUDE.md §1.5 / §4). Use Invoke to guard a unary call.
package grpcx

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrCircuitOpen is returned when the breaker is open and rejects a call.
var ErrCircuitOpen = errors.New("grpcx: circuit open")

// State is the breaker state (shares the 0/1/2 encoding with pkg/httpx so the
// same Prometheus gauge convention applies).
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

// Options configures a resilient gRPC caller.
type Options struct {
	Timeout       time.Duration   // explicit per-call timeout (required)
	MaxRetries    int             // additional attempts after the first
	Backoff       []time.Duration
	FailThreshold int             // consecutive failures before the breaker opens
	OpenFor       time.Duration   // cool-down before half-open probe
}

// DefaultBackoff matches the CLAUDE.md §4 schedule: 1s / 5s / 30s.
var DefaultBackoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

// Caller guards unary gRPC calls with retry + breaker.
type Caller struct {
	opts Options

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time

	now   func() time.Time
	sleep func(time.Duration)
}

// New builds a Caller; unset fields get sane defaults.
func New(o Options) *Caller {
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
	return &Caller{opts: o, now: time.Now, sleep: time.Sleep}
}

// State returns the current breaker state (for /metrics).
func (c *Caller) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gate()
}

func (c *Caller) gate() State {
	if c.state == StateOpen && c.now().Sub(c.openedAt) >= c.opts.OpenFor {
		c.state = StateHalfOpen
	}
	return c.state
}

func (c *Caller) allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gate() != StateOpen
}

func (c *Caller) onResult(ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok {
		c.failures = 0
		c.state = StateClosed
		return
	}
	c.failures++
	if c.state == StateHalfOpen || c.failures >= c.opts.FailThreshold {
		c.state = StateOpen
		c.openedAt = c.now()
	}
}

// Invoke runs fn (one unary call) with a fresh timeout context, retrying on
// retryable gRPC codes with backoff, guarded by the breaker. Returns
// ErrCircuitOpen immediately when the breaker is open.
func (c *Caller) Invoke(ctx context.Context, fn func(context.Context) error) error {
	if !c.allow() {
		return ErrCircuitOpen
	}
	var err error
	attempts := c.opts.MaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			c.sleep(c.backoffFor(attempt - 1))
		}
		callCtx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
		err = fn(callCtx)
		cancel()
		if err == nil {
			c.onResult(true)
			return nil
		}
		if !retryable(err) {
			c.onResult(false)
			return err
		}
	}
	c.onResult(false)
	return err
}

func (c *Caller) backoffFor(i int) time.Duration {
	if i >= len(c.opts.Backoff) {
		return c.opts.Backoff[len(c.opts.Backoff)-1]
	}
	return c.opts.Backoff[i]
}

// retryable reports whether a gRPC error is worth retrying. Transient codes
// (Unavailable, DeadlineExceeded, ResourceExhausted) retry; everything else
// (InvalidArgument, NotFound, ...) is the caller's problem.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}
