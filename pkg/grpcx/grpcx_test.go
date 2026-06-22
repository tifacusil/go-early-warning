package grpcx

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestCaller(o Options) (*Caller, *time.Time) {
	c := New(o)
	now := time.Unix(0, 0)
	p := &now
	c.now = func() time.Time { return *p }
	c.sleep = func(time.Duration) {}
	return c, p
}

func unavailable() error { return status.Error(codes.Unavailable, "down") }

func TestRetriesOnUnavailableThenSucceeds(t *testing.T) {
	c, _ := newTestCaller(Options{MaxRetries: 3, Backoff: []time.Duration{1, 1, 1}})
	calls := 0
	err := c.Invoke(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return unavailable()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 3 {
		t.Fatalf("want 3 calls, got %d", calls)
	}
	if c.State() != StateClosed {
		t.Fatalf("want closed, got %s", c.State())
	}
}

func TestNonRetryableNotRetried(t *testing.T) {
	c, _ := newTestCaller(Options{MaxRetries: 3})
	calls := 0
	err := c.Invoke(context.Background(), func(context.Context) error {
		calls++
		return status.Error(codes.InvalidArgument, "bad")
	})
	if err == nil {
		t.Fatal("want error")
	}
	if calls != 1 {
		t.Fatalf("InvalidArgument must not retry; got %d calls", calls)
	}
}

func TestBreakerOpensAndRejects(t *testing.T) {
	c, _ := newTestCaller(Options{MaxRetries: 0, FailThreshold: 2, OpenFor: 30 * time.Second})
	for i := 0; i < 2; i++ {
		_ = c.Invoke(context.Background(), func(context.Context) error { return unavailable() })
	}
	if c.State() != StateOpen {
		t.Fatalf("want open, got %s", c.State())
	}
	if err := c.Invoke(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
}

func TestHalfOpenRecovers(t *testing.T) {
	c, now := newTestCaller(Options{MaxRetries: 0, FailThreshold: 1, OpenFor: 10 * time.Second})
	_ = c.Invoke(context.Background(), func(context.Context) error { return unavailable() })
	if c.State() != StateOpen {
		t.Fatalf("want open, got %s", c.State())
	}
	*now = now.Add(11 * time.Second)
	if err := c.Invoke(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("half-open probe failed: %v", err)
	}
	if c.State() != StateClosed {
		t.Fatalf("want closed, got %s", c.State())
	}
}
