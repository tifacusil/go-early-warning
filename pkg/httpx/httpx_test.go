package httpx

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(o Options) (*Client, *time.Time) {
	c := New(o)
	now := time.Unix(0, 0)
	p := &now
	c.now = func() time.Time { return *p } // reads through pointer so tests can advance time
	c.sleep = func(time.Duration) {}       // no real waiting in tests
	return c, p
}

func TestRetriesOn5xxThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, _ := newTestClient(Options{MaxRetries: 3, Backoff: []time.Duration{1, 1, 1}})
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if calls != 3 {
		t.Fatalf("want 3 calls, got %d", calls)
	}
	if c.State() != StateClosed {
		t.Fatalf("want closed after success, got %s", c.State())
	}
}

func TestBreakerOpensAndRejects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	// No retries; each Do = one failure. Threshold 2 opens after 2 calls.
	c, _ := newTestClient(Options{MaxRetries: 0, FailThreshold: 2, OpenFor: 30 * time.Second})
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
		resp, _ := c.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	}
	if c.State() != StateOpen {
		t.Fatalf("want open after threshold, got %s", c.State())
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	if _, err := c.Do(req); err != ErrCircuitOpen {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
}

func TestHalfOpenRecoversToClosed(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, now := newTestClient(Options{MaxRetries: 0, FailThreshold: 1, OpenFor: 10 * time.Second})
	// Open the breaker.
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, _ := c.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if c.State() != StateOpen {
		t.Fatalf("want open, got %s", c.State())
	}
	// Advance past OpenFor -> half-open; server now healthy.
	*now = now.Add(11 * time.Second)
	fail.Store(false)
	req, _ = http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("half-open probe failed: %v", err)
	}
	resp.Body.Close()
	if c.State() != StateClosed {
		t.Fatalf("want closed after recovery, got %s", c.State())
	}
}
