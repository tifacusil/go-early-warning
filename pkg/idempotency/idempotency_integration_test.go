//go:build integration

// Integration test against the real shared Redis (infra must be up).
// Run: EW_REDIS_ADDR host:port go test -tags=integration ./pkg/idempotency/
package idempotency

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRedisStoreDoneAfterMarkOnly pins the consumer invariant against real
// Redis: Done stays false until MarkDone, so a failed+requeued message is not
// mistaken for a duplicate (the bug that dropped alerts on retry).
func TestRedisStoreDoneAfterMarkOnly(t *testing.T) {
	addr := os.Getenv("EW_REDIS_ADDR")
	if addr == "" {
		t.Skip("EW_REDIS_ADDR not set; skipping integration test")
	}
	c := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("EW_REDIS_PASSWORD")})
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	defer func() { _ = c.Close() }()

	id := "itest-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	s := NewRedisStore(c, "itest:idemp:", time.Minute)
	defer func() { _ = c.Del(ctx, "itest:idemp:"+id).Err() }()

	// First delivery: not done -> consumer processes.
	if done, err := s.Done(ctx, id); err != nil || done {
		t.Fatalf("want not-done first, got done=%v err=%v", done, err)
	}
	// Failed attempt left it unmarked: redelivery still not done.
	if done, _ := s.Done(ctx, id); done {
		t.Fatal("unmarked message must not look like a duplicate")
	}
	// Success -> mark -> redelivery skipped.
	if err := s.MarkDone(ctx, id); err != nil {
		t.Fatal(err)
	}
	if done, _ := s.Done(ctx, id); !done {
		t.Fatal("want done after MarkDone")
	}
}
