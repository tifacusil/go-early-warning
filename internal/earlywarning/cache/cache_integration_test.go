//go:build integration

// Integration test against the real shared Redis (infra must be up).
// Run: EW_REDIS_ADDR host:port go test -tags=integration ./internal/earlywarning/cache/
package cache

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/threshold"
)

func newTestCache(t *testing.T) (*Cache, string, func()) {
	t.Helper()
	addr := os.Getenv("EW_REDIS_ADDR")
	if addr == "" {
		t.Skip("EW_REDIS_ADDR not set; skipping integration test")
	}
	c := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("EW_REDIS_PASSWORD")})
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	sensorID := "itest-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	return &Cache{c: c}, sensorID, func() {
		_ = c.Del(ctx, lastKey(sensorID)).Err()
		_ = c.Close()
	}
}

func reading(ts string, cum float64) *threshold.Reading {
	at, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		panic(err)
	}
	return &threshold.Reading{Timestamp: at.UTC(), CumulativeDisplacementMM: cum}
}

// TestBaselineNeverMovesBackwards pins the cross-replica invariant. Sharding by
// sensor_id serialises a sensor within a replica but not between replicas, so
// the store is the only place that can refuse a rewind.
func TestBaselineNeverMovesBackwards(t *testing.T) {
	k, sensorID, cleanup := newTestCache(t)
	defer cleanup()
	ctx := context.Background()

	newest := reading("2019-06-26T02:00:00Z", 200)
	if written, err := k.SetLastReading(ctx, sensorID, newest); err != nil || !written {
		t.Fatalf("first write: written=%v err=%v", written, err)
	}

	for _, tc := range []struct {
		name string
		r    *threshold.Reading
	}{
		{"older", reading("2019-06-26T01:00:00Z", 100)},
		{"same timestamp", reading("2019-06-26T02:00:00Z", 900)},
		{"older by a microsecond", reading("2019-06-26T01:59:59.999999Z", 900)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			written, err := k.SetLastReading(ctx, sensorID, tc.r)
			if err != nil {
				t.Fatalf("write: %v", err)
			}
			if written {
				t.Fatal("write landed; the baseline moved backwards")
			}
			got, err := k.GetLastReading(ctx, sensorID)
			if err != nil {
				t.Fatalf("read back: %v", err)
			}
			if !got.Timestamp.Equal(newest.Timestamp) {
				t.Fatalf("baseline is %s, want %s", got.Timestamp, newest.Timestamp)
			}
			if got.CumulativeDisplacementMM != newest.CumulativeDisplacementMM {
				t.Fatalf("displacement is %v, want %v",
					got.CumulativeDisplacementMM, newest.CumulativeDisplacementMM)
			}
		})
	}
}

func TestNewerReadingIsStored(t *testing.T) {
	k, sensorID, cleanup := newTestCache(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := k.SetLastReading(ctx, sensorID, reading("2019-06-26T02:00:00Z", 200)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	newer := reading("2019-06-26T03:00:00Z", 205.5)
	written, err := k.SetLastReading(ctx, sensorID, newer)
	if err != nil || !written {
		t.Fatalf("second write: written=%v err=%v", written, err)
	}
	got, err := k.GetLastReading(ctx, sensorID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !got.Timestamp.Equal(newer.Timestamp) || got.CumulativeDisplacementMM != 205.5 {
		t.Fatalf("got %+v, want %+v", got, newer)
	}
}

// TestSubSecondBaselineSurvivesRoundTrip covers the RFC3339 -> RFC3339Nano fix:
// truncating the stored timestamp lengthens the next reading's dt and
// understates its deformation speed.
func TestSubSecondBaselineSurvivesRoundTrip(t *testing.T) {
	k, sensorID, cleanup := newTestCache(t)
	defer cleanup()
	ctx := context.Background()

	want := reading("2019-06-26T02:00:00.25Z", 200)
	if _, err := k.SetLastReading(ctx, sensorID, want); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := k.GetLastReading(ctx, sensorID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("baseline is %s, want %s", got.Timestamp.Format(time.RFC3339Nano), want.Timestamp)
	}
}

// TestBaselineWithoutScoreIsOverwritten keeps entries written before the CAS
// existed (and entries written by the Python original) usable: with no
// timestamp_unix_us to compare, the write lands and the entry self-heals.
func TestBaselineWithoutScoreIsOverwritten(t *testing.T) {
	k, sensorID, cleanup := newTestCache(t)
	defer cleanup()
	ctx := context.Background()

	legacy := `{"timestamp":"2019-06-26T05:00:00Z","cumulative_displacement_mm":500}`
	if err := k.c.Set(ctx, lastKey(sensorID), legacy, 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Older than the legacy entry, and still stored: without a score there is
	// nothing to compare against, so the previous unconditional-SET behaviour
	// applies.
	written, err := k.SetLastReading(ctx, sensorID, reading("2019-06-26T04:00:00Z", 400))
	if err != nil || !written {
		t.Fatalf("write over legacy entry: written=%v err=%v", written, err)
	}
}

// TestConcurrentWritesLeaveTheNewest is the race the CAS exists for: many
// writers, arbitrary order, one winner.
func TestConcurrentWritesLeaveTheNewest(t *testing.T) {
	k, sensorID, cleanup := newTestCache(t)
	defer cleanup()
	ctx := context.Background()

	base := time.Date(2019, 6, 26, 0, 0, 0, 0, time.UTC)
	const n = 50
	done := make(chan struct{})
	for i := range n {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			r := &threshold.Reading{
				Timestamp:                base.Add(time.Duration(i) * time.Minute),
				CumulativeDisplacementMM: float64(i),
			}
			if _, err := k.SetLastReading(ctx, sensorID, r); err != nil {
				t.Errorf("write %d: %v", i, err)
			}
		}(i)
	}
	for range n {
		<-done
	}

	got, err := k.GetLastReading(ctx, sensorID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	want := base.Add((n - 1) * time.Minute)
	if !got.Timestamp.Equal(want) {
		t.Fatalf("baseline is %s, want the newest write %s", got.Timestamp, want)
	}
}
