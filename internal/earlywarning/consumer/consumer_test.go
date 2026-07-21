package consumer

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The whole concurrency design rests on one invariant: two messages for the
// same sensor never land on different workers. process() does a
// read-modify-write on that sensor's last reading in Redis (get previous ->
// derive deformation speed -> store current), so if the invariant breaks the
// service computes speeds against stale readings and emits wrong alerts.
func TestShardForIsStablePerSensor(t *testing.T) {
	const workers = 8
	for _, sensor := range []string{"sensor-1", "sensor-42", "GNSS-A-007", "x"} {
		want := shardFor(wireMessage{SensorID: sensor, MessageID: "m0"}, workers)
		for i := 1; i < 100; i++ {
			// Same sensor, different message ids: the shard must not move.
			got := shardFor(wireMessage{
				SensorID:  sensor,
				MessageID: fmt.Sprintf("m%d", i),
			}, workers)
			if got != want {
				t.Fatalf("sensor %q moved shard: msg m%d got %d, want %d", sensor, i, got, want)
			}
		}
	}
}

func TestShardForStaysInRange(t *testing.T) {
	for _, workers := range []int{1, 2, 3, 8, 64} {
		for i := 0; i < 500; i++ {
			got := shardFor(wireMessage{SensorID: fmt.Sprintf("sensor-%d", i)}, workers)
			if got < 0 || got >= workers {
				t.Fatalf("workers=%d sensor-%d: shard %d out of range", workers, i, got)
			}
		}
	}
}

// Sharding must actually spread load, otherwise the pool is N workers deep and
// one worker wide — the serial bug this replaced.
func TestShardForSpreadsAcrossWorkers(t *testing.T) {
	const workers = 8
	seen := map[int]int{}
	for i := 0; i < 1000; i++ {
		seen[shardFor(wireMessage{SensorID: fmt.Sprintf("sensor-%d", i)}, workers)]++
	}
	if len(seen) != workers {
		t.Fatalf("only %d/%d workers used: %v", len(seen), workers, seen)
	}
	for shard, n := range seen {
		if n < 1000/workers/2 {
			t.Errorf("shard %d starved: %d of 1000", shard, n)
		}
	}
}

// With workers=1 every message serializes onto one shard: strict global
// ordering, the pre-existing behaviour, still available as an escape hatch.
func TestShardForSingleWorkerSerializes(t *testing.T) {
	for i := 0; i < 50; i++ {
		if got := shardFor(wireMessage{SensorID: fmt.Sprintf("s%d", i)}, 1); got != 0 {
			t.Fatalf("workers=1 must always shard to 0, got %d", got)
		}
	}
}

// Messages with no sensor_id carry no per-sensor state, but must still be
// spread rather than all piling onto shard 0.
func TestShardForFallsBackToMessageID(t *testing.T) {
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		seen[shardFor(wireMessage{MessageID: fmt.Sprintf("msg-%d", i)}, 8)] = true
	}
	if len(seen) < 2 {
		t.Fatalf("sensor-less messages did not spread: %v", seen)
	}
	if got := shardFor(wireMessage{}, 8); got != 0 {
		t.Errorf("empty message should shard to 0, got %d", got)
	}
}

// ackRecorder is a stub amqp.Acknowledger; the real one needs a live channel.
type ackRecorder struct {
	mu     sync.Mutex
	acks   int
	nacks  int
	reject int
}

func (a *ackRecorder) Ack(uint64, bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks++
	return nil
}

func (a *ackRecorder) Nack(_ uint64, _ bool, _ bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nacks++
	return nil
}

func (a *ackRecorder) Reject(uint64, bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reject++
	return nil
}

// decode runs on the dispatch path, so a malformed body must be acked and
// dropped there — never handed to a worker.
func TestDecodeDropsMalformedJSON(t *testing.T) {
	ack := &ackRecorder{}
	c := New(Deps{Log: discardLogger()})

	_, ok := c.decode(amqp.Delivery{Acknowledger: ack, Body: []byte("{not json")})
	if ok {
		t.Fatal("malformed JSON should not be dispatched to a worker")
	}
	if ack.acks != 1 {
		t.Errorf("malformed message should be acked once to drop it, got %d acks", ack.acks)
	}
	if ack.nacks != 0 {
		t.Errorf("malformed message is non-retryable, want 0 nacks, got %d", ack.nacks)
	}
}

func TestDecodeParsesWireMessage(t *testing.T) {
	ack := &ackRecorder{}
	c := New(Deps{Log: discardLogger()})

	body := []byte(`{"message_id":"m1","project_id":"p1","sensor_id":"s1","sensor_type":"gnss"}`)
	wm, ok := c.decode(amqp.Delivery{Acknowledger: ack, Body: body})
	if !ok {
		t.Fatal("valid message should decode")
	}
	if wm.MessageID != "m1" || wm.SensorID != "s1" || wm.ProjectID != "p1" {
		t.Fatalf("decoded wrong: %+v", wm)
	}
	if ack.acks != 0 {
		t.Errorf("decode must not ack a valid message, got %d acks", ack.acks)
	}
}

func TestWorkerAndPrefetchDefaults(t *testing.T) {
	cases := []struct {
		name             string
		deps             Deps
		wantW, wantPre   int
	}{
		{"zero value defaults to 8 workers", Deps{}, 8, 64},
		{"explicit workers scale prefetch", Deps{Workers: 16}, 16, 128},
		{"prefetch floor is 20", Deps{Workers: 1}, 1, 20},
		{"explicit prefetch wins", Deps{Workers: 4, Prefetch: 7}, 4, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := New(tc.deps)
			if got := c.workers(); got != tc.wantW {
				t.Errorf("workers() = %d, want %d", got, tc.wantW)
			}
			if got := c.prefetch(); got != tc.wantPre {
				t.Errorf("prefetch() = %d, want %d", got, tc.wantPre)
			}
		})
	}
}

// Prefetch must cover the pool, or workers sit idle waiting on the broker.
func TestPrefetchNeverStarvesWorkers(t *testing.T) {
	for _, w := range []int{1, 2, 8, 16, 64} {
		c := New(Deps{Workers: w})
		if c.prefetch() < c.workers() {
			t.Errorf("workers=%d: prefetch %d < workers, pool would starve", w, c.prefetch())
		}
	}
}
