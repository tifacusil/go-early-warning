package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/store"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/threshold"
	"github.com/AngelloFD/go-early-warning/pkg/grpcx"
	"github.com/AngelloFD/go-early-warning/pkg/metrics"
)

// These tests drive handleParsed — the per-message pipeline a worker runs —
// with fakes in place of Redis, MySQL, RabbitMQ and user-management. They pin
// the three behaviours the chaos/AP procedures verify on the deployed system
// (tesis-infra docs/ap5-ap6-ap7-procedimientos.md), so a regression shows up
// in CI instead of in the middle of a campaign:
//
//   - O6 / CLAUDE.md §2.3: with user-management failing, the alert is still
//     recorded and fanned out; only the push is lost.
//   - AP6: a message_id already processed is acked without side effects.
//   - AP7: a persistently failing message is requeued a bounded number of
//     times and then dead-lettered.

const (
	testProject = "shangtan"
	testSensor  = "chaos-um-1"
	testService = "early-warning-test"
)

var baseTime = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

// alertingMessage is the second reading of the pair chaos/publish-event.sh
// sends: +100mm 60s after a 0mm baseline => ~144000 mm/day, well over red (20).
func alertingMessage(messageID string) wireMessage {
	return wireMessage{
		MessageID:  messageID,
		ProjectID:  testProject,
		SensorID:   testSensor,
		SensorType: "gnss",
		Timestamp:  baseTime.Add(60 * time.Second).Format(time.RFC3339),
		Payload: map[string]any{
			"fix_quality":                "rtk_fixed",
			"cumulative_displacement_mm": 100.0,
		},
	}
}

type pipeline struct {
	c     *Consumer
	cache *fakeCache
	store *fakeStore
	pub   *fakePublisher
	subs  *fakeSubs
	idemp *fakeIdemp
	mx    *metrics.Metrics
	logs  *bytes.Buffer
}

func newPipeline(t *testing.T) *pipeline {
	t.Helper()
	p := &pipeline{
		cache: &fakeCache{
			last:    map[string]*threshold.Reading{},
			retries: map[string]int64{},
		},
		store: &fakeStore{},
		pub:   &fakePublisher{delivered: true},
		subs:  &fakeSubs{},
		idemp: &fakeIdemp{done: map[string]bool{}},
		mx:    metrics.New(testService),
		logs:  &bytes.Buffer{},
	}
	// The baseline the first reading of the pair would have written.
	p.cache.last[testSensor] = &threshold.Reading{Timestamp: baseTime, CumulativeDisplacementMM: 0}

	p.c = New(Deps{
		Engine:    threshold.DefaultConfig(7200),
		Cache:     p.cache,
		Store:     p.store,
		Publisher: p.pub,
		Subs:      p.subs,
		Levels:    fakeLevels{},
		Idemp:     p.idemp,
		Metrics:   p.mx,
		Log:       slog.New(slog.NewJSONHandler(p.logs, nil)),
	})
	return p
}

// deliver runs one delivery through the worker pipeline and returns what the
// consumer did with it (ack, or nack with/without requeue).
func (p *pipeline) deliver(wm wireMessage) *ackRecorder {
	ack := &ackRecorder{}
	p.c.handleParsed(context.Background(), amqp.Delivery{Acknowledger: ack}, wm)
	return ack
}

// events returns the structured log lines whose event ("msg") is name.
func (p *pipeline) events(t *testing.T, name string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(p.logs.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if rec["msg"] == name {
			out = append(out, rec)
		}
	}
	return out
}

// dispatchCount reads ew_alert_dispatch_duration_seconds_count for a
// push_path from the /metrics output, the same series Prometheus scrapes.
func (p *pipeline) dispatchCount(t *testing.T, pushPath string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	p.mx.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := fmt.Sprintf(`ew_alert_dispatch_duration_seconds_count{push_path=%q,service=%q} `, pushPath, testService)
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				t.Fatalf("unparseable count %q", v)
			}
			return n
		}
	}
	return 0
}

// --- O6: degraded mode on the alert path ----------------------------------

// user-management is the only synchronous dependency of the alert path. When
// it fails — breaker open, timeout, or peer down — the alert must still be
// recorded and published; only the push notification is lost. This is the
// continuity CLAUDE.md §2.3 requires and what netem-delay-ew-um.sh measures.
func TestAlertIsRecordedWhenUserManagementFails(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"breaker abierto", grpcx.ErrCircuitOpen},
		{"timeout", status.Error(codes.DeadlineExceeded, "context deadline exceeded")},
		{"peer caido", status.Error(codes.Unavailable, "connection refused")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPipeline(t)
			p.subs.err = tc.err

			ack := p.deliver(alertingMessage("m-1"))

			if ack.acks != 1 || ack.nacks != 0 {
				t.Fatalf("degraded alert is a success: want 1 ack / 0 nacks, got %d / %d", ack.acks, ack.nacks)
			}
			if got := len(p.store.rows); got != 1 {
				t.Fatalf("alert must be recorded despite user-management failing, got %d rows", got)
			}
			if p.pub.rabbit != 1 {
				t.Errorf("alert must still be published to RabbitMQ, got %d publishes", p.pub.rabbit)
			}
			if p.pub.push != 0 {
				t.Errorf("no push can be attempted without tokens, got %d", p.pub.push)
			}
			if got := testutil.ToFloat64(p.mx.AlertsPublished.WithLabelValues("red")); got != 1 {
				t.Errorf("ew_alerts_published_total{level=red}: want 1, got %v", got)
			}
			if got := p.dispatchCount(t, metrics.PushPathResolveFailed); got != 1 {
				t.Errorf("dispatch must be counted as resolve_failed, got %d", got)
			}
			// Marked done: a redelivery of a degraded alert must not re-alert.
			if !p.idemp.isDone("m-1") {
				t.Error("degraded alert must be marked done for idempotency")
			}
			emitted := p.events(t, "alert_emitted")
			if len(emitted) != 1 || emitted[0]["push_path"] != metrics.PushPathResolveFailed {
				t.Errorf("want one alert_emitted with push_path=resolve_failed, got %v", emitted)
			}
		})
	}
}

// push_path separates the degraded span from healthy dispatches; without it
// the O6 degradation latency would be averaged into normal traffic.
func TestDispatchLabelsPushPath(t *testing.T) {
	cases := []struct {
		name      string
		tokens    []string
		delivered bool
		want      string
	}{
		{"sin suscriptores elegibles", nil, true, metrics.PushPathNoTokens},
		{"push entregado", []string{"tok-1"}, true, metrics.PushPathDelivered},
		{"FCM rechaza", []string{"tok-1"}, false, metrics.PushPathPushFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPipeline(t)
			p.subs.tokens = tc.tokens
			p.pub.delivered = tc.delivered

			if ack := p.deliver(alertingMessage("m-1")); ack.acks != 1 {
				t.Fatalf("want ack, got %d acks / %d nacks", ack.acks, ack.nacks)
			}
			if got := p.dispatchCount(t, tc.want); got != 1 {
				t.Errorf("push_path=%s: want 1, got %d", tc.want, got)
			}
			if got := p.dispatchCount(t, metrics.PushPathResolveFailed); got != 0 {
				t.Errorf("healthy dispatch must not count as resolve_failed, got %d", got)
			}
		})
	}
}

// --- AP6: idempotency -------------------------------------------------------

func TestDuplicateMessageIsAckedWithoutSideEffects(t *testing.T) {
	p := newPipeline(t)
	p.idemp.done["m-1"] = true

	ack := p.deliver(alertingMessage("m-1"))

	if ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("duplicate must be acked to drop it: got %d acks / %d nacks", ack.acks, ack.nacks)
	}
	if len(p.store.rows) != 0 || p.pub.rabbit != 0 || p.subs.calls != 0 {
		t.Errorf("duplicate must have no side effects: rows=%d rabbit=%d subs=%d",
			len(p.store.rows), p.pub.rabbit, p.subs.calls)
	}
	if got := testutil.ToFloat64(p.mx.Duplicates); got != 1 {
		t.Errorf("ew_duplicates_skipped_total: want 1, got %v", got)
	}
	if b := p.cache.last[testSensor]; b.CumulativeDisplacementMM != 0 || !b.Timestamp.Equal(baseTime) {
		t.Errorf("duplicate must not move the sensor baseline, got %+v", b)
	}
	// The AP6 evidence: the duplicate is named in the log, traceable by id.
	dup := p.events(t, "duplicate_skipped")
	if len(dup) != 1 || dup[0]["trace_id"] != "m-1" {
		t.Errorf("want one duplicate_skipped with trace_id=m-1, got %v", dup)
	}
}

// The AP6 procedure end to end: inject, let it finish, reinject the same
// message_id. The second delivery must be recognised as a duplicate — not
// merely evaluated to "no alert" because the baseline moved.
func TestReinjectedMessageIsSkippedAfterFirstSuccess(t *testing.T) {
	p := newPipeline(t)

	first := p.deliver(alertingMessage("m-1"))
	second := p.deliver(alertingMessage("m-1"))

	if first.acks != 1 || second.acks != 1 {
		t.Fatalf("both deliveries must be acked, got %d and %d", first.acks, second.acks)
	}
	if got := len(p.store.rows); got != 1 {
		t.Errorf("alert rows: want 1, got %d", got)
	}
	if got := testutil.ToFloat64(p.mx.AlertsPublished.WithLabelValues("red")); got != 1 {
		t.Errorf("ew_alerts_published_total: want 1, got %v", got)
	}
	if got := testutil.ToFloat64(p.mx.Duplicates); got != 1 {
		t.Errorf("ew_duplicates_skipped_total: want 1, got %v", got)
	}
	// One evaluation only: the duplicate is dropped before the engine runs.
	if got := testutil.CollectAndCount(p.mx.Evaluations); got != 1 {
		t.Errorf("want evaluations for 1 outcome (the first delivery), got %d", got)
	}
}

// --- AP7: bounded retry -> dead letter ------------------------------------

// A persistence failure is the only retryable error (AP7.1). With MySQL down
// every redelivery fails the same way: 3 requeues, then a nack without
// requeue that the queue's x-dead-letter-exchange routes to
// sensors.dead_letter. Never acked, never marked done.
func TestPersistFailureRetriesThenDeadLetters(t *testing.T) {
	p := newPipeline(t)
	p.store.setErr(errors.New("dial tcp mysql-early-warning:3306: connect: connection refused"))

	var requeue []bool
	for i := 0; i < 4; i++ { // original delivery + 3 redeliveries
		ack := p.deliver(alertingMessage("m-1"))
		if ack.acks != 0 {
			t.Fatalf("delivery %d: a message that was not persisted must never be acked", i+1)
		}
		requeue = append(requeue, ack.requeue...)
	}

	if want := []bool{true, true, true, false}; !reflect.DeepEqual(requeue, want) {
		t.Fatalf("requeue sequence: want %v, got %v", want, requeue)
	}
	if got := testutil.ToFloat64(p.mx.MessagesDropped); got != 1 {
		t.Errorf("ew_messages_dropped_total: want 1 (the dead-lettered one), got %v", got)
	}
	if p.idemp.isDone("m-1") {
		t.Error("a failed message must not be marked done")
	}
	if got := testutil.ToFloat64(p.mx.AlertsPublished.WithLabelValues("red")); got != 0 {
		t.Errorf("no alert was recorded, ew_alerts_published_total must stay 0, got %v", got)
	}

	// The log lines the AP7 procedure counts, with the message_id as trace_id.
	requeues := p.events(t, "requeue")
	if len(requeues) != 3 {
		t.Fatalf("want 3 requeue log lines, got %d", len(requeues))
	}
	for i, ev := range requeues {
		if ev["attempt"] != float64(i+1) || ev["trace_id"] != "m-1" {
			t.Errorf("requeue %d: want attempt=%d trace_id=m-1, got %v", i, i+1, ev)
		}
	}
	dl := p.events(t, "dead_lettered")
	if len(dl) != 1 || dl[0]["attempts"] != float64(4) || dl[0]["trace_id"] != "m-1" {
		t.Errorf("want one dead_lettered with attempts=4 trace_id=m-1, got %v", dl)
	}
}

// MarkDone runs only after success, so a message that failed once is still
// processed when it comes back — not dropped as a duplicate (AP6.2).
func TestRedeliveryAfterTransientFailureIsProcessed(t *testing.T) {
	p := newPipeline(t)
	p.store.setErr(errors.New("mysql briefly unavailable"))

	if ack := p.deliver(alertingMessage("m-1")); !reflect.DeepEqual(ack.requeue, []bool{true}) {
		t.Fatalf("first failure must requeue, got %v", ack.requeue)
	}

	p.store.setErr(nil)
	ack := p.deliver(alertingMessage("m-1"))

	if ack.acks != 1 {
		t.Fatalf("redelivery after recovery must be processed and acked, got %d acks", ack.acks)
	}
	if got := len(p.store.rows); got != 1 {
		t.Errorf("alert rows: want 1, got %d", got)
	}
	if got := testutil.ToFloat64(p.mx.Duplicates); got != 0 {
		t.Errorf("a retried message is not a duplicate, ew_duplicates_skipped_total=%v", got)
	}
}

// --- fakes -----------------------------------------------------------------

type fakeCache struct {
	mu      sync.Mutex
	last    map[string]*threshold.Reading
	retries map[string]int64
}

func (f *fakeCache) GetLastReading(_ context.Context, sensorID string) (*threshold.Reading, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.last[sensorID]
	if !ok {
		return nil, nil
	}
	cp := *r
	return &cp, nil
}

func (f *fakeCache) SetLastReading(_ context.Context, sensorID string, r *threshold.Reading) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *r
	f.last[sensorID] = &cp
	return true, nil
}

func (f *fakeCache) SetLastMessageProcessedAt(context.Context, string) error { return nil }

func (f *fakeCache) IncrRetry(_ context.Context, messageID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries[messageID]++
	return f.retries[messageID], nil
}

func (f *fakeCache) IncrAlertsGenerated(context.Context) (int64, error) { return 1, nil }

func (f *fakeCache) PushRecentAlert(context.Context, *threshold.Alert) error { return nil }

type fakeStore struct {
	mu   sync.Mutex
	err  error
	rows []store.AlertRow
}

func (f *fakeStore) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeStore) RecordAlert(_ context.Context, a store.AlertRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.rows = append(f.rows, a)
	return nil
}

type fakePublisher struct {
	mu        sync.Mutex
	rabbit    int
	http      int
	push      int
	delivered bool
}

func (f *fakePublisher) PublishRabbitMQ(context.Context, *threshold.Alert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rabbit++
	return nil
}

func (f *fakePublisher) PublishHTTP(context.Context, *threshold.Alert) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.http++
}

func (f *fakePublisher) PublishPush(context.Context, *threshold.Alert, []string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.push++
	return f.delivered
}

type fakeSubs struct {
	mu     sync.Mutex
	tokens []string
	err    error
	calls  int
}

func (f *fakeSubs) TokensFor(context.Context, string, string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.tokens, f.err
}

type fakeLevels struct{}

func (fakeLevels) Levels(projectID string) *threshold.Levels {
	if projectID != testProject {
		return nil
	}
	return &threshold.Levels{Red: 20, Orange: 15, Blue: 10}
}

type fakeIdemp struct {
	mu   sync.Mutex
	done map[string]bool
}

func (f *fakeIdemp) Done(_ context.Context, messageID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done[messageID], nil
}

func (f *fakeIdemp) MarkDone(_ context.Context, messageID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.done[messageID] = true
	return nil
}

func (f *fakeIdemp) isDone(messageID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done[messageID]
}
