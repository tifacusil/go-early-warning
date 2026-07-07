// Package consumer ports consumer.py: it consumes sensor readings from the
// early_warning.evaluate queue, runs the threshold engine, and on an alert
// fans out via the publisher and persists. Ack only after successful handling
// (CLAUDE.md §2.2). Idempotency + DLQ are layered in Hito 3.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/cache"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/publisher"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/store"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/threshold"
	"github.com/AngelloFD/go-early-warning/pkg/idempotency"
	"github.com/AngelloFD/go-early-warning/pkg/logging"
	"github.com/AngelloFD/go-early-warning/pkg/metrics"
)

// SubscriptionResolver resolves FCM tokens for a project/level (user-management).
type SubscriptionResolver interface {
	TokensFor(ctx context.Context, projectID, alertLevel string) ([]string, error)
}

// ThresholdProvider returns the per-project levels, or nil for unknown projects.
type ThresholdProvider interface {
	Levels(projectID string) *threshold.Levels
}

// Deps bundles the consumer's collaborators.
type Deps struct {
	Channel    *amqp.Channel
	Queue      string
	Engine     threshold.Config
	Cache      *cache.Cache
	Store      *store.Store
	Publisher  *publisher.Publisher
	Subs       SubscriptionResolver
	Levels     ThresholdProvider
	Idemp      idempotency.Store // dedup by message_id (CLAUDE.md §1.3)
	MaxRetries int               // requeue attempts before dead-lettering (default 3)
	Metrics    *metrics.Metrics  // optional; nil disables instrumentation
	Log        *slog.Logger
}

// Consumer drives one AMQP consume loop.
type Consumer struct {
	d Deps
}

// New builds a Consumer.
func New(d Deps) *Consumer { return &Consumer{d: d} }

// wireMessage mirrors contracts/schemas/sensor_reading.schema.json.
type wireMessage struct {
	MessageID  string         `json:"message_id"`
	ProjectID  string         `json:"project_id"`
	SensorID   string         `json:"sensor_id"`
	SensorType string         `json:"sensor_type"`
	Timestamp  string         `json:"timestamp"`
	Payload    map[string]any `json:"payload"`
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.d.Channel.Qos(20, 0, false); err != nil {
		return err
	}
	deliveries, err := c.d.Channel.ConsumeWithContext(ctx, c.d.Queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	c.d.Log.Info("consumer_started", slog.String("queue", c.d.Queue))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-deliveries:
			if !ok {
				return nil
			}
			c.handle(ctx, msg)
		}
	}
}

func (c *Consumer) maxRetries() int {
	if c.d.MaxRetries <= 0 {
		return 3
	}
	return c.d.MaxRetries
}

func (c *Consumer) m() *metrics.Metrics { return c.d.Metrics }

// handle decides ack / requeue / dead-letter based on process() outcome.
func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	_ = c.d.Cache.SetLastMessageProcessedAt(ctx, time.Now().UTC().Format(time.RFC3339))

	if m := c.m(); m != nil {
		m.MessagesConsumed.Inc()
	}

	var wm wireMessage
	if err := json.Unmarshal(d.Body, &wm); err != nil {
		// Malformed JSON is non-retryable: ack to drop (matches Python).
		c.d.Log.Warn("json_decode_error", slog.Int("body_len", len(d.Body)))
		if m := c.m(); m != nil {
			m.MessagesDropped.Inc()
		}
		_ = d.Ack(false)
		return
	}

	ctx = logging.WithTraceID(ctx, wm.MessageID)
	log := logging.FromContext(ctx, c.d.Log)

	// Idempotent consumer: skip messages already processed (CLAUDE.md §1.3, §2.2).
	// Mark happens AFTER success, so a failed+requeued message is not skipped on
	// redelivery and the retry/DLQ path stays reachable.
	if c.d.Idemp != nil && wm.MessageID != "" {
		done, err := c.d.Idemp.Done(ctx, wm.MessageID)
		if err != nil {
			log.Warn("idemp_check_failed", slog.String("err", err.Error()))
		} else if done {
			log.Info("duplicate_skipped")
			if m := c.m(); m != nil {
				m.Duplicates.Inc()
			}
			_ = d.Ack(false)
			return
		}
	}

	if err := c.process(ctx, log, wm); err != nil {
		c.retryOrDeadLetter(ctx, log, d, wm.MessageID, err)
		return
	}
	if c.d.Idemp != nil && wm.MessageID != "" {
		if err := c.d.Idemp.MarkDone(ctx, wm.MessageID); err != nil {
			log.Warn("idemp_mark_failed", slog.String("err", err.Error()))
		}
	}
	_ = d.Ack(false)
}

// process runs evaluation + side effects. Returns an error only for retryable
// failures (persistence). Skips/alerts that succeed return nil.
func (c *Consumer) process(ctx context.Context, log *slog.Logger, wm wireMessage) error {
	m := parseMessage(wm)
	var prev *threshold.Reading
	if m.SensorID != "" {
		if p, err := c.d.Cache.GetLastReading(ctx, m.SensorID); err == nil {
			prev = p
		}
	}

	start := time.Now()
	res := c.d.Engine.Evaluate(m, prev, c.d.Levels.Levels(m.ProjectID))
	if c.d.Metrics != nil {
		c.d.Metrics.EvalDuration.Observe(time.Since(start).Seconds())
	}

	if res.Alert != nil {
		if err := c.dispatch(ctx, log, res.Alert); err != nil {
			return err // persistence failed -> retryable
		}
	}

	if res.ShouldStoreCurrent && res.StorePayload != nil && m.SensorID != "" {
		if err := c.d.Cache.SetLastReading(ctx, m.SensorID, res.StorePayload); err != nil {
			log.Warn("store_last_reading_failed", slog.String("err", err.Error()))
		}
	}
	return nil
}

// retryOrDeadLetter requeues up to maxRetries, then nacks without requeue so the
// queue's x-dead-letter-exchange (sensors.dlx, pre-declared in infra) routes the
// message to sensors.dead_letter (CLAUDE.md §4).
func (c *Consumer) retryOrDeadLetter(ctx context.Context, log *slog.Logger, d amqp.Delivery, messageID string, cause error) {
	attempts := 1
	if c.d.Idemp != nil && messageID != "" {
		attempts = c.bumpRetry(ctx, messageID)
	}
	if attempts > c.maxRetries() {
		log.Warn("dead_lettered", slog.Int("attempts", attempts), slog.String("err", cause.Error()))
		if c.d.Metrics != nil {
			c.d.Metrics.MessagesDropped.Inc()
		}
		_ = d.Nack(false, false) // -> DLX
		return
	}
	log.Warn("requeue", slog.Int("attempt", attempts), slog.String("err", cause.Error()))
	_ = d.Nack(false, true)
}

// dispatch fans the alert out. Persistence failure is returned as retryable;
// best-effort side channels (publish/push/redis counters) only log.
func (c *Consumer) dispatch(ctx context.Context, log *slog.Logger, a *threshold.Alert) error {
	if err := c.d.Publisher.PublishRabbitMQ(ctx, a); err != nil {
		log.Warn("rabbit_alert_publish_failed", slog.String("err", err.Error()))
	}
	c.d.Publisher.PublishHTTP(ctx, a)

	tokens, err := c.d.Subs.TokensFor(ctx, a.ProjectID, a.AlertLevel)
	switch {
	case err != nil:
		log.Warn("push_flow_failed", slog.String("err", err.Error()))
	case len(tokens) == 0:
		// Not an error: no subscriber is push_enabled/eligible for this level.
		// Logged so "no push happened" is never confused with "push succeeded".
		log.Info("push_skipped_no_tokens", slog.String("level", a.AlertLevel))
	default:
		if delivered := c.d.Publisher.PublishPush(ctx, a, tokens); delivered {
			log.Info("push_delivered", slog.Int("tokens", len(tokens)))
			if m := c.m(); m != nil && !a.EvaluatedAt.IsZero() {
				m.AlertDeliveryLatency.Observe(time.Since(a.EvaluatedAt).Seconds())
			}
		}
	}

	if _, err := c.d.Cache.IncrAlertsGenerated(ctx); err != nil {
		log.Warn("incr_alerts_failed", slog.String("err", err.Error()))
	}
	if err := c.d.Cache.PushRecentAlert(ctx, a); err != nil {
		log.Warn("push_recent_failed", slog.String("err", err.Error()))
	}
	// DB record is the durable record; INSERT IGNORE keeps a retry idempotent.
	if err := c.recordAlert(ctx, a); err != nil {
		log.Warn("db_record_failed", slog.String("err", err.Error()))
		return err
	}
	if c.d.Metrics != nil {
		c.d.Metrics.AlertsPublished.WithLabelValues(a.AlertLevel).Inc()
	}
	log.Info("alert_emitted",
		slog.String("sensor_id", a.SensorID),
		slog.String("project_id", a.ProjectID),
		slog.String("level", a.AlertLevel),
		slog.Float64("speed_mm_day", a.DeformationSpeedMMDay),
	)
	return nil
}

// bumpRetry increments and returns the retry count for a message_id, kept in
// Redis with a short TTL so the counter self-cleans.
func (c *Consumer) bumpRetry(ctx context.Context, messageID string) int {
	n, err := c.d.Cache.IncrRetry(ctx, messageID)
	if err != nil {
		return 1
	}
	return int(n)
}

func (c *Consumer) recordAlert(ctx context.Context, a *threshold.Alert) error {
	at, _ := time.Parse(time.RFC3339, a.Timestamp)
	speed := a.DeformationSpeedMMDay
	thr := a.ThresholdMMDay
	return c.d.Store.RecordAlert(ctx, store.AlertRow{
		MessageID:             a.MessageID,
		ProjectID:             a.ProjectID,
		SensorID:              a.SensorID,
		AlertLevel:            a.AlertLevel,
		DeformationSpeedMMDay: &speed,
		ThresholdMMDay:        &thr,
		AlertTimestamp:        at.UTC(),
	})
}

func parseMessage(wm wireMessage) threshold.Message {
	m := threshold.Message{
		ProjectID:  wm.ProjectID,
		SensorID:   wm.SensorID,
		SensorType: wm.SensorType,
	}
	if ts, err := time.Parse(time.RFC3339, wm.Timestamp); err == nil {
		m.Timestamp = ts.UTC()
		m.HasTime = true
	}
	if wm.Payload != nil {
		if fq, ok := wm.Payload["fix_quality"].(string); ok {
			m.FixQuality = fq
		}
		if cum, ok := wm.Payload["cumulative_displacement_mm"].(float64); ok {
			m.CumulativeDisplacementMM = cum
			m.HasCumulative = true
		}
	}
	return m
}
