// Package publisher emits alerts over three channels, ported from
// alert_publisher.py: RabbitMQ (alerts.<project>.<level>), an optional HTTP
// webhook, and Firebase Cloud Messaging (FCM) push. All outbound config is
// externalized (CLAUDE.md §1.7). HTTP uses an explicit timeout (§1.5).
package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/threshold"
)

// Config holds publisher settings.
type Config struct {
	AlertsExchange string        // e.g. sensors.raw
	HTTPAlertURL   string        // optional webhook; "" disables
	HTTPTimeout    time.Duration // explicit (CLAUDE.md §1.5)
}

// Pusher sends FCM notifications. Implemented by the firebase adapter; injected
// so the publisher and its tests stay free of the Firebase SDK.
type Pusher interface {
	Push(ctx context.Context, title, body string, data map[string]string, tokens []string) error
}

// Publisher fans an alert out to RabbitMQ + HTTP + FCM.
type Publisher struct {
	cfg     Config
	channel *amqp.Channel // may be nil (HTTP/push still work)
	http    *http.Client
	push    Pusher // may be nil
	log     *slog.Logger
}

// New builds a Publisher. channel/push may be nil to disable that path.
func New(cfg Config, channel *amqp.Channel, push Pusher, log *slog.Logger) *Publisher {
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 5 * time.Second
	}
	return &Publisher{
		cfg:     cfg,
		channel: channel,
		http:    &http.Client{Timeout: cfg.HTTPTimeout},
		push:    push,
		log:     log,
	}
}

// PublishRabbitMQ publishes the alert to alerts.<project>.<level>. No-op if no channel.
func (p *Publisher) PublishRabbitMQ(ctx context.Context, a *threshold.Alert) error {
	if p.channel == nil {
		return nil
	}
	body, err := json.Marshal(a)
	if err != nil {
		return err
	}
	routingKey := fmt.Sprintf("alerts.%s.%s", a.ProjectID, a.AlertLevel)
	return p.channel.PublishWithContext(ctx, p.cfg.AlertsExchange, routingKey, false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    a.MessageID,
			Body:         body,
		})
}

// PublishHTTP posts the alert to the configured webhook. No-op if URL empty.
func (p *Publisher) PublishHTTP(ctx context.Context, a *threshold.Alert) {
	if p.cfg.HTTPAlertURL == "" {
		return
	}
	body, err := json.Marshal(a)
	if err != nil {
		p.log.Warn("http_publish_marshal_failed", slog.String("err", err.Error()))
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.HTTPAlertURL, bytes.NewReader(body))
	if err != nil {
		p.log.Warn("http_publish_failed", slog.String("err", err.Error()))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.http.Do(req)
	if err != nil {
		p.log.Warn("http_publish_failed", slog.String("url", p.cfg.HTTPAlertURL), slog.String("err", err.Error()))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		p.log.Warn("http_publish_failed", slog.String("url", p.cfg.HTTPAlertURL), slog.Int("status", resp.StatusCode))
	}
}

// PublishPush sends an FCM notification for the alert. No-op if no pusher/tokens.
func (p *Publisher) PublishPush(ctx context.Context, a *threshold.Alert, tokens []string) {
	if p.push == nil || len(tokens) == 0 {
		return
	}
	data := map[string]string{
		"type":        "early_warning_alert",
		"project_id":  a.ProjectID,
		"sensor_id":   a.SensorID,
		"alert_level": a.AlertLevel,
		"message_id":  a.MessageID,
	}
	title := "Alerta " + a.AlertLevel
	body := fmt.Sprintf("Sensor %s velocidad %.3f mm/día", a.SensorID, a.DeformationSpeedMMDay)
	if err := p.push.Push(ctx, title, body, data, tokens); err != nil {
		p.log.Warn("push_publish_failed", slog.String("err", err.Error()))
	}
}
