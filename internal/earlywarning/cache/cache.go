// Package cache wraps the Redis state for early-warning, ported from the Redis
// half of redis_cache.py: per-sensor last reading, alert counter, recent alerts
// ring, and last-processed timestamp. Config externalized (CLAUDE.md §1.7).
package cache

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/threshold"
)

const (
	keyAlertsGenerated = "early_warning:alerts_generated"
	keyRecentAlerts    = "early_warning:alerts:recent"
	keyLastProcessed   = "early_warning:last_message_processed_at"
	recentKeep         = 200
)

// Config holds Redis connection settings.
type Config struct {
	Host string
	Port int
	Pass string
}

// Cache is the Redis-backed state store.
type Cache struct {
	c *redis.Client
}

// New builds a Cache from config.
func New(cfg Config) *Cache {
	return &Cache{c: redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Password: cfg.Pass,
	})}
}

// Ping verifies connectivity (used by the /health probe, CLAUDE.md §1.6).
func (k *Cache) Ping(ctx context.Context) error { return k.c.Ping(ctx).Err() }

func lastKey(sensorID string) string { return "alarm:last:" + sensorID }

// GetLastReading returns the stored baseline for a sensor, or nil if absent.
func (k *Cache) GetLastReading(ctx context.Context, sensorID string) (*threshold.Reading, error) {
	raw, err := k.c.Get(ctx, lastKey(sensorID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var stored struct {
		Timestamp                string  `json:"timestamp"`
		CumulativeDisplacementMM float64 `json:"cumulative_displacement_mm"`
	}
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return nil, nil // corrupt entry -> treat as no baseline (matches Python)
	}
	ts, err := time.Parse(time.RFC3339, stored.Timestamp)
	if err != nil {
		return &threshold.Reading{CumulativeDisplacementMM: stored.CumulativeDisplacementMM}, nil
	}
	return &threshold.Reading{Timestamp: ts.UTC(), CumulativeDisplacementMM: stored.CumulativeDisplacementMM}, nil
}

// SetLastReading stores the new baseline for a sensor.
func (k *Cache) SetLastReading(ctx context.Context, sensorID string, r *threshold.Reading) error {
	payload, err := json.Marshal(map[string]any{
		"timestamp":                  r.Timestamp.UTC().Format(time.RFC3339),
		"cumulative_displacement_mm": r.CumulativeDisplacementMM,
	})
	if err != nil {
		return err
	}
	return k.c.Set(ctx, lastKey(sensorID), payload, 0).Err()
}

// IncrAlertsGenerated bumps and returns the lifetime alert counter.
func (k *Cache) IncrAlertsGenerated(ctx context.Context) (int64, error) {
	return k.c.Incr(ctx, keyAlertsGenerated).Result()
}

// GetAlertsGenerated returns the lifetime alert counter (0 if unset).
func (k *Cache) GetAlertsGenerated(ctx context.Context) (int64, error) {
	v, err := k.c.Get(ctx, keyAlertsGenerated).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n, nil
}

// PushRecentAlert prepends an alert to the bounded recent list.
func (k *Cache) PushRecentAlert(ctx context.Context, a *threshold.Alert) error {
	body, err := json.Marshal(a)
	if err != nil {
		return err
	}
	pipe := k.c.TxPipeline()
	pipe.LPush(ctx, keyRecentAlerts, body)
	pipe.LTrim(ctx, keyRecentAlerts, 0, recentKeep-1)
	_, err = pipe.Exec(ctx)
	return err
}

// GetRecentAlerts returns up to limit (clamped 0..200) recent alerts as raw JSON.
func (k *Cache) GetRecentAlerts(ctx context.Context, limit int) ([]json.RawMessage, error) {
	if limit < 0 {
		limit = 0
	}
	if limit > recentKeep {
		limit = recentKeep
	}
	if limit == 0 {
		return []json.RawMessage{}, nil
	}
	items, err := k.c.LRange(ctx, keyRecentAlerts, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, 0, len(items))
	for _, it := range items {
		out = append(out, json.RawMessage(it))
	}
	return out, nil
}

// SetLastMessageProcessedAt records the last consume timestamp.
func (k *Cache) SetLastMessageProcessedAt(ctx context.Context, iso string) error {
	return k.c.Set(ctx, keyLastProcessed, iso, 0).Err()
}

// GetLastMessageProcessedAt returns the last consume timestamp, or "" if unset.
func (k *Cache) GetLastMessageProcessedAt(ctx context.Context) (string, error) {
	v, err := k.c.Get(ctx, keyLastProcessed).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}
