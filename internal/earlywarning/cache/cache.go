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

// Client exposes the underlying Redis client (e.g. for the idempotency store).
func (k *Cache) Client() *redis.Client { return k.c }

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

// baselineCAS stores the reading only if the stored one is older.
//
// Sharding the consumer by sensor_id serialises a sensor's messages within a
// replica, but not across replicas: two pods can hold two messages of the same
// sensor at once, both read the same baseline, and both write it back. The
// later write wins, so if the older message finishes last the baseline rewinds
// to an earlier instant — the exact failure the threshold engine refuses to
// cause on its own (threshold.go, "stale_reading_baseline_kept"), reintroduced
// by the store. Measured cost when it happened: the alert rate went from 3.61%
// to 5.12% just by raising the worker count.
//
// The guard is a compare-and-set rather than a lock: it encodes the invariant
// ("the baseline never moves backwards in time") in the one place that can
// enforce it atomically, costs the same single round trip as the plain SET, and
// has no lease to renew or leak if a pod dies mid-evaluation. What it does NOT
// prevent is two replicas evaluating against the same slightly stale baseline;
// that yields a speed averaged over a longer window, which is conservative, and
// converges as soon as one of the writes lands.
//
// Comparison is on timestamp_unix_us, not on the ISO string: sub-second ISO
// timestamps do not sort lexicographically ("…:00Z" > "…:00.5Z" as text, and
// backwards in time). Microseconds are finer than the wire format (which
// carries milliseconds) and stay exact as a Lua/cjson double.
//
// A baseline written before this field existed has no timestamp_unix_us and is
// overwritten unconditionally — the previous behaviour, self-healing after one
// write.
var baselineCAS = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if raw then
  local ok, stored = pcall(cjson.decode, raw)
  if ok and type(stored) == 'table' then
    local at = tonumber(stored.timestamp_unix_us)
    if at and at >= tonumber(ARGV[2]) then
      return 0
    end
  end
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
`)

// SetLastReading stores the new baseline for a sensor, unless a newer one is
// already stored. Reports whether the write landed.
func (k *Cache) SetLastReading(ctx context.Context, sensorID string, r *threshold.Reading) (bool, error) {
	at := r.Timestamp.UTC()
	micros := at.UnixMicro()
	payload, err := json.Marshal(map[string]any{
		// RFC3339Nano, not RFC3339: the latter drops the sub-second part, which
		// shortens the next reading's dt and understates its deformation speed.
		// The Python original stores isoformat(), which keeps it.
		"timestamp":                  at.Format(time.RFC3339Nano),
		"timestamp_unix_us":          micros,
		"cumulative_displacement_mm": r.CumulativeDisplacementMM,
	})
	if err != nil {
		return false, err
	}
	written, err := baselineCAS.Run(ctx, k.c, []string{lastKey(sensorID)}, payload, micros).Int()
	if err != nil {
		return false, err
	}
	return written == 1, nil
}

// IncrRetry increments the per-message retry counter (1h TTL so it self-cleans)
// and returns the new count. Used by the consumer to bound requeues before DLQ.
func (k *Cache) IncrRetry(ctx context.Context, messageID string) (int64, error) {
	key := "early_warning:retry:" + messageID
	n, err := k.c.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if n == 1 {
		_ = k.c.Expire(ctx, key, time.Hour).Err()
	}
	return n, nil
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
