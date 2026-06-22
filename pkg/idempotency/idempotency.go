// Package idempotency provides a dedup store keyed by message_id so every
// RabbitMQ consumer can drop already-seen messages before processing, per
// CLAUDE.md §1.3 / §2.2. Never assume exactly-once delivery.
package idempotency

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store records seen message IDs.
type Store interface {
	// Seen atomically marks messageID as seen and reports whether this is the
	// first time. first=false means a duplicate that must be skipped.
	Seen(ctx context.Context, messageID string) (first bool, err error)
}

// RedisStore implements Store with SET NX EX: the first writer wins, the key
// self-expires after TTL so the dedup window is bounded (CLAUDE.md §1.3).
type RedisStore struct {
	c      *redis.Client
	prefix string
	ttl    time.Duration
}

// NewRedisStore builds a RedisStore. ttl<=0 defaults to 24h.
func NewRedisStore(c *redis.Client, prefix string, ttl time.Duration) *RedisStore {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if prefix == "" {
		prefix = "idemp:"
	}
	return &RedisStore{c: c, prefix: prefix, ttl: ttl}
}

// Seen sets prefix+messageID only if absent. ok==true => first occurrence.
func (s *RedisStore) Seen(ctx context.Context, messageID string) (bool, error) {
	return s.c.SetNX(ctx, s.prefix+messageID, "1", s.ttl).Result()
}
