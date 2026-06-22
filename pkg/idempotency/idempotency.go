// Package idempotency provides a dedup store keyed by message_id so every
// RabbitMQ consumer can drop already-seen messages before processing, per
// CLAUDE.md §1.3 / §2.2. Never assume exactly-once delivery.
package idempotency

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store records successfully-processed message IDs. A message is marked done
// only AFTER processing succeeds, so a failed+requeued message is not mistaken
// for a duplicate on redelivery (the retry/DLQ path stays alive).
type Store interface {
	// Done reports whether messageID was already processed successfully.
	Done(ctx context.Context, messageID string) (bool, error)
	// MarkDone records messageID as processed. Call after process() succeeds.
	MarkDone(ctx context.Context, messageID string) error
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

// Done reports whether prefix+messageID exists (already processed).
func (s *RedisStore) Done(ctx context.Context, messageID string) (bool, error) {
	n, err := s.c.Exists(ctx, s.prefix+messageID).Result()
	return n > 0, err
}

// MarkDone sets prefix+messageID with TTL so the dedup window self-expires.
func (s *RedisStore) MarkDone(ctx context.Context, messageID string) error {
	return s.c.Set(ctx, s.prefix+messageID, "1", s.ttl).Err()
}
