// Package idempotency provides a dedup store keyed by message_id so every
// RabbitMQ consumer can drop already-seen messages before processing, per
// CLAUDE.md §1.3 / §2.2. Never assume exactly-once delivery.
package idempotency

import "context"

// Store records seen message IDs. Implementations: MySQL (INSERT IGNORE /
// ON CONFLICT DO NOTHING) or Redis SET with TTL.
type Store interface {
	// Seen reports whether messageID was already processed, and atomically
	// marks it seen. Returns (true, nil) if this is the first time.
	Seen(ctx context.Context, messageID string) (first bool, err error)
}

// TODO(SE-practice): add MySQL and Redis implementations (Hito 3). Skeleton only.
