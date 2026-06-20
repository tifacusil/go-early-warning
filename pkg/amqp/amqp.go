// Package amqp wraps RabbitMQ connection setup with publisher confirms and a
// dead-letter exchange (DLX), per CLAUDE.md §2.1 / §4. Producers confirm before
// clearing buffers; consumers route exhausted messages to sensors.dead_letter.
package amqp

import "time"

// Config holds broker connection settings (externalized, CLAUDE.md §1.7).
type Config struct {
	Host      string
	Port      int
	User      string
	Pass      string
	Heartbeat time.Duration

	// Exchange/queue topology.
	AlertsExchange string // e.g. sensors.raw
	DLX            string // dead-letter exchange, e.g. sensors.dlx
	DeadLetter     string // dead-letter queue, e.g. sensors.dead_letter
}

// TODO(SE-practice): implement Connect() returning a connection that enables
// publisher confirms, declares the DLX + dead-letter queue, and a Publisher that
// blocks on confirm before returning success (Hito 2/3). Skeleton only.
