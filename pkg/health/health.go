// Package health implements real health checks: /health verifies actual
// connectivity to RabbitMQ and the service database, never a static 200,
// per CLAUDE.md §1.6 / §4.
package health

import "context"

// Check is a single named dependency probe (e.g. "mysql", "rabbitmq").
type Check struct {
	Name  string
	Probe func(ctx context.Context) error
}

// Status is the aggregate result returned by /health.
type Status struct {
	Status string            `json:"status"` // "healthy" | "degraded"
	Checks map[string]string `json:"checks"` // name -> "ok" | error string
}

// TODO(SE-practice): add Handler(checks ...Check) http.HandlerFunc that runs all
// probes and reports 200/503 (Hito 2/4). Skeleton only.
