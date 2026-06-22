// Package logging provides structured JSON logging with a correlation/trace ID,
// per CLAUDE.md §1.8 / §5. Every log line carries service, instance_id and
// message_id/trace_id so a message can be followed across services.
package logging

import (
	"context"
	"log/slog"
	"os"
)

// ctxKey is the unexported type for context values owned by this package.
type ctxKey struct{ name string }

var traceIDKey = ctxKey{"trace_id"}

// New returns a JSON slog.Logger bound to a service and instance, writing to
// stdout at the given level ("debug"/"info"/"warn"/"error"; default info).
func New(service, instanceID, level string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(h).With(
		slog.String("service", service),
		slog.String("instance_id", instanceID),
	)
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug", "DEBUG":
		return slog.LevelDebug
	case "warn", "WARN", "warning":
		return slog.LevelWarn
	case "error", "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithTraceID stores a trace/correlation ID (e.g. message_id) on the context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceID extracts the trace ID set by WithTraceID; empty if absent.
func TraceID(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

// FromContext returns a logger annotated with the context's trace_id, if any.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if id := TraceID(ctx); id != "" {
		return base.With(slog.String("trace_id", id))
	}
	return base
}
