package api

import (
	"context"
	"log/slog"
	"time"
)

// Scheduler periodically generates the last complete daily/weekly report for
// each known project, ported from app.ReportScheduler. Idempotent: skips a
// window whose report already exists.
type Scheduler struct {
	srv      *Server
	projects []string
	interval time.Duration
	log      *slog.Logger
}

// NewScheduler builds a Scheduler over the given project ids.
func NewScheduler(srv *Server, projects []string, interval time.Duration, log *slog.Logger) *Scheduler {
	if interval <= 0 {
		interval = time.Minute
	}
	return &Scheduler{srv: srv, projects: projects, interval: interval, log: log}
}

// Run ticks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now().UTC()
	for _, p := range s.projects {
		s.maybeGenerate(ctx, p, "daily", 5*time.Minute, now)
		s.maybeGenerate(ctx, p, "weekly", 10*time.Minute, now)
	}
}

// maybeGenerate generates the report for the last complete window once it has
// settled by grace, and only if it does not already exist.
func (s *Scheduler) maybeGenerate(ctx context.Context, project, period string, grace time.Duration, now time.Time) {
	ws, we := windowFor(period, now)
	if now.Before(we.Add(grace)) {
		return
	}
	exists, err := s.srv.d.Store.ReportExists(ctx, project, period, ws, we)
	if err != nil {
		s.log.Warn("scheduler_exists_check_failed", slog.String("project", project), slog.String("err", err.Error()))
		return
	}
	if exists {
		return
	}
	if _, err := s.srv.GenerateReport(ctx, project, period); err != nil {
		s.log.Warn("scheduler_generate_failed", slog.String("project", project), slog.String("period", period), slog.String("err", err.Error()))
	}
}
