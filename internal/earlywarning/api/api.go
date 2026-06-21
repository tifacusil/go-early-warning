// Package api ports the Flask HTTP layer of app.py: health, recent alerts, and
// report read/generate endpoints, plus report generation and the daily/weekly
// scheduler. Health is a real probe (CLAUDE.md §1.6). The data-analysis call is
// wrapped with an explicit timeout (§1.5); the circuit breaker lands in Hito 3.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/cache"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/store"
)

// Deps bundles the API's collaborators.
type Deps struct {
	Service          string
	Cache            *cache.Cache
	Store            *store.Store
	DataAnalysisURL  string
	DataAnalysisTO   time.Duration
	Log              *slog.Logger
	// HealthProbes are named connectivity checks (mysql, rabbitmq, redis).
	HealthProbes map[string]func(ctx context.Context) error
}

// Server holds the API dependencies and an HTTP client for data-analysis.
type Server struct {
	d    Deps
	http *http.Client
}

// New builds a Server.
func New(d Deps) *Server {
	to := d.DataAnalysisTO
	if to <= 0 {
		to = 5 * time.Second
	}
	return &Server{d: d, http: &http.Client{Timeout: to}}
}

// Routes registers all handlers on a fresh mux and returns it.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /alerts/recent", s.alertsRecent)
	mux.HandleFunc("GET /reports/latest", s.reportsLatest)
	mux.HandleFunc("POST /reports/generate", s.reportsGenerate)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{}
	status := "healthy"
	for name, probe := range s.d.HealthProbes {
		if err := probe(r.Context()); err != nil {
			checks[name] = err.Error()
			status = "degraded"
		} else {
			checks[name] = "ok"
		}
	}
	gen, _ := s.d.Cache.GetAlertsGenerated(r.Context())
	code := http.StatusOK
	if status != "healthy" {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status":           status,
		"service":          s.d.Service,
		"alerts_generated": gen,
		"checks":           checks,
	})
}

func (s *Server) alertsRecent(w http.ResponseWriter, r *http.Request) {
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	alerts, err := s.d.Cache.GetRecentAlerts(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "cache_error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": alerts})
}

func (s *Server) reportsLatest(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.URL.Query().Get("project_id"))
	period := normPeriod(r.URL.Query().Get("period"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project_id_required"})
		return
	}
	rep, err := s.d.Store.GetLatestReport(r.Context(), projectID, period)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "store_error"})
		return
	}
	if rep == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(rep)
}

func (s *Server) reportsGenerate(w http.ResponseWriter, r *http.Request) {
	projectID := strings.TrimSpace(r.URL.Query().Get("project_id"))
	period := normPeriod(r.URL.Query().Get("period"))
	if projectID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project_id_required"})
		return
	}
	if period != "daily" && period != "weekly" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_period"})
		return
	}
	rep, err := s.GenerateReport(r.Context(), projectID, period)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "generate_failed", "detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func normPeriod(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "daily"
	}
	return v
}

// GenerateReport builds and persists a report for the most recent complete
// window, optionally enriched by data-analysis (port of _generate_report).
func (s *Server) GenerateReport(ctx context.Context, projectID, period string) (map[string]any, error) {
	now := time.Now().UTC()
	ws, we := windowFor(period, now)

	summary, err := s.d.Store.SummarizeAlerts(ctx, projectID, ws, we)
	if err != nil {
		return nil, err
	}
	gen, _ := s.d.Cache.GetAlertsGenerated(ctx)
	last, _ := s.d.Cache.GetLastMessageProcessedAt(ctx)

	report := map[string]any{
		"schema_version": "1.0",
		"report_id":      uuid.NewString(),
		"project_id":     projectID,
		"period":         period,
		"window_start":   iso(ws),
		"window_end":     iso(we),
		"generated_at":   iso(now),
		"summary":        summary,
		"system": map[string]any{
			"service":                   s.d.Service,
			"alerts_generated_total":    gen,
			"last_message_processed_at": nilIfEmpty(last),
		},
	}
	if enriched := s.enrich(ctx, projectID, period, iso(ws), iso(we)); enriched != nil {
		report["data_analysis"] = enriched
	}

	body, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	if err := s.d.Store.UpsertReport(ctx, report["report_id"].(string), projectID, period, ws, we, now, body); err != nil {
		return nil, err
	}
	return report, nil
}

// enrich calls data-analysis; returns nil on any failure (degradation to simple
// threshold — the report is still produced). Circuit breaker added in Hito 3.
func (s *Server) enrich(ctx context.Context, projectID, period, windowStart, windowEnd string) map[string]any {
	if s.d.DataAnalysisURL == "" {
		return nil
	}
	payload, _ := json.Marshal(map[string]string{
		"project_id": projectID, "period": period,
		"window_start": windowStart, "window_end": windowEnd,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.d.DataAnalysisURL, bytes.NewReader(payload))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil
	}
	var out map[string]any
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return nil
	}
	return out
}

// windowFor returns the last complete daily/weekly window in UTC.
func windowFor(period string, now time.Time) (time.Time, time.Time) {
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if period == "weekly" {
		// Monday-based week (Go: Sunday=0).
		offset := (int(dayStart.Weekday()) + 6) % 7
		weekStart := dayStart.AddDate(0, 0, -offset)
		return weekStart.AddDate(0, 0, -7), weekStart
	}
	return dayStart.AddDate(0, 0, -1), dayStart
}

func iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05Z") }

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
