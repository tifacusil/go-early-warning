// Package api ports the Flask HTTP layer of app.py: health, recent alerts, and
// report read/generate endpoints, plus report generation and the daily/weekly
// scheduler. Health is a real probe (CLAUDE.md §1.6). The data-analysis call is
// wrapped with an explicit timeout (§1.5); the circuit breaker lands in Hito 3.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/cache"
	dav1 "github.com/AngelloFD/go-early-warning/internal/earlywarning/grpc/dataanalysis/v1"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/store"
	"github.com/AngelloFD/go-early-warning/pkg/grpcx"
	"github.com/AngelloFD/go-early-warning/pkg/metrics"
)

// Deps bundles the API's collaborators.
type Deps struct {
	Service string
	Cache   *cache.Cache
	Store   *store.Store
	// data-analysis gRPC client + resilient caller (breaker/retry/timeout).
	// DataAnalysis may be nil to disable enrichment.
	DataAnalysis   dav1.AnalysisServiceClient
	DataAnalysisDo *grpcx.Caller
	Metrics        *metrics.Metrics // optional; nil disables /metrics + instrumentation
	Log            *slog.Logger
	// HealthProbes are named connectivity checks (mysql, rabbitmq, redis).
	HealthProbes map[string]func(ctx context.Context) error
}

// Server holds the API dependencies.
type Server struct {
	d Deps
}

// New builds a Server.
func New(d Deps) *Server {
	return &Server{d: d}
}

// DataAnalysisState exposes the breaker state for /metrics & /health (§2.3).
func (s *Server) DataAnalysisState() string {
	if s.d.DataAnalysisDo == nil {
		return "closed"
	}
	return s.d.DataAnalysisDo.State().String()
}

// Routes registers all handlers on a fresh mux and returns it.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.instrument("/health", s.health))
	mux.HandleFunc("GET /alerts/recent", s.instrument("/alerts/recent", s.alertsRecent))
	mux.HandleFunc("GET /reports/latest", s.instrument("/reports/latest", s.reportsLatest))
	mux.HandleFunc("POST /reports/generate", s.instrument("/reports/generate", s.reportsGenerate))
	if s.d.Metrics != nil {
		mux.Handle("GET /metrics", s.d.Metrics.Handler())
	}
	return mux
}

// instrument records per-route handler latency (CLAUDE.md §5). No-op without Metrics.
func (s *Server) instrument(route string, h http.HandlerFunc) http.HandlerFunc {
	if s.d.Metrics == nil {
		return h
	}
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h(w, r)
		s.d.Metrics.HTTPDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	}
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

// enrich calls data-analysis over gRPC through the circuit breaker. Returns nil
// on any failure OR when the breaker is open — the report is still produced from
// simple threshold data (degradación a umbral simple, continuidad operativa §2.3).
func (s *Server) enrich(ctx context.Context, projectID, period, windowStart, windowEnd string) map[string]any {
	if s.d.DataAnalysis == nil || s.d.DataAnalysisDo == nil {
		return nil
	}
	var resp *dav1.AnalyzeResponse
	err := s.d.DataAnalysisDo.Invoke(ctx, func(c context.Context) error {
		var e error
		resp, e = s.d.DataAnalysis.Analyze(c, &dav1.AnalyzeRequest{
			ProjectId:   projectID,
			Period:      period,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
		})
		return e
	})
	if s.d.Metrics != nil {
		s.d.Metrics.BreakerState.WithLabelValues("data-analysis").Set(float64(s.d.DataAnalysisDo.State()))
	}
	if err != nil {
		if err == grpcx.ErrCircuitOpen {
			s.d.Log.Warn("data_analysis_circuit_open", slog.String("project", projectID))
		}
		return nil // degradación: reporte sin data_analysis
	}
	out := map[string]any{"trend": resp.GetTrend(), "model_version": resp.GetModelVersion()}
	for k, v := range resp.GetFields() {
		out[k] = v
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
