// Package store is the MySQL persistence for early-warning (alerts + reports),
// ported from the MySQL half of redis_cache.py. The versioned schema lives in
// store/migrations (embedded) and is applied via golang-migrate at startup
// (Migrate); migrations are idempotent and tracked in schema_migrations, so the
// service never issues ad-hoc DDL in request paths (CLAUDE.md §2.4).
// Database-per-service (§1.2).
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config holds MySQL connection settings.
type Config struct {
	Host           string
	Port           int
	User           string
	Pass           string
	DB             string
	ConnectTimeout time.Duration
}

// Store wraps a *sql.DB scoped to the early_warning schema.
type Store struct {
	db *sql.DB
}

// New opens a pooled connection (does not ping; call Ping for health).
func New(cfg Config) (*Store, error) {
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=%s&loc=UTC",
		cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.DB, cfg.ConnectTimeout)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Ping verifies DB connectivity (used by /health, CLAUDE.md §1.6).
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Close releases the pool.
func (s *Store) Close() error { return s.db.Close() }

// AlertRow is the persisted shape of an alert.
type AlertRow struct {
	MessageID             string
	ProjectID             string
	SensorID              string
	AlertLevel            string
	DeformationSpeedMMDay *float64
	ThresholdMMDay        *float64
	AlertTimestamp        time.Time
}

// RecordAlert inserts an alert. INSERT IGNORE keyed by the unique message_id makes
// this idempotent (CLAUDE.md §1.3 / §2.2): replays of the same message_id no-op.
func (s *Store) RecordAlert(ctx context.Context, a AlertRow) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT IGNORE INTO alerts
			(created_at, message_id, project_id, sensor_id, alert_level,
			 deformation_speed_mm_day, threshold_mm_day, alert_timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		time.Now().UTC(), a.MessageID, a.ProjectID, a.SensorID, a.AlertLevel,
		a.DeformationSpeedMMDay, a.ThresholdMMDay, a.AlertTimestamp,
	)
	return err
}

// UpsertReport stores or replaces a report by its (project, period, window) key.
func (s *Store) UpsertReport(ctx context.Context, reportID, projectID, period string,
	windowStart, windowEnd, generatedAt time.Time, contentJSON []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO reports
			(report_id, project_id, period, window_start, window_end, generated_at, content_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			report_id = VALUES(report_id),
			generated_at = VALUES(generated_at),
			content_json = VALUES(content_json)`,
		reportID, projectID, period, windowStart, windowEnd, generatedAt, contentJSON,
	)
	return err
}

// GetLatestReport returns the most recent report content for a project/period, or nil.
func (s *Store) GetLatestReport(ctx context.Context, projectID, period string) (json.RawMessage, error) {
	var content []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT content_json FROM reports
		WHERE project_id = ? AND period = ?
		ORDER BY generated_at DESC LIMIT 1`, projectID, period).Scan(&content)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(content), nil
}

// ReportExists reports whether a report for the exact window already exists.
func (s *Store) ReportExists(ctx context.Context, projectID, period string, windowStart, windowEnd time.Time) (bool, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		SELECT id FROM reports
		WHERE project_id = ? AND period = ? AND window_start = ? AND window_end = ? LIMIT 1`,
		projectID, period, windowStart, windowEnd).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// AlertSummary aggregates alerts in a window for report generation.
type AlertSummary struct {
	Total      int            `json:"total"`
	ByLevel    map[string]int `json:"by_level"`
	MaxSpeed   *float64       `json:"max_speed_mm_day"`
	AvgSpeed   *float64       `json:"avg_speed_mm_day"`
}

// SummarizeAlerts ports summarize_alerts_db: counts + speed stats by level.
func (s *Store) SummarizeAlerts(ctx context.Context, projectID string, windowStart, windowEnd time.Time) (AlertSummary, error) {
	out := AlertSummary{ByLevel: map[string]int{"blue": 0, "orange": 0, "red": 0}}
	rows, err := s.db.QueryContext(ctx, `
		SELECT alert_level, COUNT(*) AS cnt,
		       MAX(deformation_speed_mm_day) AS max_speed,
		       AVG(deformation_speed_mm_day) AS avg_speed
		FROM alerts
		WHERE project_id = ? AND alert_timestamp >= ? AND alert_timestamp < ?
		GROUP BY alert_level`, projectID, windowStart, windowEnd)
	if err != nil {
		return out, err
	}
	defer rows.Close()

	var sumAvg float64
	var sumCnt int
	for rows.Next() {
		var level string
		var cnt int
		var maxSpeed, avgSpeed sql.NullFloat64
		if err := rows.Scan(&level, &cnt, &maxSpeed, &avgSpeed); err != nil {
			return out, err
		}
		if _, ok := out.ByLevel[level]; ok {
			out.ByLevel[level] = cnt
		}
		if maxSpeed.Valid {
			if out.MaxSpeed == nil || maxSpeed.Float64 > *out.MaxSpeed {
				v := maxSpeed.Float64
				out.MaxSpeed = &v
			}
		}
		if avgSpeed.Valid && cnt > 0 {
			sumAvg += avgSpeed.Float64 * float64(cnt)
			sumCnt += cnt
		}
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	out.Total = out.ByLevel["blue"] + out.ByLevel["orange"] + out.ByLevel["red"]
	if sumCnt > 0 {
		v := sumAvg / float64(sumCnt)
		out.AvgSpeed = &v
	}
	return out, nil
}
