// Package threshold holds the pure deformation-speed evaluation logic, ported
// from threshold_engine.py. No I/O, no RabbitMQ/Redis/MySQL — fully unit-testable
// (CLAUDE.md §2.1, §3.1).
package threshold

import (
	"math"
	"time"

	"github.com/google/uuid"
)

const schemaVersion = "1.0"

// Levels holds the per-project alert thresholds in mm/day.
type Levels struct {
	Red    float64
	Orange float64
	Blue   float64
}

// Reading is the minimal stored state for a sensor between evaluations.
type Reading struct {
	Timestamp                time.Time
	CumulativeDisplacementMM float64
}

// Alert mirrors contracts/schemas/alert.schema.json.
type Alert struct {
	SchemaVersion          string  `json:"schema_version"`
	MessageID              string  `json:"message_id"`
	ProjectID              string  `json:"project_id"`
	SensorID               string  `json:"sensor_id"`
	AlertType              string  `json:"alert_type"`
	AlertLevel             string  `json:"alert_level"`
	DeformationSpeedMMDay  float64 `json:"deformation_speed_mm_day"`
	CurrentDisplacementMM  float64 `json:"current_displacement_mm"`
	PreviousDisplacementMM float64 `json:"previous_displacement_mm"`
	ThresholdMMDay         float64 `json:"threshold_mm_day"`
	Timestamp              string  `json:"timestamp"`

	// EvaluatedAt is the moment the threshold was crossed (not part of the
	// wire contract): start of controlled latency measurement threshold->FCM ack.
	EvaluatedAt time.Time `json:"-"`
}

// Result is the outcome of evaluating one message.
type Result struct {
	// ShouldStoreCurrent indicates the current reading must become the new baseline.
	ShouldStoreCurrent bool
	StorePayload       *Reading
	// Alert is non-nil only when a threshold was crossed.
	Alert *Alert
	// SkipReason / Note are diagnostics for structured logging (empty when none).
	SkipReason string
	Note       string
}

// Message is the parsed sensor reading evaluated by the engine. The consumer
// decodes the AMQP body (contracts/schemas/sensor_reading.schema.json) into this.
type Message struct {
	ProjectID  string
	SensorID   string
	SensorType string
	Timestamp  time.Time
	HasTime    bool
	// Payload fields relevant to gnss.
	CumulativeDisplacementMM float64
	HasCumulative            bool
	FixQuality               string
}

// MaxDTSeconds bounds the gap between consecutive readings; a larger gap resets
// the baseline instead of computing a speed (port of config.MAX_DT_SECONDS).
type Config struct {
	MaxDTSeconds float64
	// NewID and Now are injected so the engine stays deterministic in tests.
	NewID func() string
	Now   func() time.Time
}

// DefaultConfig returns production defaults (2h gap, real UUID v4).
func DefaultConfig(maxDTSeconds float64) Config {
	if maxDTSeconds <= 0 {
		maxDTSeconds = 7200
	}
	return Config{MaxDTSeconds: maxDTSeconds, NewID: func() string { return uuid.NewString() }, Now: time.Now}
}

// pickLevel returns the crossed level and its threshold, or ("", 0, false).
func pickLevel(speedMMDay float64, t Levels) (string, float64, bool) {
	switch {
	case speedMMDay >= t.Red:
		return "red", t.Red, true
	case speedMMDay >= t.Orange:
		return "orange", t.Orange, true
	case speedMMDay >= t.Blue:
		return "blue", t.Blue, true
	default:
		return "", 0, false
	}
}

func iso(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// Evaluate ports threshold_engine.evaluate_message. previous may be nil (first
// reading for the sensor). thresholds nil means the project is unknown.
func (c Config) Evaluate(m Message, previous *Reading, thresholds *Levels) Result {
	if m.SensorType != "gnss" {
		return Result{SkipReason: "sensor_type_not_gnss"}
	}
	if m.FixQuality != "rtk_fixed" {
		return Result{SkipReason: "fix_quality_not_rtk_fixed"}
	}
	if m.ProjectID == "" {
		return Result{SkipReason: "missing_project_id"}
	}
	if m.SensorID == "" {
		return Result{SkipReason: "missing_sensor_id"}
	}
	if thresholds == nil {
		return Result{SkipReason: "unknown_project"}
	}
	if !m.HasTime {
		return Result{SkipReason: "bad_timestamp"}
	}
	if !m.HasCumulative || !isFinite(m.CumulativeDisplacementMM) {
		return Result{SkipReason: "missing_cumulative_displacement"}
	}

	store := &Reading{Timestamp: m.Timestamp, CumulativeDisplacementMM: m.CumulativeDisplacementMM}

	if previous == nil {
		return Result{ShouldStoreCurrent: true, StorePayload: store, Note: "previous_none"}
	}
	if previous.Timestamp.IsZero() || !isFinite(previous.CumulativeDisplacementMM) {
		return Result{ShouldStoreCurrent: true, StorePayload: store, Note: "previous_invalid_reset"}
	}

	dtSeconds := m.Timestamp.Sub(previous.Timestamp).Seconds()
	if dtSeconds <= 0 {
		return Result{ShouldStoreCurrent: true, StorePayload: store, Note: "non_positive_dt_overwrite_baseline"}
	}
	if dtSeconds > c.MaxDTSeconds {
		return Result{ShouldStoreCurrent: true, StorePayload: store, SkipReason: "gap_too_large_reset"}
	}

	speedMMDay := (m.CumulativeDisplacementMM - previous.CumulativeDisplacementMM) / dtSeconds * 86400.0

	level, thr, ok := pickLevel(speedMMDay, *thresholds)
	if !ok {
		return Result{ShouldStoreCurrent: true, StorePayload: store, Note: "alert_none"}
	}

	now := c.Now
	if now == nil {
		now = time.Now
	}
	alert := &Alert{
		SchemaVersion:          schemaVersion,
		MessageID:              c.NewID(),
		ProjectID:              m.ProjectID,
		SensorID:               m.SensorID,
		AlertType:              "deformation_speed",
		AlertLevel:             level,
		DeformationSpeedMMDay:  round6(speedMMDay),
		CurrentDisplacementMM:  m.CumulativeDisplacementMM,
		PreviousDisplacementMM: previous.CumulativeDisplacementMM,
		ThresholdMMDay:         thr,
		Timestamp:              iso(m.Timestamp),
		EvaluatedAt:            now(),
	}
	return Result{ShouldStoreCurrent: true, StorePayload: store, Alert: alert}
}

func isFinite(f float64) bool { return !math.IsInf(f, 0) && !math.IsNaN(f) }

func round6(f float64) float64 { return math.Round(f*1e6) / 1e6 }
