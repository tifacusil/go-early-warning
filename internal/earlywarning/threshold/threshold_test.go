package threshold

import (
	"testing"
	"time"
)

func cfg() Config {
	return Config{MaxDTSeconds: 7200, NewID: func() string { return "fixed-id" }}
}

var shangtan = Levels{Red: 20, Orange: 15, Blue: 10}

func baseMsg() Message {
	return Message{
		ProjectID:                "shangtan",
		SensorID:                 "s1",
		SensorType:               "gnss",
		Timestamp:                time.Date(2026, 6, 20, 1, 0, 0, 0, time.UTC),
		HasTime:                  true,
		CumulativeDisplacementMM: 100,
		HasCumulative:            true,
		FixQuality:               "rtk_fixed",
	}
}

func TestSkipReasons(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Message)
		thr    *Levels
		want   string
	}{
		{"not gnss", func(m *Message) { m.SensorType = "rainfall" }, &shangtan, "sensor_type_not_gnss"},
		{"fix not rtk", func(m *Message) { m.FixQuality = "single" }, &shangtan, "fix_quality_not_rtk_fixed"},
		{"no project", func(m *Message) { m.ProjectID = "" }, &shangtan, "missing_project_id"},
		{"no sensor", func(m *Message) { m.SensorID = "" }, &shangtan, "missing_sensor_id"},
		{"unknown project", func(m *Message) {}, nil, "unknown_project"},
		{"bad ts", func(m *Message) { m.HasTime = false }, &shangtan, "bad_timestamp"},
		{"no cum", func(m *Message) { m.HasCumulative = false }, &shangtan, "missing_cumulative_displacement"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := baseMsg()
			c.mutate(&m)
			r := cfg().Evaluate(m, nil, c.thr)
			if r.SkipReason != c.want {
				t.Fatalf("want skip %q, got %q", c.want, r.SkipReason)
			}
			if r.Alert != nil {
				t.Fatalf("skip path must not alert")
			}
		})
	}
}

func TestFirstReadingStoresNoAlert(t *testing.T) {
	r := cfg().Evaluate(baseMsg(), nil, &shangtan)
	if !r.ShouldStoreCurrent || r.StorePayload == nil {
		t.Fatal("first reading must store baseline")
	}
	if r.Alert != nil {
		t.Fatal("first reading must not alert")
	}
}

func TestGapTooLargeResets(t *testing.T) {
	prev := &Reading{Timestamp: baseMsg().Timestamp.Add(-3 * time.Hour), CumulativeDisplacementMM: 0}
	r := cfg().Evaluate(baseMsg(), prev, &shangtan)
	if r.SkipReason != "gap_too_large_reset" || r.Alert != nil {
		t.Fatalf("expected gap reset, got skip=%q alert=%v", r.SkipReason, r.Alert)
	}
}

func TestNonPositiveDTOverwrites(t *testing.T) {
	prev := &Reading{Timestamp: baseMsg().Timestamp.Add(time.Hour), CumulativeDisplacementMM: 50}
	r := cfg().Evaluate(baseMsg(), prev, &shangtan)
	if r.Alert != nil || r.Note != "non_positive_dt_overwrite_baseline" {
		t.Fatalf("expected baseline overwrite, got %+v", r)
	}
}

func TestAlertEvaluatedAtUsesInjectedClock(t *testing.T) {
	fixed := time.Date(2026, 6, 20, 2, 0, 0, 0, time.UTC)
	c := Config{MaxDTSeconds: 7200, NewID: func() string { return "fixed-id" }, Now: func() time.Time { return fixed }}
	prev := &Reading{Timestamp: baseMsg().Timestamp.Add(-1 * time.Hour), CumulativeDisplacementMM: baseMsg().CumulativeDisplacementMM - 0.9}
	r := c.Evaluate(baseMsg(), prev, &shangtan)
	if r.Alert == nil {
		t.Fatal("expected alert")
	}
	if !r.Alert.EvaluatedAt.Equal(fixed) {
		t.Fatalf("want EvaluatedAt %v, got %v", fixed, r.Alert.EvaluatedAt)
	}
}

func TestSpeedAndLevels(t *testing.T) {
	// 1 hour apart; displacement delta drives mm/day = delta/3600*86400 = delta*24.
	prevTS := baseMsg().Timestamp.Add(-1 * time.Hour)
	cases := []struct {
		name      string
		delta     float64 // current - previous over 1h
		wantLevel string
		wantAlert bool
	}{
		{"below blue", 0.4, "", false},  // 9.6 mm/day < 10
		{"blue", 0.5, "blue", true},     // 12 mm/day
		{"orange", 0.7, "orange", true}, // 16.8 mm/day
		{"red", 0.9, "red", true},       // 21.6 mm/day
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := baseMsg()
			prev := &Reading{Timestamp: prevTS, CumulativeDisplacementMM: m.CumulativeDisplacementMM - c.delta}
			r := cfg().Evaluate(m, prev, &shangtan)
			if c.wantAlert {
				if r.Alert == nil {
					t.Fatalf("expected alert level %s, got none", c.wantLevel)
				}
				if r.Alert.AlertLevel != c.wantLevel {
					t.Fatalf("want level %s, got %s (speed=%v)", c.wantLevel, r.Alert.AlertLevel, r.Alert.DeformationSpeedMMDay)
				}
				if r.Alert.MessageID != "fixed-id" {
					t.Fatalf("expected injected id")
				}
				if r.Alert.EvaluatedAt.IsZero() {
					t.Fatalf("expected EvaluatedAt set")
				}
			} else if r.Alert != nil {
				t.Fatalf("expected no alert, got level %s", r.Alert.AlertLevel)
			}
		})
	}
}
