//go:build integration

// Integration test against the real early_warning MySQL (infra must be up).
// Run: EW_MYSQL_DSN host:port go test -tags=integration ./internal/earlywarning/store/
package store

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

// newTestStore builds a Store from EW_MYSQL_* env, skipping if unset.
func newTestStore(t *testing.T) *Store {
	host := os.Getenv("EW_MYSQL_HOST")
	if host == "" {
		t.Skip("EW_MYSQL_HOST not set; skipping integration test")
	}
	port, _ := strconv.Atoi(os.Getenv("EW_MYSQL_PORT"))
	st, err := New(Config{
		Host: host, Port: port,
		User: os.Getenv("EW_MYSQL_USER"), Pass: os.Getenv("EW_MYSQL_PASSWORD"),
		DB: os.Getenv("EW_MYSQL_DATABASE"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestSummarizeTotalIncludesUnseededLevels pins the store.go Total fix: Total
// must equal the row count even for a level not in the seed map.
func TestSummarizeTotalIncludesUnseededLevels(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	project := "itest_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	ws := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)
	at := ws.Add(time.Hour)

	t.Cleanup(func() {
		_, _ = st.db.ExecContext(ctx, "DELETE FROM alerts WHERE project_id = ?", project)
	})

	sp := 12.5
	thr := 10.0
	rows := []AlertRow{
		{MessageID: project + "-1", ProjectID: project, SensorID: "s1", AlertLevel: "red", DeformationSpeedMMDay: &sp, ThresholdMMDay: &thr, AlertTimestamp: at},
		{MessageID: project + "-2", ProjectID: project, SensorID: "s1", AlertLevel: "blue", DeformationSpeedMMDay: &sp, ThresholdMMDay: &thr, AlertTimestamp: at},
		// "yellow" is NOT in the seed map; the Total bug would have dropped it.
		{MessageID: project + "-3", ProjectID: project, SensorID: "s1", AlertLevel: "yellow", DeformationSpeedMMDay: &sp, ThresholdMMDay: &thr, AlertTimestamp: at},
	}
	for _, r := range rows {
		if err := st.RecordAlert(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	sum, err := st.SummarizeAlerts(ctx, project, ws, we)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total != 3 {
		t.Fatalf("Total: want 3 (incl. unseeded 'yellow'), got %d", sum.Total)
	}
	if sum.ByLevel["red"] != 1 || sum.ByLevel["blue"] != 1 {
		t.Fatalf("ByLevel wrong: %+v", sum.ByLevel)
	}
}

// TestRecordAlertIdempotent pins INSERT IGNORE: a replayed message_id no-ops.
func TestRecordAlertIdempotent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	project := "itest_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	at := time.Date(2030, 2, 1, 0, 0, 0, 0, time.UTC)
	t.Cleanup(func() {
		_, _ = st.db.ExecContext(ctx, "DELETE FROM alerts WHERE project_id = ?", project)
	})

	sp, thr := 1.0, 1.0
	r := AlertRow{MessageID: project + "-dup", ProjectID: project, SensorID: "s1", AlertLevel: "red", DeformationSpeedMMDay: &sp, ThresholdMMDay: &thr, AlertTimestamp: at}
	for i := 0; i < 2; i++ {
		if err := st.RecordAlert(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM alerts WHERE message_id = ?", r.MessageID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want 1 row after replay, got %d", n)
	}
}
