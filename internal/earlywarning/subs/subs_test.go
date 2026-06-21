package subs

import (
	"testing"
	"time"
)

func at(h, m int) time.Time {
	return time.Date(2026, 6, 20, h, m, 0, 0, time.UTC)
}

func TestInQuietHours(t *testing.T) {
	cases := []struct {
		name       string
		start, end string
		now        time.Time
		want       bool
	}{
		{"inside same-day", "22:00:00", "23:00:00", at(22, 30), true},
		{"outside same-day", "22:00:00", "23:00:00", at(21, 0), false},
		{"wrap inside before midnight", "22:00:00", "06:00:00", at(23, 0), true},
		{"wrap inside after midnight", "22:00:00", "06:00:00", at(5, 0), true},
		{"wrap outside", "22:00:00", "06:00:00", at(12, 0), false},
		{"empty bounds", "", "", at(3, 0), false},
		{"hh:mm form", "22:00", "23:00", at(22, 30), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := inQuietHours(c.start, c.end, c.now); got != c.want {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func TestParseClockInvalid(t *testing.T) {
	if _, ok := parseClock("nope"); ok {
		t.Fatal("expected invalid")
	}
}
