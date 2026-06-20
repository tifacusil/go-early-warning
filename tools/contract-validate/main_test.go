package main

import "testing"

func TestCheckSchemaVersion(t *testing.T) {
	cases := []struct {
		name string
		inst any
		ok   bool
	}{
		{"valid", map[string]any{"schema_version": "1.0"}, true},
		{"valid two digit", map[string]any{"schema_version": "12.34"}, true},
		{"missing", map[string]any{}, false},
		{"not string", map[string]any{"schema_version": 1.0}, false},
		{"patch level rejected", map[string]any{"schema_version": "1.0.0"}, false},
		{"empty", map[string]any{"schema_version": ""}, false},
		{"not object", []any{"x"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkSchemaVersion(c.inst)
			if c.ok && err != nil {
				t.Fatalf("want ok, got %v", err)
			}
			if !c.ok && err == nil {
				t.Fatalf("want error, got nil")
			}
		})
	}
}
