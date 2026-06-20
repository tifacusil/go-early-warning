// Package config externalizes all configuration via environment variables
// (and, later, Consul KV), per CLAUDE.md §1.7. No host, port, credential or
// queue/exchange name is ever hardcoded in service code.
package config

import (
	"os"
	"strconv"
	"time"
)

// String returns env[key] or def if unset/empty.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Int returns env[key] parsed as int, or def on unset/parse error.
func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// Bool returns true when env[key] is one of 1/true/yes/y (case-insensitive).
func Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "TRUE", "True", "yes", "y":
		return true
	default:
		return false
	}
}

// Duration parses env[key] as a Go duration (e.g. "5s"), falling back to def.
func Duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// TODO(SE-practice): add Consul KV source as a fallback layer behind env (CLAUDE.md §1.7).
