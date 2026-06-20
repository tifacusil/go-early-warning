// Package metrics exposes Prometheus metrics on /metrics per CLAUDE.md §5:
// CPU, resident memory, throughput, latency percentiles (p50/p90/p95/p99),
// published/dropped counters, and circuit-breaker state.
package metrics

// TODO(SE-practice): wire prometheus/client_golang — process collectors plus
// custom counters/histograms, and a Handler() returning promhttp.Handler()
// (Hito 4). Skeleton only; kept dependency-free until then.
