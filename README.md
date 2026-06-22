# early-warning

Landslide **early-warning** microservice — a Go rewrite of the Python baseline
(`early-warning-demo`), part of the 6-service monitoring system described in
Bai et al. (2020). It consumes GNSS sensor readings, evaluates deformation
speed against per-project thresholds, and emits graded alerts
(`blue` / `orange` / `red`). It also produces periodic (daily/weekly) reports.

This repository contains **only this microservice**. The other five services and
the shared infrastructure live elsewhere (see
[ROADMAP.md](./ROADMAP.md#fuera-de-alcance-de-este-repo)). The engineering rules
this service follows are defined in [CLAUDE.md](./CLAUDE.md).

---

## What it does

```
sensors.raw ──▶ early_warning.evaluate ──▶ [ early-warning ] ──▶ alerts (sensors.raw)
                       (RabbitMQ)                  │                 + MySQL + push (FCM)
                                                   ├─▶ data-analysis  (gRPC, report enrichment)
                                                   └─▶ user-management (gRPC, FCM tokens)
```

- **Consumes** sensor readings from the RabbitMQ queue `early_warning.evaluate`.
- **Evaluates** deformation speed (mm/day) per the threshold engine; crossing a
  level produces an `alert`.
- **Publishes** each alert to RabbitMQ (`alerts.<project>.<level>` on
  `sensors.raw`), optionally to an HTTP webhook, and as an FCM push.
- **Persists** alerts and reports in its own MySQL database.
- **Serves** an HTTP API: `/health`, `/metrics`, `/alerts/recent`,
  `/reports/latest`, `/reports/generate`.
- **Degrades gracefully**: if `data-analysis` is unreachable, reports are still
  produced from the simple threshold data (circuit breaker open → no enrichment).

### Runtime dependencies

| Dependency | Used for | Protocol |
|---|---|---|
| RabbitMQ | consume readings, publish alerts | AMQP |
| MySQL (own DB) | persist alerts & reports | TCP |
| Redis (shared) | last reading, counters, dedup, recent alerts | TCP |
| data-analysis | report enrichment | gRPC (`:50051`) |
| user-management | resolve FCM tokens | gRPC (`:9090`) |
| Consul | service registration / discovery | HTTP |
| Firebase (FCM) | push notifications | HTTPS (optional) |

---

## Building the container

The image is built from the multi-stage [Dockerfile](./Dockerfile)
(static `CGO_ENABLED=0` binary on a minimal Alpine runtime, non-root user,
listening on container port `8080`):

```bash
docker build -t ghcr.io/tifacusil/early-warning:latest .
```

---

## Running detached

The service expects to reach its dependencies by hostname on the shared
`landslide-net` Docker network. The command below is the **`docker run`
equivalent of the infra compose**
([`tesis-infra/.../early-warning/docker-compose.yml`](../../otros/tesis-infra/local/services/early-warning/docker-compose.yml)):
same image, same network, same `env_file`, same `INSTANCE_ID` / `HTTP_PORT`
overrides, host port `8087:8080`, restart policy and health check.

```bash
docker run -d \
  --name early-warning-1 \
  --hostname early-warning-1 \
  --network landslide-net \
  --restart unless-stopped \
  --init \
  -p 8087:8080 \
  --env-file ../tesis-infra/local/services/envs/early-warning.env \
  -e INSTANCE_ID=early-warning-01 \
  -e HTTP_PORT=8080 \
  --health-cmd 'wget -qO- http://127.0.0.1:8080/health || exit 1' \
  --health-interval 10s \
  --health-timeout 2s \
  --health-retries 3 \
  --health-start-period 20s \
  ghcr.io/tifacusil/early-warning:latest
```

> The canonical way to start it (both replicas, `8087`/`8088`) is through infra:
> `make svc-up SVC=early-warning`. The `docker run` above mirrors a single
> instance for local debugging.

On startup the service **runs its database migrations** (idempotent), connects
to RabbitMQ/Redis/MySQL, registers in Consul, and starts serving on `:8080`.

### Quick check

```bash
curl http://localhost:8087/health        # {"status":"healthy","checks":{"mysql":"ok","redis":"ok"},...}
curl http://localhost:8087/metrics       # Prometheus exposition
```

### Key configuration (env)

All configuration is externalized — see
[`early-warning.env`](../../otros/tesis-infra/local/services/envs/early-warning.env).
Notable variables: `RABBITMQ_*`, `REDIS_*`, `MYSQL_*`, `DATA_ANALYSIS_GRPC`,
`USER_MANAGEMENT_GRPC`, `CIRCUIT_BREAKER_*`, `HTTP_PORT`, `INSTANCE_ID`,
`LOG_LEVEL`, `RUN_MIGRATIONS`.

---

## Software-engineering & microservice practices

Each practice below is mandated by [CLAUDE.md](./CLAUDE.md) and motivated by the
empirical evidence the rewrite aims to validate (El Fellah et al. 2026; Schwarz
et al. 2025; Aderaldo et al. 2024; Taibi & Lenarduzzi 2018).

### Contract-first

- **Where:** [`contracts/schemas/`](./contracts/schemas) (JSON Schema for AMQP
  messages: `sensor_reading`, `alert`, `report`),
  [`contracts/proto/`](./contracts/proto) (gRPC `.proto` for synchronous calls),
  [`contracts/openapi/`](./contracts/openapi) (external HTTP API), validated by
  [`tools/contract-validate`](./tools/contract-validate).
- **Why:** no message or RPC is published/consumed without a versioned contract,
  so producers and consumers evolve independently and breakage is caught in CI,
  not in production. A semantic `schema_version` envelope enforces compatible
  evolution (additive → minor; breaking → major).

### Database-per-service

- **Where:** [`internal/earlywarning/store`](./internal/earlywarning/store) +
  [`store/migrations`](./internal/earlywarning/store/migrations); the service
  touches **only** its own `early_warning` schema. Data owned by other services
  is fetched over gRPC, never by cross-database queries.
- **Why:** avoids the *Shared Persistency* anti-pattern (empirically linked to
  coupling and change-failure rate, Taibi & Lenarduzzi 2018). Each service owns
  its data and schema lifecycle.

### Migrations at startup (idempotent, versioned)

- **Where:** [`store/migrate.go`](./internal/earlywarning/store/migrate.go) —
  `golang-migrate` over **embedded** (`go:embed`) per-table migrations, run from
  [`main.go`](./cmd/early-warning/main.go) before serving.
- **Why:** the binary is self-sufficient (no external migration step), and the
  applied version is tracked in `schema_migrations`, so re-running on every boot
  is a safe no-op. The service never issues ad-hoc DDL on request paths.

### Idempotent consumer

- **Where:** [`consumer.go`](./internal/earlywarning/consumer/consumer.go)
  dedups by `message_id` via
  [`pkg/idempotency`](./pkg/idempotency) (Redis `SET NX EX`) **before**
  processing; [`store.RecordAlert`](./internal/earlywarning/store/store.go) uses
  `INSERT IGNORE` keyed by `message_id` as a second guard.
- **Why:** RabbitMQ guarantees at-least-once delivery, so duplicates must not
  produce duplicate alerts. Never assume exactly-once.

### Resilient synchronous calls (timeout + retry + circuit breaker)

- **Where:** [`pkg/grpcx`](./pkg/grpcx) wraps every outbound gRPC call
  (data-analysis enrichment in [`api`](./internal/earlywarning/api/api.go),
  FCM-token lookup in [`subs`](./internal/earlywarning/subs)) with an explicit
  per-call timeout, exponential backoff (1s/5s/30s) on transient codes, and a
  circuit breaker. ([`pkg/httpx`](./pkg/httpx) provides the same for HTTP.)
- **Why:** a slow or dead dependency must not cascade. When the data-analysis
  breaker is **open**, early-warning degrades to simple-threshold alerts instead
  of stalling — operational continuity is a hard requirement (CLAUDE.md §2.3;
  Saleh Sedghpour et al. 2022; Aderaldo et al. 2024).

### Dead-letter queue + bounded retry

- **Where:** [`consumer.go`](./internal/earlywarning/consumer/consumer.go) —
  retryable failures are requeued a bounded number of times (Redis-tracked),
  then `nack`'d without requeue so the queue's pre-declared
  `x-dead-letter-exchange` (`sensors.dlx`) routes them to `sensors.dead_letter`.
- **Why:** no infinite requeue loops; poison messages are isolated for
  inspection.

### Real health checks

- **Where:** `/health` in [`api.go`](./internal/earlywarning/api/api.go) probes
  **actual** connectivity to Redis and MySQL and returns `503` when degraded;
  registered in Consul with a 10s HTTP check via
  [`pkg/consul`](./pkg/consul).
- **Why:** a static `200 OK` hides broken dependencies; the load balancer and
  Consul need the truth to route around unhealthy instances.

### Externalized configuration

- **Where:** [`pkg/config`](./pkg/config); all hosts, ports, credentials, queue
  and exchange names come from environment variables (the infra `env_file`).
- **Why:** the same image runs in any environment without rebuilds; no secrets
  or topology are hardcoded.

### Structured logging with correlation ID

- **Where:** [`pkg/logging`](./pkg/logging) (JSON via `slog`); the consumer
  binds `message_id` as `trace_id` so a message can be followed end-to-end.
- **Why:** machine-parsable logs with a correlation ID make the
  ingest → evaluate → alert flow traceable across the system.

### Observability (Prometheus metrics)

- **Where:** [`pkg/metrics`](./pkg/metrics) exposes `/metrics`: process CPU /
  resident memory, throughput counters, evaluation-latency histogram, per-route
  HTTP latency, and circuit-breaker state.
- **Why:** the rewrite's hypothesis (throughput, latency, CPU, memory vs. the
  Python baseline) is only verifiable with real metrics.

### Stateless replicas behind a round-robin load balancer

- **Where:** the service holds no local session state (state lives in Redis /
  MySQL), so infra runs **two instances** (`early-warning-1/-2`) behind an
  Nginx + consul-template gateway that load-balances round-robin over the
  Consul-healthy instances.
- **Why:** horizontal scalability and availability — the polling/round-robin
  strategy of the reference architecture (Bai et al. 2020).

### Testing

- **Where:** unit tests for the pure logic
  ([`threshold`](./internal/earlywarning/threshold),
  [`subs`](./internal/earlywarning/subs) quiet-hours,
  [`grpcx`](./pkg/grpcx)/[`httpx`](./pkg/httpx) breaker,
  [`metrics`](./pkg/metrics)) plus contract tests
  ([`tools/contract-validate`](./tools/contract-validate)).
- **Why:** the business logic is isolated from I/O so it is fast and
  deterministic to test; contracts are checked on every change.

---

## Development

```bash
make build              # go build ./...
make test               # go test ./...
make vet                # go vet ./...
make contract-validate  # validate examples against schemas
make proto              # regenerate gRPC stubs (needs buf + protoc-gen-go[-grpc])
make ci                 # vet + build + test + contract-validate
```
