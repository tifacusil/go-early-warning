# ROADMAP — Microservicio `early-warning` (Python → Go)

> Reescritura en Go del microservicio `early-warning` (de Bai et al. 2020),
> aplicando **sin excepción** las prácticas de [CLAUDE.md](./CLAUDE.md).
> **Alcance de este repo: solo el servicio `early-warning`.** Es 1 de los 6 microservicios
> del sistema; los otros 5 y el benchmark del sistema completo viven fuera (ver
> [Fuera de alcance](#fuera-de-alcance-de-este-repo)). El baseline Python
> (`early-warning-demo`) implementa este mismo servicio.

---

## Estado de partida

| Repo | Contenido |
|---|---|
| `early-warning-demo` (Python) | Servicio `early-warning`: Flask API, consumer RabbitMQ, threshold engine, Redis cache, MySQL (alerts/reports), Consul registrar, Firebase push. **MySQL único compartido** (viola database-per-service). |
| `go-early-warning` (Go) | Solo `CLAUDE.md`. Sin `go.mod`, sin git, sin código. |

### Brechas del baseline vs. CLAUDE.md (a corregir durante el port)
- **Database-per-service violado** → MySQL único `early_warning_db` con tabla `alerts` esquema crudo. Strangler Fig incremental.
- **Sin idempotencia** → consumer hace `nack+requeue` sin dedup por `message_id`.
- **Sin contratos** → mensajes/endpoints sin JSON Schema ni OpenAPI versionado en `contracts/`.
- **Sin circuit breaker / retry / backoff** → llamada HTTP `early-warning → data-analysis` usa timeout simple, sin breaker.
- **Sin DLQ** → fallo de proceso reintenta infinito vía requeue.
- **Health check parcial** → `/health` devuelve estático, no verifica RabbitMQ ni MySQL reales.
- **Sin métricas Prometheus, sin logging estructurado JSON, sin trace_id.**
- **Sin testing** (unit / contract / CDCT / integration).

---

## Estructura del proyecto

Layout Go de **un solo servicio** (`early-warning`).

```
go-early-warning/
├── CLAUDE.md
├── ROADMAP.md
├── go.mod / go.sum                 # módulo github.com/AngelloFD/go-early-warning (Go 1.26)
├── Makefile                        # build, test, lint, vet, contract-validate, ci
├── Dockerfile                      # -> ghcr.io/tifacusil/early-warning
│                                   # infra (tesis-infra) provee compose/env, no este repo
│
├── contracts/
│   ├── schemas/                    # JSON Schema 2020-12: sensor_reading, alert, report
│   ├── openapi/                    # early-warning.yaml + contratos consumidos
│   │                               #   (data-analysis, user-management)
│   └── examples/                   # ejemplos validados en CI
│
├── cmd/
│   └── early-warning/main.go       # entrypoint: cablea todo + shutdown por señal
│
├── internal/earlywarning/          # lógica del servicio (no importable fuera)
│   ├── threshold/                  # lógica pura, sin I/O (port threshold_engine.py)
│   ├── consumer/                   # RabbitMQ: dedup + DLQ + fan-out
│   ├── publisher/                  # alertas: AMQP + HTTP + FCM (firebase.go)
│   ├── subs/                       # cliente user-management (tokens FCM, quiet hours)
│   ├── cache/                      # Redis
│   ├── store/                      # MySQL (alerts, reports)
│   └── api/                        # handlers HTTP + /metrics + scheduler
│
├── pkg/                            # libs (reutilizables por otros servicios del sistema)
│   ├── logging/                    # slog JSON + trace_id
│   ├── config/                     # env
│   ├── httpx/                      # timeout + circuit breaker + retry
│   ├── idempotency/                # dedup por message_id (Redis SET NX)
│   ├── metrics/                    # Prometheus
│   ├── consul/                     # register / deregister
│   ├── amqp/                       # esqueleto (publisher confirms/DLX) — TODO data-receiving
│   └── health/                     # esqueleto (health usa probes en api/)
│
├── tools/contract-validate/        # valida examples vs schemas + versionado
├── migrations/early_warning/       # golang-migrate (alerts, reports)
└── deploy/                         # artefactos de despliegue
```

**Convención de contratos (Hito 1):**
- Envelope con `schema_version` semántico (`major.minor`).
- Aditivo + opcional → bump `minor`. Incompatible → bump `major` + período de gracia (CLAUDE.md §1.4).
- Todo `*.example.json` debe validar contra su schema en CI (`make contract-validate`).
- Ningún handler (Hito 2+) se escribe sin su schema/OpenAPI presente aquí primero.

---

## Hito 0 — Fundación del repositorio

**Objetivo:** repo Go inicializado, layout estándar, tooling, CI base. Sin lógica de negocio.

- [x] `git init` + `.gitignore` + `.gitattributes` (eol=lf).
- [x] `go.mod` (módulo `github.com/AngelloFD/go-early-warning`), Go 1.26 (forzado por deps: hashicorp/consul/api).
- [x] Layout monorepo: `cmd/<service>/`, `internal/<service>/`, `pkg/`, `contracts/`, `tools/`, `migrations/`, `deploy/`.
- [x] `pkg/` compartido — esqueletos: `logging` (JSON+trace_id, ya implementado), `config` (env), `httpx` (breaker/retry — esqueleto), `health`, `metrics`, `amqp` (confirms+DLX — esqueleto), `idempotency` (esqueleto), `consul` (implementado).
- [x] Makefile: `build`, `test`, `lint`, `vet`, `contract-validate`, `ci`.
- [x] CI (GitHub Actions): `go vet`, `golangci-lint`, `go test ./...`, `contract-validate` (Go 1.26).
- [x] `Dockerfile` multi-stage → `ghcr.io/tifacusil/early-warning`. Infra (tesis-infra) provee compose/env por servicio; **este repo no lleva compose**.

**DoD:** `go build && go test` verde; imagen Docker construye. ✅

---

## Hito 1 — Contratos (contract-first, prerequisito de todo handler)

**Objetivo:** JSON Schemas + OpenAPI versionados ANTES de portar lógica. CLAUDE.md §1.1 bloquea handlers sin contrato.

- [x] `contracts/schemas/*.schema.json` (JSON Schema Draft 2020-12) con envelope `schema_version` semántico:
  - [x] `sensor_reading` (mensaje `sensors.raw` → `early_warning.evaluate`): campos `message_id`, `project_id`, `sensor_id`, `sensor_type`, `timestamp`, `payload{cumulative_displacement_mm, fix_quality,...}`.
  - [x] `alert` (mensaje publicado a `alerts.<project>.<level>`): derivado del alert dict de `threshold_engine.py:132`.
  - [x] `report` (entidad de reportes daily/weekly).
- [x] `contracts/openapi/early-warning.yaml` — port del `_openapi_spec()` de `app.py`: `/health`, `/alerts/recent`, `/reports/latest`, `/reports/generate`.
- [x] OpenAPI para los servicios síncronos consumidos: `data-analysis` (endpoint que enriquece reporte), `user-management` (`/api/v1/users/subscriptions`).
- [x] `contracts/examples/` — mensajes ejemplo válidos por schema.
- [x] `contract-validate` tool (Go) — valida examples contra schemas + regla envelope `schema_version` major.minor (CLAUDE.md §1.4). Unit test incluido.

**DoD:** todos los examples validan; CI falla si un schema rompe versionado. ✅ (`make contract-validate` verde)

---

## Hito 2 — Port del servicio `early-warning` (núcleo, paridad funcional)

**Objetivo:** reescribir el único servicio existente con paridad funcional + prácticas CLAUDE.md. Pieza central de la tesis (de aquí salen los benchmarks).

### 2.1 Lógica de negocio pura (sin I/O)
- [x] `internal/earlywarning/threshold` — port de `threshold_engine.py`: `Config.Evaluate(msg, prev, levels) → Result`. Velocidad de deformación, niveles blue/orange/red, gap reset, dt máximo. **Unit-testeable sin deps** (`NewID` inyectable).
- [x] Umbrales por proyecto inyectados vía `ThresholdProvider` (no en lógica). `staticLevels` provisional en `cmd/`, `// TODO(SE-practice):` → project-management (Hito 5).

### 2.2 Adaptadores I/O
- [x] `internal/earlywarning/cache` (Redis, go-redis/v9) — port de `redis_cache.py`: last reading, alerts_generated, recent alerts, last_message_processed_at, `Ping` para /health.
- [x] `internal/earlywarning/store` (MySQL, go-sql-driver) — alerts + reports. Migraciones `golang-migrate` en `migrations/early_warning/` (CLAUDE.md §2.4), **NO** `CREATE TABLE` en runtime. `RecordAlert` usa `INSERT IGNORE` keyed por `message_id` (base de idempotencia Hito 3).
- [x] `internal/earlywarning/consumer` (RabbitMQ, amqp091) — port de `consumer.py`: consume `early_warning.evaluate`, evalúa, fan-out, ack tras procesar.
- [x] `internal/earlywarning/publisher` — port de `alert_publisher.py`: publish RabbitMQ (`alerts.<p>.<l>`), HTTP webhook, push FCM (`Pusher` inyectable).
- [x] `internal/earlywarning/api` (HTTP, net/http) — port de `app.py`: `/health` (probe real Redis+MySQL), `/alerts/recent`, `/reports/latest`, `/reports/generate`, generación de reportes + scheduler daily/weekly idempotente.
- [ ] OpenAPI/`/docs` servidos en runtime — spec ya en `contracts/openapi/`; servirlo es menor. `// TODO`.

### 2.3 Integraciones externas
- [x] Subscriptions fetch desde `user-management` (HTTP) — `internal/earlywarning/subs`: filtros push_enabled / min_alert_level / quiet hours (red override), unit-testeado.
- [x] Firebase Admin SDK Go (FCM) — `firebase.google.com/go/v4`, multicast, no-op si sin credenciales. Port de `publish_notification`.
- [x] Consul registrar — `pkg/consul`: self-register, health check 10s, deregister, tags `lang-go`/`version-1`/`env-`.

**DoD:** `early-warning` Go compila, `cmd/early-warning` cablea todo, vet+test verdes. Paridad funcional vs baseline. Verificación contra infra real (alertas idénticas) → pendiente, requiere docker-compose levantado (Hito 6 integration).

---

## Hito 3 — Resiliencia (CLAUDE.md §4, no negociable)

**Objetivo:** aplicar todos los patrones de resiliencia al servicio portado. Núcleo de la hipótesis empírica (Bloque 3: disponibilidad >99% sin ML Service).

- [x] **Idempotent Consumer** — dedup por `message_id` en Redis (`SET NX EX`, `pkg/idempotency.RedisStore`) **antes** de procesar; duplicado → ack+skip. Además `store.RecordAlert` usa `INSERT IGNORE` keyed por `message_id` (doble red). (CLAUDE.md §1.3, §2.2)
- [x] **Retry + backoff exponencial** — `pkg/httpx`: máx reintentos + backoff 1s/5s/30s en HTTP saliente; consumer requeue acotado (contador Redis `IncrRetry`) antes de DLQ. (CLAUDE.md §4)
- [x] **Dead Letter Queue** — consumer `nack(requeue=false)` tras N intentos → la queue `early_warning.evaluate` (con `x-dead-letter-exchange: sensors.dlx`, pre-declarada en infra) enruta a `sensors.dead_letter`. Sin requeue infinito.
- [ ] **Publisher Confirms** — N/A crítico en early-warning (publica alerta best-effort + persiste en MySQL como verdad durable). Aplica a `data-receiving` Hito 5. `// TODO(SE-practice):` en `pkg/amqp`.
- [x] **Circuit Breaker** `early-warning → data-analysis` (HTTP, `pkg/httpx`) — abre tras `CIRCUIT_BREAKER_FAILURE_THRESHOLD` fallos, half-open tras `..._RESET_TIMEOUT_MS`. **Degradación a umbral simple**: breaker abierto → `enrich` devuelve nil → reporte sin `data_analysis` (continuidad operativa §2.3). Estado vía `Server.DataAnalysisState()` (expuesto en `/metrics` Hito 4).
- [x] **Timeout explícito** en toda llamada HTTP saliente (httpx + subs + publisher; nunca default del cliente).

**DoD:** lógica verificada por unit tests de `pkg/httpx` (breaker open/half-open/closed, retry-on-5xx) + dedup idempotente. Test de caos end-to-end (matar data-analysis/consumer contra infra real) → Hito 6 integration.

---

## Hito 4 — Observabilidad (CLAUDE.md §5)

**Objetivo:** logs, métricas, health, tracing en el servicio portado (patrón replicable a los demás).

- [x] **Logging JSON estructurado** (`pkg/logging`, slog JSON) — campos `timestamp`, `level`, `service`, `instance_id`, `trace_id` (= `message_id` vía `WithTraceID`/`FromContext`), `event`. Nivel por `LOG_LEVEL`. Reemplaza el `logger.info("eval sensor=%s...")` del baseline.
- [x] **Métricas Prometheus** `/metrics` (`pkg/metrics`, client_golang) — process collector (CPU `process_cpu_seconds_total`, `process_resident_memory_bytes`) + Go collector; custom: `ew_messages_consumed_total`, `ew_messages_dropped_total`, `ew_duplicates_skipped_total`, `ew_alerts_published_total{level}`, `ew_eval_duration_seconds` (native histogram → percentiles), `ew_http_request_duration_seconds{route,method}`, `ew_circuit_breaker_state{target}`. Label `service` por instancia.
- [x] **Health check real** `/health` — probes Redis + MySQL, 503 si degradado (hecho en Hito 2).
- [ ] **Distributed tracing** (OpenTelemetry) — `trace_id` (= `message_id`) ya propagado en logs por contexto. OTel/exporter completo **diferido**: sin collector en infra no aporta. `// TODO(SE-practice):` (CLAUDE.md §5 lo marca "recomendado", no obligatorio).

**DoD:** `/metrics` scrape-able (proceso + custom + breaker); mensaje rastreable por `trace_id` en logs JSON. Tracing distribuido OTel diferido a cuando infra tenga collector.

---

## Hito 5 — Verificación y cierre del servicio

**Objetivo:** cerrar `early-warning`: completar la pirámide de testing (CLAUDE.md §3) y los huecos pendientes de hitos previos. Último hito **de este repo**.

- [x] **Unit tests** — `threshold` (skip paths, niveles, gap, dt), `subs` (quiet hours), `httpx` (breaker), `metrics` (handler). Sin RabbitMQ/MySQL/Consul.
- [x] **Contract tests** — `tools/contract-validate`: examples validan contra schemas + regla de versionado.
- [ ] **Integration tests** — Testcontainers (RabbitMQ + MySQL + Redis) para el flujo crítico: ingesta `early_warning.evaluate` → evaluación → alerta persistida + publicada. Cierra el DoD "alertas idénticas al baseline".
- [ ] **Test de caos** — matar data-analysis ⇒ sigue alertando por umbral simple (breaker abierto); reprocesar mensaje ⇒ no duplica (idempotencia). Verifica resiliencia de Hito 3 end-to-end.
- [ ] **Huecos pendientes** — servir `/openapi.json` + `/docs` en runtime (spec ya en `contracts/openapi/`); OTel distribuido (diferido hasta collector en infra).

**DoD:** integration + caos verdes contra infra efímera; servicio empaquetable (`Dockerfile` ✅) y desplegable por `tesis-infra`.

---

## Fuera de alcance de este repo

`go-early-warning` implementa **un** microservicio. Lo siguiente pertenece al sistema completo (otros repos / `tesis-infra`), no aquí:

- **Otros 5 microservicios** — `data-receiving`, `data-management`, `data-analysis`, `user-management`, `project-management`. Cada uno su propio repo/imagen, reutilizando el patrón de `pkg/` de este servicio. `early-warning` ya los consume vía contratos (`contracts/openapi/`) y env (`DATA_ANALYSIS_URL`, `USER_MANAGEMENT_URL`).
- **CDCT (Pact / `pact-go`)** — requiere la contraparte (productor/consumidor del otro lado). Se monta cuando exista el servicio par.
- **Benchmark Go vs. Python** (El Fellah et al. 2026) — validación empírica cross-repo a nivel tesis; se corre sobre el sistema desplegado por `tesis-infra`, no desde este repo.
- **`project-management` para umbrales** — hoy `staticLevels` provisional en `cmd/` (`// TODO(SE-practice):`); migra a cliente HTTP cuando exista ese servicio.

## Trazabilidad CLAUDE.md (este servicio)

| Principio CLAUDE.md | Hito |
|---|---|
| §1.1 Contract-first | 1 |
| §1.2 Database-per-service | 2 |
| §1.3 Idempotencia | 3 |
| §1.4 Versionado contratos | 1 |
| §1.5 Resiliencia HTTP | 3 |
| §1.6 Health checks reales | 2 |
| §1.7 Config externalizada | 0, 2 |
| §1.8 Logging + correlation ID | 4 |
| §3 Testing | 1–4 (por componente), 5 (integración) |
| §4 Resiliencia | 3 |
| §5 Observabilidad | 4 |
