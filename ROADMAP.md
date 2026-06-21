# ROADMAP — Reescritura `early-warning-demo` (Python) → `go-early-warning` (Go)

> Plan de hitos para reescribir el sistema de monitoreo de deslizamientos (Bai et al. 2020) en Go,
> aplicando **sin excepción** las prácticas de [CLAUDE.md](./CLAUDE.md).
> El sistema baseline en Python (`early-warning-demo`) implementa **1 de los 6 microservicios**
> objetivo: `early-warning`. El resto de la arquitectura debe construirse contract-first.
> Objetivo final: validar empíricamente mejora de rendimiento vs. baseline (marco El Fellah et al. 2026).

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

Layout monorepo Go. **Hito 1 arranca por `contracts/`** (contract-first, bloquea handlers).

```
go-early-warning/
├── CLAUDE.md
├── ROADMAP.md
├── go.mod                          # módulo github.com/AngelloFD/go-early-warning
├── go.sum
├── Makefile                        # build, test, lint, vet, contract-validate
├── docker-compose.dev.yml          # RabbitMQ, MySQL, Redis, Consul
│
├── contracts/                      # ◄── HITO 1 empieza aquí
│   ├── schemas/                    # JSON Schema Draft 2020-12
│   │   ├── sensor_reading.schema.json
│   │   ├── alert.schema.json
│   │   └── report.schema.json
│   ├── openapi/                    # OpenAPI 3.x por servicio
│   │   ├── early-warning.yaml
│   │   ├── data-analysis.yaml
│   │   └── user-management.yaml
│   └── examples/                   # mensajes ejemplo, validados en CI
│       ├── sensor_reading.example.json
│       ├── alert.example.json
│       └── report.example.json
│
├── cmd/                            # entrypoints (1 por servicio) — Hito 2+
│   ├── early-warning/main.go
│   ├── data-receiving/main.go
│   ├── data-management/main.go
│   ├── data-analysis/main.go
│   ├── user-management/main.go
│   └── project-management/main.go
│
├── internal/                       # lógica por servicio (no importable fuera) — Hito 2+
│   └── earlywarning/
│       ├── threshold/              # lógica pura, sin I/O (port threshold_engine.py)
│       ├── consumer/               # RabbitMQ consumer
│       ├── publisher/              # alertas: AMQP + HTTP + FCM
│       ├── cache/                  # Redis
│       ├── store/                  # MySQL (alerts, reports)
│       └── api/                    # handlers HTTP + scheduler
│
├── pkg/                            # libs compartidas — Hito 0
│   ├── logging/                    # JSON estructurado + trace_id
│   ├── config/                     # env / Consul KV
│   ├── httpx/                      # cliente: timeout + circuit breaker + retry
│   ├── amqp/                       # conn, publisher confirms, DLX
│   ├── idempotency/                # dedup por message_id
│   ├── health/                     # health check real (BD + RabbitMQ)
│   ├── metrics/                    # Prometheus
│   └── consul/                     # registrar / deregister
│
├── tools/
│   └── contract-validate/          # valida examples vs schemas (Hito 1)
│
├── migrations/                     # golang-migrate, 1 dir por esquema/servicio
│   └── early_warning/
│
└── deploy/                         # Dockerfiles, k8s/compose prod
```

**Convención de contratos (Hito 1):**
- Envelope con `schema_version` semántico (`major.minor`).
- Aditivo + opcional → bump `minor`. Incompatible → bump `major` + período de gracia (CLAUDE.md §1.4).
- Todo `*.example.json` debe validar contra su schema en CI (`make contract-validate`).
- Ningún handler (Hito 2+) se escribe sin su schema/OpenAPI presente aquí primero.

---

## Hito 0 — Fundación del repositorio

**Objetivo:** repo Go inicializado, layout estándar, tooling, CI base. Sin lógica de negocio.

- [ ] `git init` + `.gitignore` Go.
- [ ] `go.mod` (módulo `github.com/AngelloFD/go-early-warning`), Go 1.22+.
- [ ] Layout monorepo: `cmd/<service>/`, `internal/<service>/`, `pkg/` (libs compartidas), `contracts/`, `deploy/`.
- [ ] `pkg/` compartido — esqueletos: `logging` (JSON estructurado + trace_id), `config` (env/Consul KV), `httpx` (cliente con timeout/breaker/retry), `health`, `metrics` (Prometheus), `amqp` (conn + publisher confirms + DLX), `idempotency` (dedup store).
- [ ] Makefile / `Taskfile`: `build`, `test`, `lint`, `vet`, `contract-validate`.
- [ ] CI (GitHub Actions): `go vet`, `golangci-lint`, `go test ./...`, validación de contratos.
- [ ] `docker-compose.dev.yml`: RabbitMQ, MySQL (×N esquemas), Redis, Consul.

**DoD:** `make build && make test` verde con servicios vacíos.

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

- [ ] **Idempotent Consumer** — dedup por `message_id` (`INSERT IGNORE` / `ON CONFLICT DO NOTHING`) antes de procesar. Tabla/Redis de mensajes vistos. (CLAUDE.md §1.3, §2.2)
- [ ] **Retry + backoff exponencial** — consumidores y HTTP saliente: máx 3 intentos, backoff 1s/5s/30s, luego DLQ. (CLAUDE.md §4)
- [ ] **Dead Letter Queue** — `sensors.dlx` → `sensors.dead_letter` configurada en el consumer; nack sin requeue infinito.
- [ ] **Publisher Confirms** — confirmar antes de borrar buffer (aplica a Data Receiving en Hito 5; en early-warning, confirmar publish de alerta).
- [ ] **Circuit Breaker** `early-warning → data-analysis` (HTTP) — abre tras N fallos, half-open. **Degradación a umbral simple** mientras abierto (continuidad operativa obligatoria, CLAUDE.md §2.3). Estado expuesto en `/metrics`.
- [ ] **Timeout explícito** en toda llamada HTTP saliente (nunca default del cliente).

**DoD:** test de caos — matar `data-analysis` ⇒ early-warning sigue alertando por umbral simple; matar consumer mid-process ⇒ no duplica alertas al reprocesar.

---

## Hito 4 — Observabilidad (CLAUDE.md §5)

**Objetivo:** logs, métricas, health, tracing en el servicio portado (patrón replicable a los demás).

- [ ] **Logging JSON estructurado** — campos `timestamp`, `level`, `service`, `instance_id`, `message_id`/`trace_id`, `event`. Reemplaza `logger.info("eval sensor=%s...")` del baseline.
- [ ] **Métricas Prometheus** `/metrics` — `cpu_user_seconds_total`, `process_resident_memory_bytes`, throughput (msg/req por seg), latencia p50/p90/p95/p99, contador alerts publicadas/descartadas, estado del circuit breaker.
- [ ] **Health check real** `/health` — verifica conectividad RabbitMQ + MySQL (no estático como `app.py:211`).
- [ ] **Distributed tracing** (OpenTelemetry) — `message_id` como `trace_id` en el flujo ingest → consume → alerta.

**DoD:** dashboard scrape-able; un mensaje rastreable end-to-end por `trace_id`.

---

## Hito 5 — Servicios restantes (contract-first, Strangler Fig)

**Objetivo:** construir los otros 5 microservicios para completar la arquitectura. Cada uno reusa `pkg/` de Hitos 0/3/4 y respeta database-per-service.

> Orden por dependencia del flujo de datos: ingesta → persistencia → análisis → usuarios/proyectos.

- [ ] **`data-receiving`** (Productor) — recibe frames de sensores, dedup vía Redis (hash payload, TTL corto), genera `message_id` UUIDv4 post-dedup, valida contra JSON Schema, escribe Redis `sensor_buffer:{message_id}` antes de publicar, borra tras publisher confirm. Cola de errores local para inválidos. (CLAUDE.md §2.1)
- [ ] **`data-management`** (Consumidor) — persiste readings; Idempotent Consumer; su propio esquema MySQL.
- [ ] **`data-analysis`** (HTTP) — endpoint que enriquece reportes (consumido por early-warning con circuit breaker). Su propio esquema.
- [ ] **`user-management`** (HTTP) — `/api/v1/users/subscriptions` (tokens FCM, quiet hours, min_alert_level). Origen de los datos que `early-warning` consume hoy vía HTTP.
- [ ] **`project-management`** (HTTP) — proyectos + umbrales por proyecto (hoy hardcodeados en `config.py:74`). early-warning lee umbrales de aquí, no de config local.
- [ ] **Database-per-service** — cada servicio su esquema/usuario MySQL con permisos mínimos. Migrar el MySQL único compartido incrementalmente (CLAUDE.md §1.2). Migraciones `golang-migrate`.

**DoD:** flujo completo data-receiving → RabbitMQ → data-management/early-warning funciona; ningún servicio query-ea la tabla de otro.

---

## Hito 6 — Testing (pirámide obligatoria, CLAUDE.md §3)

**Objetivo:** las 4 capas para cada servicio. Esqueletos mínimos creados junto al código en hitos previos; aquí se completan.

- [ ] **Unit tests** — lógica de negocio aislada (threshold engine, quiet hours, level picking) con mocks/fakes. Sin RabbitMQ/MySQL/Consul.
- [ ] **Contract tests** — mensajes publicados validan contra JSON Schema vigente; examples siguen válidos (`contract-validate` Go).
- [ ] **CDCT (Pact / `pact-go`)** — productor↔consumidor compatibles sin levantar el sistema. HTTP pacts + message pacts AMQP. (CLAUDE.md §3.3, Schwarz et al. 2025)
- [ ] **Integration tests** — Testcontainers (RabbitMQ + MySQL) para flujos críticos: ingesta → persistencia → alerta.

**DoD:** CI corre las 4 capas; cobertura de los flujos críticos.

---

## Hito 7 — Benchmark y validación empírica (objetivo de la tesis)

**Objetivo:** medir Go vs. baseline Python bajo carga idéntica, validar hipótesis de mejora (El Fellah et al. 2026: throughput +134%, latencia −49%, memoria −7..−13%, CPU +1-2%).

- [ ] Harness de carga reproducible (mismo dataset de sensores a ambos sistemas).
- [ ] Captura de métricas: throughput, latencia (p50/p90/p95/p99), CPU, memoria residente, ambos sistemas.
- [ ] **Bloque 3** — disponibilidad >99% sin ML/data-analysis (circuit breaker degradado a umbral simple).
- [ ] Reporte comparativo con tablas/gráficas para la tesis.

**DoD:** dataset de resultados Go vs. Python publicable, con la disponibilidad bajo fallo medida.

---

## Resumen de dependencias

```
Hito 0 (fundación)
   └─> Hito 1 (contratos)  ← bloquea TODO handler
          └─> Hito 2 (port early-warning)
                 ├─> Hito 3 (resiliencia)
                 ├─> Hito 4 (observabilidad)
                 └─> Hito 5 (5 servicios restantes)  ← reusa pkg/ de 3 y 4
                        └─> Hito 6 (testing pirámide)
                               └─> Hito 7 (benchmark)
```

## Trazabilidad CLAUDE.md

| Principio CLAUDE.md | Hito |
|---|---|
| §1.1 Contract-first | 1 |
| §1.2 Database-per-service | 2, 5 |
| §1.3 Idempotencia | 3 |
| §1.4 Versionado contratos | 1 |
| §1.5 Resiliencia HTTP | 3 |
| §1.6 Health checks reales | 4 |
| §1.7 Config externalizada | 0, 2 |
| §1.8 Logging + correlation ID | 4 |
| §3 Testing | 6 |
| §4 Resiliencia | 3 |
| §5 Observabilidad | 4 |
