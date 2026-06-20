# CLAUDE.md — Guía de ingeniería para la reescritura del sistema de monitoreo de deslizamientos

> Este documento define las prácticas de software engineering y de diseño de microservicios que Claude debe aplicar **siempre**, sin excepción, al generar, revisar o modificar código durante la reescritura del sistema (basado en Bai et al. 2020) en Go. Si una sugerencia de código no cumple con estas prácticas, Claude debe señalarlo explícitamente antes de continuar.

---

## 0. Contexto del proyecto

- Sistema de monitoreo y alerta temprana de deslizamientos de tierra, originalmente descrito en Bai et al. (2020), reescrito en **Go**.
- Arquitectura de 6 microservicios: `data-receiving`, `data-management`, `data-analysis`, `early-warning`, `user-management`, `project-management`.
- Comunicación asíncrona vía **RabbitMQ** (pub/sub, at-least-once delivery), comunicación síncrona vía **HTTP/REST**, y registro/descubrimiento vía **Consul**.
- Contratos de datos definidos en `contracts/schemas/*.schema.json` (JSON Schema Draft 2020-12), con `schema_version` semántico en el envelope.
- El objetivo de la reescritura es validar empíricamente que la aplicación de buenas prácticas SE mejora rendimiento (throughput, latencia, CPU, memoria) frente al sistema baseline, siguiendo el marco de El Fellah et al. (2026).

---

## 1. Principios no negociables

Estos principios aplican a **todo** código generado, sin importar el microservicio:

1. **Contract-first siempre.** Ningún servicio publica o consume un mensaje de RabbitMQ, ni expone/consume un endpoint HTTP, sin que exista primero un JSON Schema (para mensajes) o una definición OpenAPI (para HTTP) en `contracts/`. Si el contrato no existe, Claude debe crearlo o pedirlo antes de escribir el handler.
2. **Database-per-service.** Cada microservicio accede únicamente a su propio esquema/base de datos. Si una tarea requiere que un servicio lea datos que "pertenecen" a otro, la solución correcta es una llamada HTTP o un evento, **no** una query directa a la tabla de otro servicio. Señalar explícitamente si el sistema actual (MySQL único compartido) viola esto, y proponer la migración incremental (Strangler Fig) en lugar de ignorarlo.
3. **Idempotencia en todos los consumidores.** Cualquier consumidor de RabbitMQ (Data Management, Early Warning, y Data Receiving si actúa como productor con reintentos) debe deduplicar por `message_id` o por hash del payload antes de procesar. Nunca asumir entrega exactly-once.
4. **Versionado semántico de contratos.** Cambios de campo siguen la regla ya definida en `tarea-0-contratos.md`: aditivo y opcional → minor; incompatible → major con período de gracia de aceptación de la versión anterior.
5. **Resiliencia explícita en llamadas síncronas.** Toda llamada HTTP entre servicios (ej. Early Warning → Data Analysis) debe implementar:
   - **Circuit breaker** (abrir el circuito tras N fallos consecutivos, half-open para probar recuperación).
   - **Retry con backoff exponencial** (nunca retry inmediato ni infinito).
   - **Timeout explícito** (nunca usar el default del cliente HTTP sin configurar).
6. **Health checks reales.** `/health` debe verificar conectividad real a RabbitMQ y a la base de datos del servicio, no solo responder `200 OK` estático.
7. **Configuración externalizada.** Nunca hardcodear hosts, puertos, credenciales o nombres de colas/exchanges. Todo via variables de entorno o Consul KV, siguiendo la convención de `tarea-0-contratos.md`.
8. **Logging estructurado con correlation ID.** Todo log debe ser JSON estructurado e incluir el `message_id` (o un `trace_id` derivado) para poder seguir un mensaje a través de Data Receiving → RabbitMQ → consumidores.

---

## 2. Patrones obligatorios por tipo de componente

### 2.1 Productores (Data Receiving)

- Generar `message_id` (UUID v4) **después** de deduplicar el frame crudo vía Redis (hash del payload con TTL corto), para evitar publicar duplicados cuando el sensor reintenta por timeout.
- Escribir a Redis (`sensor_buffer:{message_id}`) **antes** de publicar a RabbitMQ; borrar la entrada solo tras confirmar el `publisher confirm` del broker.
- Validar el mensaje contra el JSON Schema correspondiente **antes** de publicar. Un mensaje que no valida va a una cola de errores local, nunca a `sensors.raw`.
- Exponer métricas Prometheus: `cpu_user_seconds_total`, `process_resident_memory_bytes`, contador de mensajes publicados/descartados.

### 2.2 Consumidores (Data Management, Early Warning)

- Implementar **Idempotent Consumer**: `INSERT IGNORE` / `ON CONFLICT DO NOTHING` keyed por `message_id`, o verificación previa de existencia.
- Manejar fallos de procesamiento con retry acotado (máximo 3 intentos con backoff) antes de enviar a `sensors.dead_letter` vía DLX.
- Confirmar (`ack`) solo después de persistir exitosamente, nunca antes.

### 2.3 Llamadas síncronas entre servicios (Early Warning → Data Analysis)

- Circuit breaker con métricas expuestas (estado open/closed/half-open visible en `/health` o `/metrics`).
- Si el circuito está abierto, Early Warning debe seguir emitiendo alarmas basadas en umbral simple (sin ML/análisis avanzado) — esto es un requisito de **continuidad operativa**, no opcional. Ver Bloque 3 del plan de pruebas (disponibilidad >99% sin ML Service).

### 2.4 Esquema de base de datos

- Cada servicio define su propio esquema/usuario MySQL con permisos mínimos (ya documentado en `tarea-0-contratos.md`, sección B3).
- Migraciones versionadas (Flyway/Liquibase o equivalente Go como `golang-migrate`), nunca cambios manuales a esquema en producción.
- Particionamiento por año en tablas de alto volumen (GNSS, deep displacement), siguiendo B2.

---

## 3. Testing — pirámide obligatoria

Para cada microservicio nuevo o reescrito, Claude debe generar (o al menos dejar el esqueleto de):

1. **Unit tests** — lógica de negocio aislada, sin RabbitMQ/MySQL/Consul reales (mocks/fakes).
2. **Contract tests** — validar que los mensajes publicados cumplen el JSON Schema vigente, y que los mensajes de ejemplo en `contracts/examples/` siguen siendo válidos (`validate.py` o equivalente en Go).
3. **Consumer-driven contract tests (CDCT)** — usando Pact (o `pact-go`) para verificar que productor y consumidor de cada cola/endpoint son compatibles, sin levantar todo el sistema. Aplica tanto a interacciones HTTP como a mensajes AMQP (message pacts).
4. **Integration tests mínimos** — con contenedores efímeros (Testcontainers o `docker-compose` de test) para RabbitMQ + MySQL, solo para los flujos críticos (ingesta → persistencia → alerta).

Si Claude detecta que una de estas capas falta para un componente que está modificando, debe mencionarlo aunque no esté en el alcance inmediato de la tarea.

---

## 4. Resiliencia — patrones esperados

| Patrón | Dónde aplica | Comportamiento esperado |
|---|---|---|
| Circuit Breaker | Early Warning → Data Analysis (HTTP) | Abre tras N fallos; degradación a umbral simple mientras está abierto |
| Retry + backoff exponencial | Consumidores RabbitMQ, llamadas HTTP salientes | Máx 3 intentos, backoff 1s/5s/30s, luego DLQ |
| Dead Letter Queue | Todos los consumidores | `sensors.dlx` → `sensors.dead_letter`, con alerta a monitoring |
| Publisher Confirms | Data Receiving | Confirmar antes de borrar buffer de Redis |
| Idempotent Consumer | Data Management, Early Warning | Deduplicación por `message_id` |
| Health checks activos | Todos los servicios | `/health` verifica BD + RabbitMQ, registrado en Consul cada 10s |

No se requiere implementar **Bulkhead** de forma aislada salvo que se identifique un punto de contención de recursos compartidos entre handlers dentro del mismo proceso (ej. pool de conexiones DB saturado por un endpoint lento bloqueando otros). Si Claude lo identifica, debe señalarlo y proponerlo, pero no es un requisito por defecto dado que carece de sustento empírico aislado en la literatura revisada para este proyecto.

---

## 5. Observabilidad

- **Logs estructurados (JSON)** con campos mínimos: `timestamp`, `level`, `service`, `instance_id`, `message_id`/`trace_id`, `event`.
- **Métricas Prometheus** expuestas en `/metrics`: CPU, memoria residente, throughput (mensajes/requests por segundo), latencia por percentil (p50/p90/p95/p99).
- **Distributed tracing** (OpenTelemetry) recomendado para el flujo Data Receiving → RabbitMQ → Data Management/Early Warning, usando `message_id` como `trace_id` o correlándolo.

---

## 6. Al generar código nuevo, Claude debe siempre

1. Verificar si existe un contrato (`contracts/schemas/*.schema.json` u OpenAPI) para el mensaje/endpoint en cuestión. Si no existe, proponer su definición primero.
2. Aplicar idempotencia en cualquier punto que consuma de RabbitMQ.
3. Aplicar circuit breaker + retry + timeout en cualquier llamada HTTP saliente entre servicios.
4. Externalizar toda configuración (variables de entorno / Consul KV).
5. Incluir health check real y registro en Consul siguiendo la convención de `tarea-0-contratos.md` (nombres `<service-name>-<n>`, tags `version-`, `lang-`, `env-`).
6. Generar al menos el esqueleto de unit tests y contract tests para el código nuevo.
7. Mantener cada servicio accediendo solo a su propio esquema de BD (database-per-service), incluso si la migración completa desde MySQL único es incremental.
8. Usar logging estructurado con `message_id`/`trace_id`.

---

## 7. Cuando una práctica no se pueda aplicar

Si una restricción del proyecto (tiempo, alcance de wave, decisión previa documentada) impide aplicar alguna de estas prácticas, Claude debe:

- Implementar la versión más simple que no contradiga el principio (ej. si no hay tiempo para Pact, al menos generar el contract test contra el JSON Schema).
- Dejar un comentario `// TODO(SE-practice):` explicando qué falta y por qué, para que quede trazable en el código.
- Nunca omitir silenciosamente una práctica de la sección 1 (no negociables) sin advertirlo explícitamente en la respuesta.

---

## 8. Referencias de sustento (para justificar estas reglas si se pide)

- Bai et al. (2020) — arquitectura base del sistema. DOI: 10.1080/19475705.2020.1766580
- El Fellah et al. (2026) — catálogo de prácticas SE y evidencia empírica de mejora (throughput +134%, latencia −49%, memoria −7 a −13%, CPU +1-2%) en sistemas IoT con microservicios. DOI: 10.1016/j.iot.2025.101867
- Schwarz, Quast, Riehle (2025) — CDCT sobre HTTP + AMQP/RabbitMQ, 77% de defectos detectados. DOI: 10.1002/stvr.70006
- Saleh Sedghpour et al. (2022) — circuit breaker y retry en service mesh, 110M requests. DOI: 10.1145/3489525.3511686
- Aderaldo et al. (2024) — benchmark de patrones de resiliencia (retry, circuit breaker). DOI: 10.1002/spe.3368
- Taibi & Lenarduzzi (2018) — Shared Persistency como bad smell empíricamente validado. DOI: 10.1109/MS.2018.2141031
