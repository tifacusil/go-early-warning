# syntax=docker/dockerfile:1

# ── build ──────────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS build
WORKDIR /src

# Capa de dependencias cacheable.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Binario estatico (CGO off) para el servicio early-warning.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/early-warning ./cmd/early-warning

# ── runtime ────────────────────────────────────────────────────────────────
# Alpine (no distroless) para tener wget: el healthcheck de infra usa
# wget -qO- http://localhost:8080/health.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 app
USER app
WORKDIR /app
COPY --from=build /out/early-warning /app/early-warning

EXPOSE 8080
ENTRYPOINT ["/app/early-warning"]
