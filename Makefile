.DEFAULT_GOAL := help
GO ?= go

.PHONY: help build test lint vet contract-validate proto tidy ci

help: ## Lista los targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  %-18s %s\n", $$1, $$2}'

build: ## Compila todos los paquetes
	$(GO) build ./...

test: ## Corre los tests
	$(GO) test ./...

vet: ## go vet
	$(GO) vet ./...

lint: ## golangci-lint (si está instalado)
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint no instalado, omitido"

contract-validate: ## Valida examples vs schemas (Hito 1)
	$(GO) run ./tools/contract-validate

proto: ## Genera stubs gRPC desde contracts/proto (requiere buf + protoc-gen-go[-grpc])
	cd contracts/proto && buf lint && buf generate

tidy: ## go mod tidy
	$(GO) mod tidy

ci: vet build test contract-validate ## Pipeline local equivalente a CI
