.DEFAULT_GOAL := help
GO ?= go

.PHONY: help build test lint vet contract-validate proto proto-tools tidy ci

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

proto-tools: ## Instala protoc-gen-go[-grpc] en las versiones que espera el CI
	# Las versiones NO son libres: el job `contracts` regenera los stubs y falla
	# si difieren de los commiteados, y la cabecera generada incluye el numero de
	# version del plugin. Instalar con @latest hace que el diff aparezca solo
	# porque upstream publico algo nuevo.
	$(GO) install "google.golang.org/protobuf/cmd/protoc-gen-go@$$($(GO) list -m -f '{{.Version}}' google.golang.org/protobuf)"
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

proto: ## Genera stubs gRPC desde contracts/proto (requiere buf + `make proto-tools`)
	cd contracts/proto && buf lint && buf generate

tidy: ## go mod tidy
	$(GO) mod tidy

ci: vet build test contract-validate ## Pipeline local equivalente a CI
