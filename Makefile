.DEFAULT_GOAL := help
BIN := bin/ratelimitd
export RL_REDIS_URL ?= redis://localhost:6379/0

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the ratelimitd binary
	go build -trimpath -o $(BIN) ./cmd/ratelimitd

.PHONY: run
run: ## Run ratelimitd locally (needs `make up`)
	go run ./cmd/ratelimitd

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-redis
test-redis: ## Run all tests, including Redis ones (needs `make up`)
	RL_TEST_REDIS_URL=redis://localhost:6379/15 go test -race -count=1 ./...

.PHONY: fuzz
fuzz: ## Fuzz the token bucket for 60s
	go test -run '^$$' -fuzz FuzzTokenBucket -fuzztime 60s ./internal/algo

.PHONY: bench
bench: ## Run benchmarks
	go test -run '^$$' -bench . -benchmem ./internal/...

.PHONY: cover
cover: ## Run tests and print total coverage
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: fmt
fmt: ## Format code
	gofmt -s -w .

.PHONY: up
up: ## Start local dependencies (Redis)
	docker compose up -d

.PHONY: stack
stack: ## Run ratelimitd and Redis in Docker
	docker compose --profile stack up --build

.PHONY: down
down: ## Stop everything
	docker compose --profile stack down
