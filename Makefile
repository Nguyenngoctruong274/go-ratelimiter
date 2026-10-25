.DEFAULT_GOAL := help
BIN := bin/ratelimitd
export RL_REDIS_URL ?= redis://localhost:6379/0
export RL_POLICY_FILE ?= deploy/policies.json

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
fuzz: ## Fuzz each algorithm for 60s
	for f in FuzzTokenBucket FuzzGCRA FuzzSlidingWindow; do \
		go test -run '^$$' -fuzz "^$$f$$" -fuzztime 60s ./internal/algo || exit 1; \
	done

.PHONY: compare
compare: ## Print the accuracy comparison of the algorithms
	go test -count=1 -run TestAccuracyComparison -v ./internal/algo

.PHONY: bench
bench: ## Run benchmarks
	go test -run '^$$' -bench . -benchmem ./internal/...

.PHONY: bench-redis
bench-redis: ## Benchmark checks/s and p99 on Redis, direct vs pipelined (needs `make up`)
	RL_TEST_REDIS_URL=redis://localhost:6379/15 go test -run '^$$' -bench 'Pipeline|ServiceCheck' -benchtime 3s ./internal/redisstore ./cmd/ratelimitd

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
stack: ## Run two ratelimitd instances, Redis and Prometheus in Docker
	docker compose --profile stack up --build

.PHONY: load
load: ## Send 100 login checks to both stack instances; together they allow 5
	go run ./cmd/rlload -targets http://localhost:8080,http://localhost:8081 -policy login -key demo-$$(date +%s) -requests 100 -want-allowed 5

.PHONY: down
down: ## Stop everything
	docker compose --profile stack down
