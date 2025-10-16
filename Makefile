# StreamForge Makefile
# Common development commands

.PHONY: help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: setup
setup: ## Initial project setup
	@echo "Setting up StreamForge development environment..."
	@cp .env.example .env 2>/dev/null || true
	@cargo build
	@echo "Setup complete! Run 'make run' to start the server."

.PHONY: build
build: ## Build the project in debug mode
	cargo build

.PHONY: build-release
build-release: ## Build the project in release mode
	cargo build --release

.PHONY: run
run: ## Run the application
	cargo run

.PHONY: run-release
run-release: ## Run the application in release mode
	cargo run --release

.PHONY: test
test: ## Run all tests
	cargo test

.PHONY: test-verbose
test-verbose: ## Run tests with verbose output
	cargo test -- --nocapture

.PHONY: test-integration
test-integration: ## Run integration tests only
	cargo test --test '*' -- --nocapture

.PHONY: test-unit
test-unit: ## Run unit tests only
	cargo test --lib

.PHONY: bench
bench: ## Run benchmarks
	cargo bench

.PHONY: check
check: ## Check code without building
	cargo check

.PHONY: clippy
clippy: ## Run clippy linter
	cargo clippy -- -D warnings

.PHONY: clippy-all
clippy-all: ## Run clippy with all targets and features
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: fmt
fmt: ## Format code
	cargo fmt

.PHONY: fmt-check
fmt-check: ## Check code formatting
	cargo fmt -- --check

.PHONY: lint
lint: fmt-check clippy ## Run all linters

.PHONY: clean
clean: ## Clean build artifacts
	cargo clean
	rm -rf data/parquet

.PHONY: docs
docs: ## Generate and open documentation
	cargo doc --no-deps --open

.PHONY: deps
deps: ## Show dependency tree
	cargo tree

.PHONY: outdated
outdated: ## Check for outdated dependencies
	cargo outdated

.PHONY: audit
audit: ## Security audit of dependencies
	cargo audit

.PHONY: coverage
coverage: ## Generate test coverage report
	cargo tarpaulin --out Html --output-dir coverage

.PHONY: docker-up
docker-up: ## Start Docker dependencies
	docker-compose up -d

.PHONY: docker-down
docker-down: ## Stop Docker dependencies
	docker-compose down

.PHONY: docker-logs
docker-logs: ## Show Docker logs
	docker-compose logs -f

.PHONY: docker-clean
docker-clean: ## Clean Docker volumes
	docker-compose down -v

.PHONY: kafka-topics
kafka-topics: ## Create Kafka topics
	docker exec -it streamforge-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic events --partitions 3 --replication-factor 1 --if-not-exists
	docker exec -it streamforge-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic events-dlq --partitions 1 --replication-factor 1 --if-not-exists

.PHONY: kafka-produce
kafka-produce: ## Produce test message to Kafka
	@echo '{"event_id":"550e8400-e29b-41d4-a716-446655440000","event_type":"CLICK","occurred_at":"2024-01-15T10:30:00Z","user_id":"123e4567-e89b-12d3-a456-426614174000","path":"/home","referrer":"google.com"}' | \
	docker exec -i streamforge-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic events

.PHONY: kafka-consume
kafka-consume: ## Consume messages from Kafka input topic
	docker exec -it streamforge-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic events --from-beginning

.PHONY: kafka-consume-dlq
kafka-consume-dlq: ## Consume messages from DLQ
	docker exec -it streamforge-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic events-dlq --from-beginning

.PHONY: kafka-test
kafka-test: ## Run Kafka integration tests
	cargo test kafka --lib

.PHONY: kafka-test-integration
kafka-test-integration: ## Run full Kafka integration tests (requires Docker)
	docker-compose -f docker-compose.test.yml up -d
	sleep 10  # Wait for services to be ready
	./scripts/send_test_events.sh 10 events --with-invalid
	cargo test --test kafka_integration_test
	docker-compose -f docker-compose.test.yml down

.PHONY: kafka-send-events
kafka-send-events: ## Send test events to Kafka
	./scripts/send_test_events.sh 100 events

.PHONY: kafka-monitor
kafka-monitor: ## Monitor Kafka consumer lag
	docker exec -it streamforge-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group streamforge-consumer

.PHONY: db-migrate
db-migrate: ## Run database migrations
	sqlx migrate run

.PHONY: db-reset
db-reset: ## Reset database
	sqlx database reset

.PHONY: watch
watch: ## Run with auto-reload on changes
	cargo watch -x run

.PHONY: watch-test
watch-test: ## Run tests with auto-reload
	cargo watch -x test

.PHONY: pre-commit
pre-commit: lint test ## Run pre-commit checks
	@echo "All pre-commit checks passed!"

.PHONY: ci
ci: lint test build ## Run CI pipeline locally
	@echo "CI pipeline passed!"

.PHONY: install-tools
install-tools: ## Install development tools
	cargo install cargo-watch
	cargo install cargo-outdated
	cargo install cargo-audit
	cargo install cargo-tarpaulin
	cargo install sqlx-cli --no-default-features --features postgres
	cargo install cargo-nextest

.PHONY: flamegraph
flamegraph: ## Generate flamegraph for performance profiling
	cargo flamegraph

.PHONY: todo
todo: ## Show all TODO comments in code
	@grep -r "TODO\|FIXME\|XXX" --include="*.rs" src/ || true

.PHONY: stats
stats: ## Show code statistics
	@echo "Lines of Rust code:"
	@find src -name "*.rs" | xargs wc -l | tail -1
	@echo ""
	@echo "Number of dependencies:"
	@cargo tree --depth 1 | wc -l
	@echo ""
	@echo "Kafka module size:"
	@find src/kafka -name "*.rs" | xargs wc -l | tail -1

.PHONY: phase4-demo
phase4-demo: ## Demo Phase 4 Kafka integration
	@echo "Starting Phase 4 Kafka Integration Demo..."
	@echo "1. Starting Docker services..."
	@docker-compose up -d
	@sleep 5
	@echo "2. Creating Kafka topics..."
	@make kafka-topics
	@echo "3. Running database migrations..."
	@make db-migrate || true
	@echo "4. Sending test events..."
	@./scripts/send_test_events.sh 10 events
	@echo "5. Starting StreamForge consumer..."
	@echo "Check logs with 'docker-compose logs -f' in another terminal"
	@echo "Press Ctrl+C to stop the demo"
	@cargo run

# Default target
.DEFAULT_GOAL := help
