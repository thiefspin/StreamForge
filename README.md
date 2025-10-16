# StreamForge

A high-performance, production-grade Rust service for real-time event processing with Kafka, PostgreSQL, and Parquet export capabilities.

## Overview

StreamForge is a real-time data pipeline that:
- Consumes events from Apache Kafka
- Validates and enriches event data
- Persists events to PostgreSQL with idempotent writes
- Exports data to Parquet files for analytics
- Provides comprehensive observability with metrics and health endpoints

Built with Rust for maximum performance, safety, and reliability.

## Features

- **Async Event Processing**: Built on Tokio for high-throughput async I/O
- **At-Least-Once Delivery**: Reliable message processing with manual offset management
- **Idempotent Storage**: Duplicate events are handled gracefully
- **Dead Letter Queue**: Failed messages are captured for analysis
- **Parquet Export**: Efficient columnar storage for analytics workloads
- **Production Observability**: Prometheus metrics, structured logging, health checks
- **Graceful Shutdown**: Clean shutdown with in-flight request completion
- **Configuration Management**: Environment-based configuration with validation

## Architecture

```
Kafka → StreamForge → PostgreSQL
           ↓
       Parquet Files
```

## Quick Start

### Prerequisites

- Rust 1.75+
- Docker and Docker Compose
- PostgreSQL 15+
- Apache Kafka 3.5+

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/streamforge.git
cd streamforge
```

2. Copy environment configuration:
```bash
cp .env.example .env
```

3. Start dependencies:
```bash
docker-compose up -d
```

4. Run database migrations:
```bash
cargo install sqlx-cli --no-default-features --features postgres
sqlx migrate run
```

5. Build and run the application:
```bash
cargo build --release
cargo run --release
```

### Development

Use the Makefile for common tasks:

```bash
make help          # Show all available commands
make setup         # Initial project setup
make docker-up     # Start Docker dependencies
make run           # Run the application
make test          # Run all tests
make lint          # Run linters (fmt + clippy)
```

For development with auto-reload:
```bash
cargo install cargo-watch
make watch
```

## Configuration

Configuration is managed through environment variables. See `.env.example` for all available options.

Key configurations:

| Variable | Description | Default |
|----------|-------------|---------|
| `HOST` | Server host | `0.0.0.0` |
| `PORT` | Server port | `8080` |
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `POSTGRES_URL` | PostgreSQL connection string | Required |
| `LOG_LEVEL` | Logging level | `info` |

## API Endpoints

### Health Checks

- `GET /healthz` - Liveness probe
- `GET /readyz` - Readiness probe (checks dependencies)
- `GET /metrics` - Prometheus metrics
- `GET /build` - Build information

## Data Schema

### Input Event (Kafka JSON)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "CLICK",
  "occurred_at": "2024-01-15T10:30:00Z",
  "user_id": "123e4567-e89b-12d3-a456-426614174000",
  "amount_cents": 1000,
  "path": "/products/123",
  "referrer": "google.com"
}
```

### Normalized Event (PostgreSQL)

The events are stored in the `events_normalized` table with additional metadata:
- `processed_at` - When the event was processed
- `src_partition` - Source Kafka partition
- `src_offset` - Source Kafka offset

## Testing

Run the test suite:

```bash
# All tests
cargo test

# Unit tests only
cargo test --lib

# Integration tests only
cargo test --test '*'

# With coverage
cargo tarpaulin --out Html
```

## Performance

StreamForge is designed for high throughput:
- Processes 10,000+ events/second
- p99 latency < 100ms
- Memory usage < 100MB idle, < 500MB under load

Run benchmarks:
```bash
cargo bench
```

## Monitoring

### Metrics

Prometheus metrics are exposed at `/metrics`:
- `stream_events_ingested_total` - Total events consumed from Kafka
- `stream_events_processed_total` - Events processed by status
- `stream_dlq_total` - Dead letter queue messages
- `stream_db_write_latency_seconds` - Database write latency
- `stream_parquet_flush_bytes_total` - Bytes written to Parquet

### Logging

Structured JSON logs are output to stdout. Configure with:
```bash
RUST_LOG=streamforge=debug,tower_http=debug
```

## Docker

Build the Docker image:
```bash
docker build -t streamforge:latest .
```

Run with Docker Compose:
```bash
docker-compose up
```

## Development Workflow

1. Create a feature branch
2. Make changes with tests
3. Run `make pre-commit` to validate
4. Submit a pull request

## Project Structure

```
streamforge/
├── src/
│   ├── main.rs              # Application entry point
│   ├── config.rs            # Configuration management
│   ├── error.rs             # Error types
│   ├── api/                 # HTTP API handlers
│   ├── kafka/               # Kafka consumer/producer
│   ├── db/                  # Database layer
│   ├── models/              # Data models
│   └── parquet/             # Parquet writer
├── migrations/              # SQL migrations
├── tests/                   # Integration tests
├── benches/                 # Performance benchmarks
└── spec/                    # Specifications and documentation
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass (`cargo test`)
- Code is formatted (`cargo fmt`)
- No clippy warnings (`cargo clippy`)
- New features include tests
- Documentation is updated

## License

MIT License - see [LICENSE](LICENSE) for details.

## Roadmap

- [ ] Phase 1: Foundation & Infrastructure ✅
- [ ] Phase 2: Data Models & Validation
- [ ] Phase 3: Database Layer
- [ ] Phase 4: Kafka Integration
- [ ] Phase 5: Observability
- [ ] Phase 6: Parquet Export
- [ ] Phase 7: Production Hardening

## Support

For issues and questions:
- Create a GitHub issue
- Check the [documentation](./spec/)
- Review the [development guide](./spec/development-phases.md)

---

Built with ❤️ in Rust