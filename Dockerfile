# Multi-stage Dockerfile for StreamForge
# Build stage for compiling the Rust application

# Stage 1: Builder
FROM rust:1.75-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/app

# Copy Cargo files first for better caching
COPY Cargo.toml Cargo.lock ./

# Create dummy main.rs to build dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy the actual source code
COPY src ./src
COPY migrations ./migrations

# Build the application
RUN touch src/main.rs && \
    cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash streamforge

# Create necessary directories
RUN mkdir -p /app/data/parquet && \
    chown -R streamforge:streamforge /app

WORKDIR /app

# Copy the binary from builder
COPY --from=builder --chown=streamforge:streamforge \
    /usr/src/app/target/release/streamforge /app/streamforge

# Copy migrations
COPY --chown=streamforge:streamforge migrations ./migrations

# Switch to non-root user
USER streamforge

# Set environment variables
ENV RUST_LOG=info \
    RUST_BACKTRACE=1 \
    HOST=0.0.0.0 \
    PORT=8080

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

# Run the application
ENTRYPOINT ["/app/streamforge"]
