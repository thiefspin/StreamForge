//! HTTP server implementation for StreamForge
//!
//! This module sets up the Axum web server with all routes, middleware,
//! and graceful shutdown handling.

use axum::{
    extract::MatchedPath,
    http::{header, Method, Request},
    routing::get,
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::HeaderName;
use tokio::net::TcpListener;
use tower_http::{
    cors::{Any, CorsLayer},
    request_id::{MakeRequestId, RequestId, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use uuid::Uuid;

use crate::{
    api::health::{build_info, health_check, ready_check, HealthState},
    config::Config,
    error::Result,
};

/// Request ID generator
#[derive(Clone, Default)]
struct MakeRequestUuid;

impl MakeRequestId for MakeRequestUuid {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let id = Uuid::new_v4().to_string();
        Some(RequestId::new(id.parse().ok()?))
    }
}

/// Create the main application router
pub fn create_router(config: Arc<Config>) -> Router {
    let health_state = Arc::new(HealthState::new());

    // Create API routes
    let api_routes = Router::new()
        .route("/healthz", get(health_check))
        .route("/readyz", get(ready_check))
        .route("/build", get(build_info))
        .with_state(health_state.clone());

    // Create metrics route if enabled
    let metrics_routes = if config.features.metrics {
        Router::new().route("/metrics", get(metrics_handler))
    } else {
        Router::new()
    };

    // Combine all routes
    let app = Router::new().merge(api_routes).merge(metrics_routes);

    // Apply middleware
    app.layer(TimeoutLayer::new(config.server.request_timeout()))
        .layer(SetRequestIdLayer::new(
            HeaderName::from_static("x-request-id"),
            MakeRequestUuid::default(),
        ))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    let matched_path =
                        request.extensions().get::<MatchedPath>().map(MatchedPath::as_str);
                    let request_id = request
                        .headers()
                        .get("x-request-id")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("unknown");

                    tracing::info_span!(
                        "http_request",
                        method = ?request.method(),
                        matched_path,
                        request_id,
                        latency = tracing::field::Empty,
                        status = tracing::field::Empty,
                    )
                })
                .on_request(DefaultOnRequest::new().level(tracing::Level::INFO))
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::INFO)
                        .latency_unit(LatencyUnit::Millis),
                ),
        )
}

/// Metrics endpoint handler
async fn metrics_handler() -> Result<String> {
    // TODO: Implement actual metrics collection
    Ok(
        "# HELP stream_events_ingested_total Total events ingested from Kafka\n\
        # TYPE stream_events_ingested_total counter\n\
        stream_events_ingested_total{topic=\"events.input.v1\"} 0\n\
        \n\
        # HELP stream_events_processed_total Events processed by status\n\
        # TYPE stream_events_processed_total counter\n\
        stream_events_processed_total{status=\"success\"} 0\n\
        stream_events_processed_total{status=\"error\"} 0\n"
            .to_string(),
    )
}

/// Create and start the HTTP server
pub async fn create_server(config: Arc<Config>) -> Result<()> {
    let app = create_router(config.clone());
    let addr: SocketAddr = config
        .server
        .address()
        .parse()
        .map_err(|e| crate::error::Error::config(format!("Invalid server address: {}", e)))?;

    tracing::info!(
        address = %addr,
        environment = %config.server.environment,
        "Starting HTTP server"
    );

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| crate::error::Error::internal(format!("Failed to bind to {}: {}", addr, e)))?;

    tracing::info!(
        address = %addr,
        "HTTP server listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| crate::error::Error::internal(format!("Server error: {}", e)))
}

/// Shutdown signal handler
///
/// Waits for CTRL+C or SIGTERM signals to gracefully shutdown the server.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received CTRL+C, starting graceful shutdown");
        },
        _ = terminate => {
            tracing::info!("Received SIGTERM, starting graceful shutdown");
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let config = Arc::new(Config {
            server: crate::config::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                log_level: "info".to_string(),
                environment: "test".to_string(),
                request_timeout_secs: 30,
                shutdown_timeout_secs: 30,
            },
            kafka: crate::config::KafkaConfig {
                brokers: "localhost:9092".to_string(),
                group_id: "test".to_string(),
                input_topic: "test".to_string(),
                dlq_topic: "test-dlq".to_string(),
                auto_offset_reset: "earliest".to_string(),
                session_timeout_ms: 30000,
                enable_auto_commit: false,
                max_poll_interval_ms: 300000,
            },
            database: crate::config::DatabaseConfig {
                url: "postgresql://test@localhost/test".to_string(),
                pool_max_size: 10,
                pool_min_idle: 1,
                pool_timeout_seconds: 30,
                pool_idle_timeout_seconds: 600,
            },
            parquet: crate::config::ParquetConfig {
                root: "/tmp".to_string(),
                flush_max_rows: 1000,
                flush_interval_secs: 60,
                compression: "snappy".to_string(),
                buffer_size: 100,
            },
            processing: crate::config::ProcessingConfig {
                worker_concurrency: 4,
                max_retries: 3,
                retry_base_ms: 100,
                retry_max_ms: 10000,
                batch_size: 100,
            },
            metrics: crate::config::MetricsConfig {
                enabled: true,
                port: 9090,
                path: "/metrics".to_string(),
            },
            features: crate::config::FeatureFlags {
                parquet_export: true,
                metrics: true,
                tracing: false,
                tls: false,
            },
        });

        let app = create_router(config);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/healthz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_build_endpoint() {
        let config = Arc::new(Config {
            server: crate::config::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                log_level: "info".to_string(),
                environment: "test".to_string(),
                request_timeout_secs: 30,
                shutdown_timeout_secs: 30,
            },
            kafka: crate::config::KafkaConfig {
                brokers: "localhost:9092".to_string(),
                group_id: "test".to_string(),
                input_topic: "test".to_string(),
                dlq_topic: "test-dlq".to_string(),
                auto_offset_reset: "earliest".to_string(),
                session_timeout_ms: 30000,
                enable_auto_commit: false,
                max_poll_interval_ms: 300000,
            },
            database: crate::config::DatabaseConfig {
                url: "postgresql://test@localhost/test".to_string(),
                pool_max_size: 10,
                pool_min_idle: 1,
                pool_timeout_seconds: 30,
                pool_idle_timeout_seconds: 600,
            },
            parquet: crate::config::ParquetConfig {
                root: "/tmp".to_string(),
                flush_max_rows: 1000,
                flush_interval_secs: 60,
                compression: "snappy".to_string(),
                buffer_size: 100,
            },
            processing: crate::config::ProcessingConfig {
                worker_concurrency: 4,
                max_retries: 3,
                retry_base_ms: 100,
                retry_max_ms: 10000,
                batch_size: 100,
            },
            metrics: crate::config::MetricsConfig {
                enabled: true,
                port: 9090,
                path: "/metrics".to_string(),
            },
            features: crate::config::FeatureFlags {
                parquet_export: true,
                metrics: true,
                tracing: false,
                tls: false,
            },
        });

        let app = create_router(config);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/build")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
