//! StreamForge - A high-performance real-time data pipeline
//!
//! This application consumes events from Kafka, validates and enriches them,
//! stores them in PostgreSQL, and exports to Parquet files for analytics.

mod api;
mod config;
mod db;
mod error;
mod kafka;
mod logging;
mod models;

use std::sync::Arc;
use tokio::signal;

use crate::error::Result;
use crate::kafka::EventConsumer;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration from environment
    let config = Arc::new(config::Config::from_env()?);

    // Validate configuration
    config.validate()?;

    // Initialize logging/tracing
    logging::init_tracing(&config.server.log_level, &config.server.environment)?;

    // Log configuration (with sensitive data masked)
    config.log_config();

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "Starting StreamForge");

    // Create database pool
    let db_pool = db::create_pool(&config.database).await?;

    // Run migrations
    tracing::info!("Running database migrations");
    db::run_migrations(&db_pool)
        .await
        .map_err(|e| crate::error::Error::database(format!("Migration failed: {}", e)))?;

    // Create event repository
    let event_repo = Arc::new(db::PgEventRepository::new(db_pool.clone()));

    // Create Kafka configuration
    let kafka_config = crate::kafka::KafkaConfig {
        brokers: config.kafka.brokers.clone(),
        consumer_group: config.kafka.group_id.clone(),
        events_topic: config.kafka.input_topic.clone(),
        dlq_topic: config.kafka.dlq_topic.clone(),
        auto_commit: config.kafka.enable_auto_commit,
        session_timeout_ms: config.kafka.session_timeout_ms,
        max_poll_interval_ms: config.kafka.max_poll_interval_ms,
        batch_size: config.processing.batch_size,
        max_retries: config.processing.max_retries,
        retry_backoff_ms: config.processing.retry_base_ms,
        ..Default::default()
    };

    // Create Kafka consumer
    let consumer = EventConsumer::new(kafka_config, event_repo)?;

    // Spawn consumer task
    let consumer_handle = tokio::spawn(async move {
        if let Err(e) = consumer.start().await {
            tracing::error!("Kafka consumer error: {}", e);
        }
    });

    // Start the HTTP server in a separate task
    let server_config = Arc::clone(&config);
    let server_handle = tokio::spawn(async move {
        if let Err(e) = api::server::create_server(server_config).await {
            tracing::error!("HTTP server error: {}", e);
        }
    });

    // Wait for shutdown signal
    shutdown_signal().await;

    tracing::info!("Received shutdown signal, starting graceful shutdown");

    // Cancel tasks
    consumer_handle.abort();
    server_handle.abort();

    // Wait for tasks to complete (with timeout)
    let shutdown_future = async {
        let _ = consumer_handle.await;
        let _ = server_handle.await;
    };

    let _ = tokio::time::timeout(config.server.shutdown_timeout(), shutdown_future).await;

    tracing::info!("StreamForge shutdown complete");
    Ok(())
}

/// Wait for shutdown signal (Ctrl+C or SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
