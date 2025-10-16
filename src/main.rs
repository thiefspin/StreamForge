//! StreamForge - A high-performance real-time data pipeline
//!
//! This application consumes events from Kafka, validates and enriches them,
//! stores them in PostgreSQL, and exports to Parquet files for analytics.

mod api;
mod config;
mod error;
mod logging;

use std::sync::Arc;

use crate::error::Result;

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

    // Start the HTTP server
    // TODO: In future phases, we'll also start:
    // - Kafka consumer
    // - Parquet writer
    // - Health monitor
    // - Metrics collector

    api::server::create_server(config).await?;

    tracing::info!("StreamForge shutdown complete");
    Ok(())
}
