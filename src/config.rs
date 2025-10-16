//! Configuration module for StreamForge
//!
//! This module handles loading and validating configuration from environment
//! variables, providing strongly-typed configuration structures for all
//! application components.

use envconfig::Envconfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::{Error, Result};

/// Main configuration structure for StreamForge
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct Config {
    /// Server configuration
    #[serde(flatten)]
    #[envconfig(nested)]
    pub server: ServerConfig,

    /// Kafka configuration
    #[serde(flatten)]
    #[envconfig(nested)]
    pub kafka: KafkaConfig,

    /// Database configuration
    #[serde(flatten)]
    #[envconfig(nested)]
    pub database: DatabaseConfig,

    /// Parquet configuration
    #[serde(flatten)]
    #[envconfig(nested)]
    pub parquet: ParquetConfig,

    /// Processing configuration
    #[serde(flatten)]
    #[envconfig(nested)]
    pub processing: ProcessingConfig,

    /// Metrics configuration
    #[serde(flatten)]
    #[envconfig(nested)]
    pub metrics: MetricsConfig,

    /// Feature flags
    #[serde(flatten)]
    #[envconfig(nested)]
    pub features: FeatureFlags,
}

/// Server configuration
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct ServerConfig {
    /// Host to bind to
    #[envconfig(from = "HOST", default = "0.0.0.0")]
    pub host: String,

    /// Port to listen on
    #[envconfig(from = "PORT", default = "8080")]
    pub port: u16,

    /// Log level
    #[envconfig(from = "LOG_LEVEL", default = "info")]
    pub log_level: String,

    /// Environment (development, staging, production)
    #[envconfig(from = "ENVIRONMENT", default = "development")]
    pub environment: String,

    /// Request timeout in seconds
    #[envconfig(from = "REQUEST_TIMEOUT_SECS", default = "30")]
    pub request_timeout_secs: u64,

    /// Shutdown timeout in seconds
    #[envconfig(from = "SHUTDOWN_TIMEOUT_SECS", default = "30")]
    pub shutdown_timeout_secs: u64,
}

impl ServerConfig {
    /// Get the server address as a string
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Get request timeout as Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    /// Get shutdown timeout as Duration
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_secs)
    }

    /// Check if running in development mode
    pub fn is_development(&self) -> bool {
        self.environment == "development"
    }

    /// Check if running in production mode
    pub fn is_production(&self) -> bool {
        self.environment == "production"
    }
}

/// Kafka configuration
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct KafkaConfig {
    /// Kafka brokers (comma-separated)
    #[envconfig(from = "KAFKA_BROKERS", default = "localhost:9092")]
    pub brokers: String,

    /// Consumer group ID
    #[envconfig(from = "KAFKA_GROUP_ID", default = "streamforge-consumer")]
    pub group_id: String,

    /// Input topic
    #[envconfig(from = "KAFKA_INPUT_TOPIC", default = "events.input.v1")]
    pub input_topic: String,

    /// Dead letter queue topic
    #[envconfig(from = "KAFKA_DLQ_TOPIC", default = "events.dlq.v1")]
    pub dlq_topic: String,

    /// Auto offset reset (earliest, latest)
    #[envconfig(from = "KAFKA_AUTO_OFFSET_RESET", default = "earliest")]
    pub auto_offset_reset: String,

    /// Session timeout in milliseconds
    #[envconfig(from = "KAFKA_SESSION_TIMEOUT_MS", default = "30000")]
    pub session_timeout_ms: u32,

    /// Enable auto commit
    #[envconfig(from = "KAFKA_ENABLE_AUTO_COMMIT", default = "false")]
    pub enable_auto_commit: bool,

    /// Max poll interval in milliseconds
    #[envconfig(from = "KAFKA_MAX_POLL_INTERVAL_MS", default = "300000")]
    pub max_poll_interval_ms: u32,
}

impl KafkaConfig {
    /// Get brokers as a vector
    pub fn brokers_list(&self) -> Vec<String> {
        self.brokers.split(',').map(|s| s.trim().to_string()).collect()
    }
}

/// Database configuration
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL
    #[envconfig(from = "POSTGRES_URL")]
    pub url: String,

    /// Maximum pool size
    #[envconfig(from = "DATABASE_POOL_MAX_SIZE", default = "20")]
    pub pool_max_size: u32,

    /// Minimum idle connections
    #[envconfig(from = "DATABASE_POOL_MIN_IDLE", default = "5")]
    pub pool_min_idle: u32,

    /// Pool timeout in seconds
    #[envconfig(from = "DATABASE_POOL_TIMEOUT_SECONDS", default = "30")]
    pub pool_timeout_seconds: u64,

    /// Idle timeout in seconds
    #[envconfig(from = "DATABASE_POOL_IDLE_TIMEOUT_SECONDS", default = "600")]
    pub pool_idle_timeout_seconds: u64,
}

impl DatabaseConfig {
    /// Get pool timeout as Duration
    pub fn pool_timeout(&self) -> Duration {
        Duration::from_secs(self.pool_timeout_seconds)
    }

    /// Get idle timeout as Duration
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.pool_idle_timeout_seconds)
    }

    /// Mask password in URL for logging
    pub fn masked_url(&self) -> String {
        if let Some(at_pos) = self.url.find('@') {
            if let Some(scheme_end) = self.url.find("://") {
                let start = &self.url[..scheme_end + 3];
                let end = &self.url[at_pos..];
                return format!("{}***{}", start, end);
            }
        }
        "***".to_string()
    }
}

/// Parquet configuration
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct ParquetConfig {
    /// Root directory for Parquet files
    #[envconfig(from = "PARQUET_ROOT", default = "./data/parquet/events")]
    pub root: String,

    /// Maximum rows before flush
    #[envconfig(from = "PARQUET_FLUSH_MAX_ROWS", default = "10000")]
    pub flush_max_rows: usize,

    /// Flush interval in seconds
    #[envconfig(from = "PARQUET_FLUSH_INTERVAL_SECS", default = "60")]
    pub flush_interval_secs: u64,

    /// Compression type (snappy, lz4, zstd, none)
    #[envconfig(from = "PARQUET_COMPRESSION", default = "snappy")]
    pub compression: String,

    /// Buffer size for batching
    #[envconfig(from = "PARQUET_BUFFER_SIZE", default = "1000")]
    pub buffer_size: usize,
}

impl ParquetConfig {
    /// Get flush interval as Duration
    pub fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.flush_interval_secs)
    }
}

/// Processing configuration
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct ProcessingConfig {
    /// Number of concurrent workers
    #[envconfig(from = "WORKER_CONCURRENCY", default = "10")]
    pub worker_concurrency: usize,

    /// Maximum retry attempts
    #[envconfig(from = "MAX_RETRIES", default = "3")]
    pub max_retries: u32,

    /// Base retry delay in milliseconds
    #[envconfig(from = "RETRY_BASE_MS", default = "100")]
    pub retry_base_ms: u64,

    /// Maximum retry delay in milliseconds
    #[envconfig(from = "RETRY_MAX_MS", default = "10000")]
    pub retry_max_ms: u64,

    /// Batch size for processing
    #[envconfig(from = "BATCH_SIZE", default = "100")]
    pub batch_size: usize,
}

impl ProcessingConfig {
    /// Get base retry delay as Duration
    pub fn retry_base_delay(&self) -> Duration {
        Duration::from_millis(self.retry_base_ms)
    }

    /// Get max retry delay as Duration
    pub fn retry_max_delay(&self) -> Duration {
        Duration::from_millis(self.retry_max_ms)
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct MetricsConfig {
    /// Enable metrics collection
    #[envconfig(from = "METRICS_ENABLED", default = "true")]
    pub enabled: bool,

    /// Metrics port
    #[envconfig(from = "METRICS_PORT", default = "9090")]
    pub port: u16,

    /// Metrics path
    #[envconfig(from = "METRICS_PATH", default = "/metrics")]
    pub path: String,
}

/// Feature flags
#[derive(Debug, Clone, Deserialize, Serialize, Envconfig)]
pub struct FeatureFlags {
    /// Enable Parquet export
    #[envconfig(from = "ENABLE_PARQUET_EXPORT", default = "true")]
    pub parquet_export: bool,

    /// Enable metrics collection
    #[envconfig(from = "ENABLE_METRICS", default = "true")]
    pub metrics: bool,

    /// Enable distributed tracing
    #[envconfig(from = "ENABLE_TRACING", default = "false")]
    pub tracing: bool,

    /// Enable TLS
    #[envconfig(from = "TLS_ENABLED", default = "false")]
    pub tls: bool,
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        // Load .env file if it exists (for local development)
        dotenv::dotenv().ok();

        // Parse configuration from environment
        Config::init_from_env().map_err(Error::from)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Validate server config
        if self.server.port == 0 {
            return Err(Error::config("Server port cannot be 0"));
        }

        // Validate Kafka config
        if self.kafka.brokers.is_empty() {
            return Err(Error::config("Kafka brokers cannot be empty"));
        }

        // Validate database config
        if self.database.url.is_empty() {
            return Err(Error::config("Database URL cannot be empty"));
        }

        // Validate worker concurrency
        if self.processing.worker_concurrency == 0 {
            return Err(Error::config("Worker concurrency must be at least 1"));
        }

        Ok(())
    }

    /// Log configuration (with sensitive data masked)
    pub fn log_config(&self) {
        tracing::info!(
            server_address = %self.server.address(),
            environment = %self.server.environment,
            log_level = %self.server.log_level,
            "Server configuration"
        );

        tracing::info!(
            brokers = %self.kafka.brokers,
            group_id = %self.kafka.group_id,
            input_topic = %self.kafka.input_topic,
            dlq_topic = %self.kafka.dlq_topic,
            "Kafka configuration"
        );

        tracing::info!(
            url = %self.database.masked_url(),
            pool_size = %self.database.pool_max_size,
            "Database configuration"
        );

        tracing::info!(
            root = %self.parquet.root,
            flush_rows = %self.parquet.flush_max_rows,
            flush_interval_secs = %self.parquet.flush_interval_secs,
            "Parquet configuration"
        );

        tracing::info!(
            concurrency = %self.processing.worker_concurrency,
            max_retries = %self.processing.max_retries,
            "Processing configuration"
        );

        tracing::info!(
            parquet_export = %self.features.parquet_export,
            metrics = %self.features.metrics,
            tracing = %self.features.tracing,
            "Feature flags"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_defaults() {
        let config = ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 8080,
            log_level: "info".to_string(),
            environment: "development".to_string(),
            request_timeout_secs: 30,
            shutdown_timeout_secs: 30,
        };

        assert_eq!(config.address(), "127.0.0.1:8080");
        assert!(config.is_development());
        assert!(!config.is_production());
    }

    #[test]
    fn test_kafka_brokers_list() {
        let config = KafkaConfig {
            brokers: "broker1:9092, broker2:9092, broker3:9092".to_string(),
            group_id: "test".to_string(),
            input_topic: "input".to_string(),
            dlq_topic: "dlq".to_string(),
            auto_offset_reset: "earliest".to_string(),
            session_timeout_ms: 30000,
            enable_auto_commit: false,
            max_poll_interval_ms: 300000,
        };

        let brokers = config.brokers_list();
        assert_eq!(brokers.len(), 3);
        assert_eq!(brokers[0], "broker1:9092");
        assert_eq!(brokers[1], "broker2:9092");
        assert_eq!(brokers[2], "broker3:9092");
    }

    #[test]
    fn test_database_url_masking() {
        let config = DatabaseConfig {
            url: "postgresql://user:password@localhost:5432/db".to_string(),
            pool_max_size: 20,
            pool_min_idle: 5,
            pool_timeout_seconds: 30,
            pool_idle_timeout_seconds: 600,
        };

        let masked = config.masked_url();
        assert!(masked.contains("***"));
        assert!(!masked.contains("password"));
    }
}
