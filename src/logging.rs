//! Logging module for StreamForge
//!
//! This module configures structured logging using the tracing crate,
//! providing JSON output for production and pretty formatting for development.

use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Registry,
};

use crate::error::Result;

/// Initialize the logging system
///
/// Configures tracing based on the environment:
/// - Production: JSON formatted logs
/// - Development: Pretty formatted logs with colors
pub fn init_tracing(log_level: &str, environment: &str) -> Result<()> {
    // Create environment filter from RUST_LOG or use provided log level
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("streamforge={},tower_http=debug", log_level)));

    // Check if we're in production
    let is_production = environment == "production";

    if is_production {
        // Production: JSON formatting
        let formatting_layer = fmt::layer()
            .json()
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_target(true)
            .with_span_events(FmtSpan::CLOSE)
            .with_current_span(true);

        Registry::default()
            .with(env_filter)
            .with(formatting_layer)
            .try_init()
            .map_err(|e| {
                crate::error::Error::internal(format!("Failed to initialize tracing: {}", e))
            })?;
    } else {
        // Development: Pretty formatting with colors
        let formatting_layer = fmt::layer()
            .pretty()
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_target(true)
            .with_span_events(FmtSpan::CLOSE);

        Registry::default()
            .with(env_filter)
            .with(formatting_layer)
            .try_init()
            .map_err(|e| {
                crate::error::Error::internal(format!("Failed to initialize tracing: {}", e))
            })?;
    }

    tracing::info!(
        environment = environment,
        log_level = log_level,
        "Logging initialized"
    );

    Ok(())
}

/// Create a span for request tracking
#[macro_export]
macro_rules! request_span {
    ($request_id:expr) => {
        tracing::info_span!(
            "request",
            request_id = %$request_id,
            method = tracing::field::Empty,
            path = tracing::field::Empty,
            status = tracing::field::Empty,
            latency_ms = tracing::field::Empty,
        )
    };
}

/// Create a span for Kafka message processing
#[macro_export]
macro_rules! kafka_span {
    ($partition:expr, $offset:expr) => {
        tracing::info_span!(
            "kafka_message",
            partition = $partition,
            offset = $offset,
            event_id = tracing::field::Empty,
            event_type = tracing::field::Empty,
            processing_time_ms = tracing::field::Empty,
        )
    };
}

/// Create a span for database operations
#[macro_export]
macro_rules! db_span {
    ($operation:expr) => {
        tracing::info_span!(
            "database",
            operation = $operation,
            query = tracing::field::Empty,
            rows_affected = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
        )
    };
}

/// Create a span for Parquet operations
#[macro_export]
macro_rules! parquet_span {
    ($operation:expr) => {
        tracing::info_span!(
            "parquet",
            operation = $operation,
            file_path = tracing::field::Empty,
            rows_written = tracing::field::Empty,
            bytes_written = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
        )
    };
}

/// Log structured event data
#[macro_export]
macro_rules! log_event {
    ($level:ident, $msg:expr, $($key:ident = $value:expr),* $(,)?) => {
        tracing::$level!(
            $($key = tracing::field::display(&$value),)*
            $msg
        )
    };
}

/// Log an error with context
#[macro_export]
macro_rules! log_error {
    ($error:expr, $msg:expr) => {
        tracing::error!(
            error = %$error,
            error_type = ?$error,
            $msg
        )
    };
    ($error:expr, $msg:expr, $($key:ident = $value:expr),* $(,)?) => {
        tracing::error!(
            error = %$error,
            error_type = ?$error,
            $($key = tracing::field::display(&$value),)*
            $msg
        )
    };
}

/// Helper struct for logging metrics
pub struct LogMetrics;

impl LogMetrics {
    /// Log a counter increment
    pub fn counter(name: &str, value: u64, labels: &[(&str, &str)]) {
        tracing::info!(
            metric_type = "counter",
            metric_name = name,
            metric_value = value,
            metric_labels = ?labels,
            "Metric recorded"
        );
    }

    /// Log a gauge value
    pub fn gauge(name: &str, value: f64, labels: &[(&str, &str)]) {
        tracing::info!(
            metric_type = "gauge",
            metric_name = name,
            metric_value = value,
            metric_labels = ?labels,
            "Metric recorded"
        );
    }

    /// Log a histogram observation
    pub fn histogram(name: &str, value: f64, labels: &[(&str, &str)]) {
        tracing::info!(
            metric_type = "histogram",
            metric_name = name,
            metric_value = value,
            metric_labels = ?labels,
            "Metric recorded"
        );
    }
}

/// Helper for timing operations
pub struct Timer {
    start: std::time::Instant,
    operation: String,
}

impl Timer {
    /// Start a new timer
    pub fn start(operation: impl Into<String>) -> Self {
        Timer {
            start: std::time::Instant::now(),
            operation: operation.into(),
        }
    }

    /// Stop the timer and log the duration
    pub fn stop(self) -> std::time::Duration {
        let duration = self.start.elapsed();
        tracing::debug!(
            operation = %self.operation,
            duration_ms = duration.as_millis(),
            "Operation completed"
        );
        duration
    }

    /// Stop the timer and log with custom level
    pub fn stop_and_log(self, level: tracing::Level) -> std::time::Duration {
        let duration = self.start.elapsed();
        match level {
            tracing::Level::TRACE => tracing::trace!(
                operation = %self.operation,
                duration_ms = duration.as_millis(),
                "Operation completed"
            ),
            tracing::Level::DEBUG => tracing::debug!(
                operation = %self.operation,
                duration_ms = duration.as_millis(),
                "Operation completed"
            ),
            tracing::Level::INFO => tracing::info!(
                operation = %self.operation,
                duration_ms = duration.as_millis(),
                "Operation completed"
            ),
            tracing::Level::WARN => tracing::warn!(
                operation = %self.operation,
                duration_ms = duration.as_millis(),
                "Operation completed"
            ),
            tracing::Level::ERROR => tracing::error!(
                operation = %self.operation,
                duration_ms = duration.as_millis(),
                "Operation completed"
            ),
        }
        duration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer() {
        let timer = Timer::start("test_operation");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let duration = timer.stop();
        assert!(duration.as_millis() >= 10);
    }

    #[test]
    fn test_log_metrics() {
        // Just ensure the methods can be called without panicking
        LogMetrics::counter("test_counter", 1, &[("label", "value")]);
        LogMetrics::gauge("test_gauge", 1.0, &[("label", "value")]);
        LogMetrics::histogram("test_histogram", 1.0, &[("label", "value")]);
    }
}
