//! Error handling module for StreamForge
//!
//! This module defines the error types used throughout the application,
//! providing a unified error handling strategy with proper error context
//! and HTTP response mapping.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

/// Result type alias for StreamForge operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for StreamForge
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Database connection or query errors
    #[error("Database error: {0}")]
    Database(String),

    /// Kafka related errors
    #[error("Kafka error: {0}")]
    Kafka(String),

    /// Validation errors for incoming data
    #[error("Validation error: {0}")]
    Validation(String),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// HTTP client errors
    #[error("HTTP error: {0}")]
    Http(String),

    /// Parquet file operations
    #[error("Parquet error: {0}")]
    Parquet(String),

    /// Generic internal errors
    #[error("Internal error: {0}")]
    Internal(String),

    /// Not found errors
    #[error("Not found: {0}")]
    NotFound(String),

    /// Timeout errors
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Rate limiting errors
    #[error("Rate limit exceeded: {0}")]
    RateLimit(String),

    /// Circuit breaker open
    #[error("Circuit breaker open: {0}")]
    CircuitBreakerOpen(String),

    /// Shutdown in progress
    #[error("Service is shutting down")]
    ShuttingDown,
}

impl Error {
    /// Create a configuration error
    pub fn config<S: Into<String>>(msg: S) -> Self {
        Error::Config(msg.into())
    }

    /// Create a database error
    pub fn database<S: Into<String>>(msg: S) -> Self {
        Error::Database(msg.into())
    }

    /// Create a Kafka error
    pub fn kafka<S: Into<String>>(msg: S) -> Self {
        Error::Kafka(msg.into())
    }

    /// Create a validation error
    pub fn validation<S: Into<String>>(msg: S) -> Self {
        Error::Validation(msg.into())
    }

    /// Create an internal error
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        Error::Internal(msg.into())
    }

    /// Get the appropriate HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::Validation(_) => StatusCode::BAD_REQUEST,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::RateLimit(_) => StatusCode::TOO_MANY_REQUESTS,
            Error::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
            Error::CircuitBreakerOpen(_) => StatusCode::SERVICE_UNAVAILABLE,
            Error::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
            Error::Config(_)
            | Error::Database(_)
            | Error::Kafka(_)
            | Error::Serialization(_)
            | Error::Io(_)
            | Error::Http(_)
            | Error::Parquet(_)
            | Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Database(_) | Error::Kafka(_) | Error::Timeout(_) | Error::Http(_)
        )
    }

    /// Check if this error should be sent to DLQ
    pub fn should_dlq(&self) -> bool {
        matches!(self, Error::Validation(_) | Error::Serialization(_))
    }
}

/// Implement IntoResponse for automatic error responses in Axum
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = self.status_code();

        // Create error response body
        let body = Json(json!({
            "error": {
                "message": self.to_string(),
                "type": error_type(&self),
                "status": status.as_u16(),
            }
        }));

        // Log error based on severity
        match status {
            StatusCode::INTERNAL_SERVER_ERROR | StatusCode::SERVICE_UNAVAILABLE => {
                tracing::error!(error = ?self, "Internal server error");
            },
            StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND => {
                tracing::warn!(error = ?self, "Client error");
            },
            _ => {
                tracing::info!(error = ?self, "Request error");
            },
        }

        (status, body).into_response()
    }
}

/// Get a string representation of the error type
fn error_type(error: &Error) -> &'static str {
    match error {
        Error::Config(_) => "configuration_error",
        Error::Database(_) => "database_error",
        Error::Kafka(_) => "kafka_error",
        Error::Validation(_) => "validation_error",
        Error::Serialization(_) => "serialization_error",
        Error::Io(_) => "io_error",
        Error::Http(_) => "http_error",
        Error::Parquet(_) => "parquet_error",
        Error::Internal(_) => "internal_error",
        Error::NotFound(_) => "not_found",
        Error::Timeout(_) => "timeout",
        Error::RateLimit(_) => "rate_limit",
        Error::CircuitBreakerOpen(_) => "circuit_breaker_open",
        Error::ShuttingDown => "shutting_down",
    }
}

/// Convert from anyhow::Error to our Error type
impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Internal(err.to_string())
    }
}

/// Convert from envconfig::Error to our Error type
impl From<envconfig::Error> for Error {
    fn from(err: envconfig::Error) -> Self {
        Error::Config(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_status_codes() {
        assert_eq!(
            Error::validation("test").status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            Error::NotFound("test".to_string()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            Error::internal("test").status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn test_error_retryable() {
        assert!(Error::database("test").is_retryable());
        assert!(Error::kafka("test").is_retryable());
        assert!(!Error::validation("test").is_retryable());
    }

    #[test]
    fn test_error_dlq() {
        assert!(Error::validation("test").should_dlq());
        assert!(!Error::database("test").should_dlq());
    }
}
