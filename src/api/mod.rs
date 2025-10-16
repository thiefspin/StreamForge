//! API module for StreamForge
//!
//! This module contains all HTTP API endpoints and server setup,
//! including health checks, metrics, and request handling middleware.

pub mod health;
pub mod server;

pub use health::{build_info, health_check, ready_check};
pub use server::{create_router, create_server, shutdown_signal};

/// API version constant
pub const API_VERSION: &str = "v1";

/// Get build information
pub fn get_build_info() -> BuildInfo {
    BuildInfo {
        version: env!("CARGO_PKG_VERSION"),
        commit: option_env!("GIT_COMMIT").unwrap_or("unknown"),
        build_time: option_env!("BUILD_TIME").unwrap_or("unknown"),
        rust_version: option_env!("RUSTC_VERSION").unwrap_or("unknown"),
    }
}

/// Build information populated at compile time
pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: env!("CARGO_PKG_VERSION"),
    commit: "unknown",
    build_time: "unknown",
    rust_version: "unknown",
};

/// Build information structure
#[derive(Debug, Clone, serde::Serialize)]
pub struct BuildInfo {
    /// Application version from Cargo.toml
    pub version: &'static str,
    /// Git commit hash
    pub commit: &'static str,
    /// Build timestamp
    pub build_time: &'static str,
    /// Rust version used for compilation
    pub rust_version: &'static str,
}

/// Health check response
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct HealthResponse {
    /// Service status
    pub status: HealthStatus,
    /// Optional message
    pub message: Option<String>,
    /// Current timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Ready check response
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ReadyResponse {
    /// Overall readiness status
    pub status: HealthStatus,
    /// Individual component checks
    pub checks: std::collections::HashMap<String, ComponentHealth>,
    /// Current timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Component health status
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ComponentHealth {
    /// Component status
    pub status: HealthStatus,
    /// Optional error message
    pub message: Option<String>,
    /// Last check timestamp
    pub last_check: chrono::DateTime<chrono::Utc>,
}

/// Health status enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Service is healthy
    Healthy,
    /// Service is degraded but operational
    Degraded,
    /// Service is unhealthy
    Unhealthy,
}

impl HealthStatus {
    /// Check if the status is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Convert to HTTP status code
    pub fn to_status_code(&self) -> axum::http::StatusCode {
        match self {
            HealthStatus::Healthy => axum::http::StatusCode::OK,
            HealthStatus::Degraded => axum::http::StatusCode::OK,
            HealthStatus::Unhealthy => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// API error response
#[derive(Debug, serde::Serialize)]
pub struct ApiError {
    /// Error message
    pub message: String,
    /// Error code for programmatic handling
    pub code: String,
    /// Request ID for tracing
    pub request_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status() {
        assert!(HealthStatus::Healthy.is_healthy());
        assert!(!HealthStatus::Unhealthy.is_healthy());

        assert_eq!(
            HealthStatus::Healthy.to_status_code(),
            axum::http::StatusCode::OK
        );
        assert_eq!(
            HealthStatus::Unhealthy.to_status_code(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn test_build_info() {
        assert!(!BUILD_INFO.version.is_empty());
        assert!(!BUILD_INFO.rust_version.is_empty());
    }
}
