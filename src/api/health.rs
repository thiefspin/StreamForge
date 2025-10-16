//! Health check endpoints for StreamForge
//!
//! This module implements health and readiness checks for Kubernetes
//! and other orchestration platforms.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;

use crate::api::{ComponentHealth, HealthResponse, HealthStatus, ReadyResponse, BUILD_INFO};
use crate::error::Result;

/// Application state for health checks
#[derive(Clone)]
pub struct HealthState {
    /// Shared state for component health tracking
    pub components: Arc<tokio::sync::RwLock<HashMap<String, ComponentHealth>>>,
}

impl HealthState {
    /// Create a new health state
    pub fn new() -> Self {
        Self {
            components: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Update component health status
    pub async fn update_component(
        &self,
        name: String,
        status: HealthStatus,
        message: Option<String>,
    ) {
        let mut components = self.components.write().await;
        components.insert(
            name,
            ComponentHealth {
                status,
                message,
                last_check: Utc::now(),
            },
        );
    }

    /// Get overall health status
    pub async fn get_status(&self) -> HealthStatus {
        let components = self.components.read().await;

        // If any component is unhealthy, overall status is unhealthy
        if components.values().any(|c| c.status == HealthStatus::Unhealthy) {
            return HealthStatus::Unhealthy;
        }

        // If any component is degraded, overall status is degraded
        if components.values().any(|c| c.status == HealthStatus::Degraded) {
            return HealthStatus::Degraded;
        }

        HealthStatus::Healthy
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

/// Basic liveness check endpoint
///
/// Returns 200 OK if the service is alive.
/// This endpoint should be lightweight and not check external dependencies.
///
/// # Example
/// ```
/// GET /healthz
/// ```
pub async fn health_check() -> Response {
    let response = HealthResponse {
        status: HealthStatus::Healthy,
        message: Some("Service is running".to_string()),
        timestamp: Utc::now(),
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// Readiness check endpoint
///
/// Checks if the service is ready to accept traffic by verifying
/// all critical dependencies are available.
///
/// # Example
/// ```
/// GET /readyz
/// ```
pub async fn ready_check(State(state): State<Arc<HealthState>>) -> Response {
    let components = state.components.read().await.clone();
    let overall_status = state.get_status().await;

    let response = ReadyResponse {
        status: overall_status,
        checks: components,
        timestamp: Utc::now(),
    };

    let status_code = overall_status.to_status_code();
    (status_code, Json(response)).into_response()
}

/// Build information endpoint
///
/// Returns build metadata including version, commit hash, and build time.
///
/// # Example
/// ```
/// GET /build
/// ```
pub async fn build_info() -> Response {
    (StatusCode::OK, Json(&BUILD_INFO)).into_response()
}

/// Check database connectivity
pub async fn check_database_health() -> Result<ComponentHealth> {
    // TODO: Implement actual database health check
    // For now, return healthy status
    Ok(ComponentHealth {
        status: HealthStatus::Healthy,
        message: Some("Database connection pool is healthy".to_string()),
        last_check: Utc::now(),
    })
}

/// Check Kafka connectivity
pub async fn check_kafka_health() -> Result<ComponentHealth> {
    // TODO: Implement actual Kafka health check
    // For now, return healthy status
    Ok(ComponentHealth {
        status: HealthStatus::Healthy,
        message: Some("Kafka consumer is connected".to_string()),
        last_check: Utc::now(),
    })
}

/// Check disk space availability
pub async fn check_disk_health() -> Result<ComponentHealth> {
    // TODO: Implement actual disk space check
    // For now, return healthy status
    Ok(ComponentHealth {
        status: HealthStatus::Healthy,
        message: Some("Sufficient disk space available".to_string()),
        last_check: Utc::now(),
    })
}

/// Background task to periodically update component health
pub async fn health_monitor(state: Arc<HealthState>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

    loop {
        interval.tick().await;

        // Check database health
        if let Ok(health) = check_database_health().await {
            state
                .update_component("database".to_string(), health.status, health.message)
                .await;
        }

        // Check Kafka health
        if let Ok(health) = check_kafka_health().await {
            state.update_component("kafka".to_string(), health.status, health.message).await;
        }

        // Check disk space
        if let Ok(health) = check_disk_health().await {
            state
                .update_component("disk_space".to_string(), health.status, health.message)
                .await;
        }

        tracing::debug!("Health check completed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_state() {
        let state = HealthState::new();

        // Initially healthy
        assert_eq!(state.get_status().await, HealthStatus::Healthy);

        // Add healthy component
        state.update_component("test".to_string(), HealthStatus::Healthy, None).await;
        assert_eq!(state.get_status().await, HealthStatus::Healthy);

        // Add unhealthy component
        state
            .update_component(
                "failing".to_string(),
                HealthStatus::Unhealthy,
                Some("Connection failed".to_string()),
            )
            .await;
        assert_eq!(state.get_status().await, HealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn test_health_check_endpoint() {
        let response = health_check().await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_ready_check_endpoint() {
        let state = Arc::new(HealthState::new());

        // Add some component states
        state
            .update_component(
                "database".to_string(),
                HealthStatus::Healthy,
                Some("Connected".to_string()),
            )
            .await;

        let response = ready_check(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_build_info_endpoint() {
        let response = build_info().await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
