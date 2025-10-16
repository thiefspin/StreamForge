//! Database connection pool management for StreamForge
//!
//! This module provides connection pooling using SQLx with configuration
//! options for connection limits, timeouts, and retry behavior.

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::ConnectOptions;
use std::str::FromStr;
use std::time::Duration;

use crate::config::DatabaseConfig;
use crate::error::{Error, Result};

/// Type alias for the database connection pool
pub type DbPool = PgPool;

/// Create a new database connection pool
///
/// # Arguments
/// * `config` - Database configuration
///
/// # Returns
/// A configured connection pool ready for use
pub async fn create_pool(config: &DatabaseConfig) -> Result<DbPool> {
    // Parse connection options from URL
    let connect_options = PgConnectOptions::from_str(&config.url)
        .map_err(|e| Error::config(format!("Invalid database URL: {}", e)))?
        // Set application name for monitoring
        .application_name("streamforge")
        // Enable statement logging in debug mode
        .log_statements(tracing::log::LevelFilter::Debug)
        // Set statement timeout
        .statement_cache_capacity(100);

    // Configure pool options
    let pool = PgPoolOptions::new()
        // Connection pool size
        .max_connections(config.pool_max_size)
        .min_connections(config.pool_min_idle)
        // Timeouts
        .acquire_timeout(config.pool_timeout())
        .idle_timeout(Some(config.idle_timeout()))
        // Test connections before use
        .test_before_acquire(true)
        // Connection lifecycle
        .max_lifetime(Some(Duration::from_secs(3600))) // 1 hour
        // Build pool with options
        .connect_with(connect_options)
        .await
        .map_err(|e| Error::database(format!("Failed to create connection pool: {}", e)))?;

    // Verify connectivity
    sqlx::query("SELECT 1")
        .fetch_one(&pool)
        .await
        .map_err(|e| Error::database(format!("Failed to verify database connection: {}", e)))?;

    tracing::info!(
        max_connections = config.pool_max_size,
        min_idle = config.pool_min_idle,
        "Database connection pool created"
    );

    Ok(pool)
}

/// Create a test pool for unit tests
///
/// This creates a smaller pool suitable for testing with shorter timeouts.
#[cfg(test)]
pub async fn create_test_pool(database_url: &str) -> Result<DbPool> {
    let connect_options = PgConnectOptions::from_str(database_url)
        .map_err(|e| Error::config(format!("Invalid test database URL: {}", e)))?
        .application_name("streamforge_test");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Some(Duration::from_secs(10)))
        .test_before_acquire(true)
        .connect_with(connect_options)
        .await
        .map_err(|e| Error::database(format!("Failed to create test pool: {}", e)))?;

    Ok(pool)
}

/// Pool health check
///
/// Verifies that the pool can acquire a connection and execute a simple query.
pub async fn health_check(pool: &DbPool) -> Result<()> {
    let start = std::time::Instant::now();

    sqlx::query("SELECT 1")
        .fetch_one(pool)
        .await
        .map_err(|e| Error::database(format!("Health check failed: {}", e)))?;

    let elapsed = start.elapsed();

    if elapsed > Duration::from_secs(1) {
        tracing::warn!(
            elapsed_ms = elapsed.as_millis(),
            "Database health check slow"
        );
    }

    Ok(())
}

/// Get pool metrics for monitoring
pub struct PoolMetrics {
    /// Current pool size
    pub size: u32,
    /// Number of idle connections
    pub idle: usize,
    /// Maximum pool size
    pub max_size: u32,
    /// Number of connections being acquired
    pub acquiring: u32,
}

impl PoolMetrics {
    /// Create metrics from a pool
    pub fn from_pool(pool: &DbPool) -> Self {
        Self {
            size: pool.size(),
            idle: pool.num_idle(),
            max_size: pool.options().get_max_connections(),
            acquiring: pool.size() - pool.num_idle() as u32,
        }
    }

    /// Check if pool is healthy
    pub fn is_healthy(&self) -> bool {
        // Pool is healthy if we have some idle connections
        // or we're not at max capacity
        self.idle > 0 || self.size < self.max_size
    }

    /// Get pool utilization as a percentage
    pub fn utilization(&self) -> f64 {
        if self.max_size == 0 {
            return 0.0;
        }
        ((self.size - self.idle as u32) as f64 / self.max_size as f64) * 100.0
    }
}

/// Connection guard for manual transaction management
pub struct ConnectionGuard {
    conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

impl ConnectionGuard {
    /// Acquire a connection from the pool
    pub async fn acquire(pool: &DbPool) -> Result<Self> {
        let conn = pool
            .acquire()
            .await
            .map_err(|e| Error::database(format!("Failed to acquire connection: {}", e)))?;

        Ok(Self { conn })
    }

    /// Get a mutable reference to the connection
    pub fn conn_mut(&mut self) -> &mut sqlx::pool::PoolConnection<sqlx::Postgres> {
        &mut self.conn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_metrics() {
        // Create mock metrics
        let metrics = PoolMetrics {
            size: 10,
            idle: 3,
            max_size: 20,
            acquiring: 7,
        };

        assert!(metrics.is_healthy());
        assert_eq!(metrics.utilization(), 35.0); // (10-3)/20 * 100

        // Test unhealthy pool
        let unhealthy = PoolMetrics {
            size: 20,
            idle: 0,
            max_size: 20,
            acquiring: 20,
        };

        assert!(!unhealthy.is_healthy());
        assert_eq!(unhealthy.utilization(), 100.0);
    }

    #[test]
    fn test_pool_metrics_edge_cases() {
        // Test zero max size
        let metrics = PoolMetrics {
            size: 0,
            idle: 0,
            max_size: 0,
            acquiring: 0,
        };

        assert_eq!(metrics.utilization(), 0.0);
    }
}
